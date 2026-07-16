"""
Job scheduler — APScheduler BackgroundScheduler (UTC).

Runs saved jobs on a cron schedule:
  1. Load job from DB
  2. Fetch data via connectors (compare: conn_a + conn_b; quality/profile: source_conn)
  3. Run the analysis (compare_dataframes / analyze_quality from main.py)
  4. Email a rich report (Outlook on Windows, sendmail/SMTP on Linux)
  5. Store a summary in run history

Falls back to a plain pandas merge if importing main.py fails.
"""

import importlib
import os
import platform

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from workspace import db
from workspace.connectors import BaseConnector

_scheduler = None


# --------------------------------------------------------------------------
# Lifecycle
# --------------------------------------------------------------------------
def start_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        return _scheduler
    _scheduler = BackgroundScheduler(timezone="UTC")
    _scheduler.start()
    return _scheduler


def stop_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
    _scheduler = None


def load_all_jobs():
    """Re-register every cron job in the DB on startup."""
    if _scheduler is None:
        start_scheduler()
    # all users' jobs — db.list_jobs filters by user, so read raw
    conn = db._conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id, username, schedule_cron FROM ws_jobs "
                    "WHERE schedule_cron IS NOT NULL AND schedule_cron <> ''")
        rows = db._rows(cur)
    except Exception:
        rows = []
    for row in rows:
        try:
            schedule_job(row["id"], row["username"], row["schedule_cron"])
        except Exception as e:
            print(f"[scheduler] could not load job {row['id']}: {e}")


def schedule_job(job_id, username, cron_expr):
    if _scheduler is None:
        start_scheduler()
    unregister_job(job_id)
    trigger = CronTrigger.from_crontab(cron_expr, timezone="UTC")
    _scheduler.add_job(_execute_job, trigger=trigger, id=str(job_id),
                       args=[job_id, username], replace_existing=True)


def unregister_job(job_id):
    if _scheduler is None:
        return
    try:
        _scheduler.remove_job(str(job_id))
    except Exception:
        pass


def trigger_job_now(job_id, username):
    """Manual trigger -- runs synchronously, returns {"run_id", "email_sent",
    "email_skipped_reason", "email_error"}. Kept for callers that genuinely
    want to block until finished; the "Run Now" button uses
    trigger_job_now_background() instead so a slow/unreachable connector
    can't hang the HTTP request."""
    return _execute_job(job_id, username)


def trigger_job_now_background(job_id, username):
    """Create the run row immediately (fast -- one DB insert) and hand the
    actual fetch/analyze/email work to a background thread, returning the
    run_id right away. The caller polls get_run(run_id) / list_runs() for
    the real outcome instead of waiting on a request that could otherwise
    take minutes for a slow or unreachable data source."""
    import threading
    run_id = db.create_run(job_id, username)
    thread = threading.Thread(
        target=_run_job_body, args=(run_id, job_id, username), daemon=True
    )
    thread.start()
    return run_id


# --------------------------------------------------------------------------
# Execution
# --------------------------------------------------------------------------
def _fetch(conn_id, username):
    rec = db.get_connection(conn_id, username)
    if not rec:
        raise ValueError(f"Connection {conn_id} not found.")
    connector = BaseConnector.from_type(rec["source_type"], rec["config"])
    return connector.fetch()


def _run_compare(df_a, df_b, job):
    """Use main.compare_dataframes; fall back to a pandas merge."""
    keys = (job.get("key_columns") or "").split(",")
    keys = [k.strip() for k in keys if k.strip()] or None
    excludes = (job.get("exclude_columns") or "").split(",")
    excludes = [c.strip() for c in excludes if c.strip()] or None
    try:
        main = importlib.import_module("main")
        diff = main.compare_dataframes(df_a, df_b, keys, True,
                                       exclude_cols=excludes)
        diff["counts"] = {
            "matched": diff.get("file1_rows", 0) - diff.get("removed_count", 0),
            "file1_only": diff.get("file1_only_count", 0),
            "file2_only": diff.get("file2_only_count", 0),
            "modified": diff.get("modified_count", 0),
        }
        return diff
    except Exception as e:
        common = [c for c in df_a.columns if c in set(df_b.columns)]
        merged = df_a.merge(df_b, on=keys or common, how="outer",
                            indicator=True, suffixes=("_a", "_b"))
        return {
            "fallback": f"used pandas merge ({e})",
            "counts": {
                "file1_only": int((merged["_merge"] == "left_only").sum()),
                "file2_only": int((merged["_merge"] == "right_only").sum()),
                "matched": int((merged["_merge"] == "both").sum()),
            },
        }


def _generate_ai_hints(main, columns: list[str]) -> dict:
    # Same LLM hint-generation the interactive AI Copilot run uses (see
    # /rerun-quality-json) -- a scheduled job never had this at all, even
    # though the DB/API have carried an ai_hints field since the job was
    # created. Best-effort: any failure (no LLM configured, bad JSON, etc.)
    # just means the job runs without AI-suggested hints, same as before.
    try:
        raw = main._ask_llm([{"role": "user", "content": [{"text":
            "You are a data quality hint generator. Given these column names, suggest "
            "helpful hints as a single JSON object with these optional keys: "
            f"columns={columns}. "
            '"nullable_hints" (comma-separated column names that may legitimately be blank), '
            '"key_hints" (comma-separated column names likely to form a unique row key), '
            '"timeliness_hints" (comma-separated "column_name max_age_days" pairs), '
            '"bfsi_validators":["positive:price","allowed_values:side:BUY,SELL"]}'
            "\nReturn {} if nothing specific."
        }]}])
        import re
        m = re.search(r'\{.*\}', raw, re.DOTALL)
        if m:
            import json
            return json.loads(m.group(0))
    except Exception:
        pass
    return {}


def _run_quality(df, job):
    try:
        main = importlib.import_module("main")
        hints = dict(job.get("ai_hints") or {})
        if not hints:
            # Manually-configured hints (set via the job's ai_hints field)
            # take precedence and skip the LLM call entirely; otherwise
            # auto-generate them so every scheduled run gets the same
            # AI assistance an interactive AI Copilot run would.
            hints = _generate_ai_hints(main, list(df.columns))
        return main.analyze_quality(df, name=job.get("name", "job"), user_hints=hints)
    except Exception as e:
        return {"error": str(e), "rows": len(df), "columns": len(df.columns)}


def _run_fan_out(pairs, job, username):
    """Run the same compare logic across every {conn_a_id, conn_b_id, label}
    pair in `pairs`. A single pair failing (bad connection, fetch error, etc.)
    is recorded and skipped rather than aborting the rest of the batch -- one
    branch's feed being down shouldn't hide results for every other branch."""
    per_pair = []
    agg = {"matched": 0, "file1_only": 0, "file2_only": 0, "modified": 0}
    errors = []
    for pair in pairs:
        label = pair.get("label") or f"{pair.get('conn_a_id')} vs {pair.get('conn_b_id')}"
        try:
            df_a = _fetch(pair["conn_a_id"], username)
            df_b = _fetch(pair["conn_b_id"], username)
            result = _run_compare(df_a, df_b, job)
            counts = result.get("counts") or {}
            for k in agg:
                agg[k] += int(counts.get(k, 0) or 0)
            per_pair.append({"label": label, "counts": counts})
        except Exception as e:
            errors.append({"label": label, "error": str(e)})
            per_pair.append({"label": label, "error": str(e)})
    return {
        "fan_out": True,
        "pair_count": len(pairs),
        "error_count": len(errors),
        "counts": agg,
        "per_pair": per_pair,
    }


def _evaluate_sla(sla: dict, result: dict) -> dict:
    """Check a job's actual run result against its configured SLA
    thresholds (set via the job's `sla` field -- max_breaks, min_dq_score,
    max_null_pct, alert_on_schema_drift). Returns
    {"breached": bool, "reasons": [str, ...]}; an empty/unconfigured sla
    never breaches, so this is a no-op for every job that doesn't use SLAs."""
    if not sla:
        return {"breached": False, "reasons": []}
    reasons = []
    if sla.get("max_breaks") not in (None, ""):
        counts = result.get("counts") or {}
        breaks = (int(counts.get("file1_only", 0)) + int(counts.get("file2_only", 0))
                  + int(counts.get("modified", 0)))
        max_breaks = int(sla["max_breaks"])
        if breaks > max_breaks:
            reasons.append(f"{breaks} breaks exceeds max_breaks ({max_breaks})")
    if sla.get("min_dq_score") not in (None, ""):
        score = (result.get("dq_score") or {}).get("score")
        min_score = float(sla["min_dq_score"])
        if score is not None and float(score) < min_score:
            reasons.append(f"DQ score {score} is below min_dq_score ({min_score})")
    if sla.get("max_null_pct") not in (None, ""):
        max_null = float(sla["max_null_pct"])
        worst = None
        for col in result.get("columns") or []:
            pct = col.get("null_pct")
            if pct is not None and float(pct) > max_null and (worst is None or pct > worst[1]):
                worst = (col.get("name"), pct)
        if worst:
            reasons.append(f"Column '{worst[0]}' is {worst[1]}% null, exceeds max_null_pct ({max_null}%)")
    if sla.get("alert_on_schema_drift") and result.get("schema_drift"):
        reasons.append(f"Schema drift detected: {len(result['schema_drift'])} change(s)")
    return {"breached": bool(reasons), "reasons": reasons}


def _execute_job(job_id, username):
    """Returns a dict describing what happened -- run_id plus enough about
    the email outcome (sent / skipped-by-design / failed-to-send) that a
    manual "Run Now" click can tell the user the real story instead of a
    meaningless "triggered" placeholder. The cron scheduler (APScheduler)
    calls this too but ignores the return value, so this stays safe there."""
    run_id = db.create_run(job_id, username)
    return _run_job_body(run_id, job_id, username)


def _run_job_body(run_id, job_id, username):
    """The actual fetch + analyze + email work for an already-created run.
    Split out from _execute_job so a manual "Run Now" can create the run row
    (fast) and hand this off to a background thread instead of blocking the
    HTTP request for however long the data fetch + email take -- a slow or
    unreachable connector could take minutes, and Railway's own proxy will
    drop the connection well before that, making the button look like it did
    nothing even though the job may still finish (and the result would only
    show up later in History)."""
    email_info = {"email_sent": False, "email_skipped_reason": None, "email_error": None}
    try:
        job = db.get_job(job_id, username)
        if not job:
            raise ValueError("Job not found.")
        action = job["action"]
        if action == "compare" and job.get("fan_out_pairs"):
            result = _run_fan_out(job["fan_out_pairs"], job, username)
        elif action == "compare":
            df_a = _fetch(job["conn_a_id"], username)
            df_b = _fetch(job["conn_b_id"], username)
            result = _run_compare(df_a, df_b, job)
        elif action in ("quality", "profile"):
            df = _fetch(job["source_conn_id"], username)
            result = _run_quality(df, job)
        else:
            raise ValueError(f"Scheduled action '{action}' not supported.")

        summary = result.get("counts") or result.get("dimensions") or {}
        db.update_job_status(job_id, "ok")
        sla = job.get("sla") or {}
        sla_result = _evaluate_sla(sla, result)
        if not job.get("notify_email"):
            email_info["email_skipped_reason"] = "No notification email configured for this job."
        elif not sla_result["breached"] and sla.get("alert_only_on_fail"):
            email_info["email_skipped_reason"] = (
                "This job only emails on SLA breach/failure, and this run passed with no breach."
            )
        else:
            try:
                _send_rich_email_report(job, result, sla_result)
                email_info["email_sent"] = True
            except Exception as e:
                print(f"[scheduler] email failed for job {job_id}: {e}")
                email_info["email_error"] = str(e)
        db.finish_run(run_id, "success", summary=summary, **email_info)
        return {"run_id": run_id, **email_info}
    except Exception as e:
        db.update_job_status(job_id, "error")
        print(f"[scheduler] job {job_id} failed: {e}")
        # A job that silently stops working (bad credentials, connector down,
        # unsupported action) previously never told anyone -- notify_email
        # was only ever used on the success path. Best-effort: re-fetch the
        # job for the notify_email address in case the failure happened
        # before `job` was bound above (e.g. db.get_job itself failing).
        try:
            _job_for_alert = db.get_job(job_id, username)
            if _job_for_alert and _job_for_alert.get("notify_email"):
                _send_failure_email(_job_for_alert, str(e))
                email_info["email_sent"] = True
            else:
                email_info["email_skipped_reason"] = "No notification email configured for this job."
        except Exception as _email_exc:
            print(f"[scheduler] failure-alert email also failed for job {job_id}: {_email_exc}")
            email_info["email_error"] = str(_email_exc)
        db.finish_run(run_id, "failed", error_msg=str(e), **email_info)
        return {"run_id": run_id, **email_info}


# --------------------------------------------------------------------------
# Email
# --------------------------------------------------------------------------
def _html_report(job, result, sla_result=None):
    counts = result.get("counts") or {}
    dims = result.get("dimensions") or {}
    rows = "".join(f"<tr><td>{k}</td><td style='text-align:right'>{v}</td></tr>"
                   for k, v in {**counts, **dims}.items())
    sla_banner = ""
    if sla_result and sla_result.get("breached"):
        reasons = "".join(f"<li>{r}</li>" for r in sla_result.get("reasons", []))
        sla_banner = f"""
        <div style="background:#7f1d1d;color:#fff;padding:12px 16px;border-radius:6px;margin-bottom:12px;font-family:Arial">
          <b>&#128308; SLA BREACH</b>
          <ul style="margin:6px 0 0 0">{reasons}</ul>
        </div>"""
    return f"""
    {sla_banner}
    <h2>Data Validation report — {job.get('name')}</h2>
    <p>Action: <b>{job.get('action')}</b></p>
    <table border="1" cellpadding="6" cellspacing="0"
           style="border-collapse:collapse;font-family:Arial">
      <tr style="background:#1e293b;color:#fff"><th>Metric</th><th>Value</th></tr>
      {rows}
    </table>
    """


def _deliver_email(to_email: str, from_email: str, subject: str, html: str) -> None:
    if platform.system() == "Windows" and not os.getenv("SMTP_HOST"):
        try:
            import win32com.client
            outlook = win32com.client.Dispatch("Outlook.Application")
            mail = outlook.CreateItem(0)
            mail.To = to_email
            mail.Subject = subject
            mail.HTMLBody = html
            if from_email:
                mail.SentOnBehalfOfName = from_email
            mail.Send()
            return
        except Exception as e:
            print(f"[email] Outlook failed, falling back to SMTP: {e}")

    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_email or "no-reply@datavalidation.local"
    msg["To"] = to_email
    msg.attach(MIMEText(html, "html"))

    host = os.getenv("SMTP_HOST", "")
    if not host:
        # Defaulting to localhost:25 here used to fail silently (caught by
        # the caller's try/except and only ever printed to server logs) --
        # a container platform like Railway has no local mail server at
        # all, so every job's "successful" email delivery was actually
        # failing every time with nothing telling the user why.
        raise RuntimeError(
            "SMTP_HOST is not configured -- set SMTP_HOST, SMTP_PORT (587 recommended; "
            "port 25 is blocked outbound by most cloud platforms), SMTP_USER (or "
            "SMTP_USERNAME) and SMTP_PASSWORD as environment variables to enable email delivery."
        )
    port = int(os.getenv("SMTP_PORT", "587"))
    # Accept both SMTP_USERNAME and SMTP_USER -- the Settings page and the
    # manual "Send Email" button (main.py) write/read SMTP_USER, so a Railway
    # deployment configured through Settings only ever has SMTP_USER set.
    # Reading only SMTP_USERNAME here meant scheduled-job emails silently sent
    # with no login at all (most real SMTP providers then reject the send)
    # even though the exact same credentials worked fine for manual sends.
    username = os.getenv("SMTP_USERNAME") or os.getenv("SMTP_USER", "")
    password = os.getenv("SMTP_PASSWORD", "")

    # Force IPv4 DNS resolution for the duration of the SMTP connection.
    # Real symptom this fixes: "[Errno 101] Network is unreachable" on
    # Railway (and most container platforms) -- smtp-mail.outlook.com (and
    # most real mail providers) resolve to both an IPv4 and IPv6 address,
    # socket.create_connection() tries the IPv6 one first, and the
    # container has no outbound IPv6 route at all, so it fails before ever
    # trying the IPv4 address that would have worked. The hostname string
    # itself is untouched (still passed to smtplib as host=host below), so
    # STARTTLS certificate hostname verification is unaffected -- only the
    # underlying address family smtplib's socket.create_connection resolves
    # to is constrained, and only briefly, restored in `finally` either way.
    import socket
    _orig_getaddrinfo = socket.getaddrinfo
    def _ipv4_only_getaddrinfo(host_, port_, family=0, type=0, proto=0, flags=0):
        return _orig_getaddrinfo(host_, port_, socket.AF_INET, type, proto, flags)
    socket.getaddrinfo = _ipv4_only_getaddrinfo
    try:
        if port == 465:
            with smtplib.SMTP_SSL(host, port, timeout=20) as s:
                if username and password:
                    s.login(username, password)
                s.send_message(msg)
        else:
            # 587 (submission) and most other non-SSL ports expect STARTTLS --
            # almost every real provider (Gmail, Outlook365, SendGrid, SES SMTP,
            # etc.) rejects a plaintext, unauthenticated connection outright,
            # which is what the previous code sent.
            with smtplib.SMTP(host, port, timeout=20) as s:
                s.starttls()
                if username and password:
                    s.login(username, password)
                s.send_message(msg)
    finally:
        socket.getaddrinfo = _orig_getaddrinfo


def _send_rich_email_report(job, result, sla_result=None):
    to_email = job["notify_email"]
    from_email = job.get("from_email") or os.getenv("EMAIL_FROM", "")
    breached = bool(sla_result and sla_result.get("breached"))
    subject = f"[Data Validation] {'SLA BREACH -- ' if breached else ''}{job.get('name')} — {job.get('action')}"
    html = _html_report(job, result, sla_result)
    _deliver_email(to_email, from_email, subject, html)


def _send_failure_email(job, error_msg: str) -> None:
    # Previously a job that started failing (bad credentials, connector
    # down, unsupported action, anything) never told anyone -- notify_email
    # was only ever wired to the success path, so "set it and forget it"
    # automation could silently stop working indefinitely.
    to_email = job["notify_email"]
    from_email = job.get("from_email") or os.getenv("EMAIL_FROM", "")
    subject = f"[Data Validation] JOB FAILED -- {job.get('name')} ({job.get('action')})"
    html = f"""
    <div style="background:#7f1d1d;color:#fff;padding:12px 16px;border-radius:6px;margin-bottom:12px;font-family:Arial">
      <b>&#9888;&#65039; This scheduled job failed to run.</b>
    </div>
    <h2>Data Validation job failure — {job.get('name')}</h2>
    <p>Action: <b>{job.get('action')}</b></p>
    <p>Error: <code>{error_msg}</code></p>
    <p style="color:#64748b;font-size:12px">Check Saved Runs in Workspace for full history, or fix the underlying
    issue (credentials, connector availability, job configuration) and the next scheduled run will proceed normally.</p>
    """
    _deliver_email(to_email, from_email, subject, html)

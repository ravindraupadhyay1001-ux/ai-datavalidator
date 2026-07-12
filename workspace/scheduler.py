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
    """Manual trigger — runs synchronously, returns run_id."""
    return _execute_job(job_id, username)


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


def _run_quality(df, job):
    try:
        main = importlib.import_module("main")
        return main.analyze_quality(df, name=job.get("name", "job"))
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
    run_id = db.create_run(job_id, username)
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
        db.finish_run(run_id, "success", summary=summary)
        db.update_job_status(job_id, "ok")
        sla = job.get("sla") or {}
        sla_result = _evaluate_sla(sla, result)
        should_email = bool(job.get("notify_email")) and (
            sla_result["breached"] or not sla.get("alert_only_on_fail")
        )
        if should_email:
            try:
                _send_rich_email_report(job, result, sla_result)
            except Exception as e:
                print(f"[scheduler] email failed for job {job_id}: {e}")
        return run_id
    except Exception as e:
        db.finish_run(run_id, "failed", error_msg=str(e))
        db.update_job_status(job_id, "error")
        print(f"[scheduler] job {job_id} failed: {e}")
        return run_id


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


def _send_rich_email_report(job, result, sla_result=None):
    to_email = job["notify_email"]
    from_email = job.get("from_email") or os.getenv("EMAIL_FROM", "")
    breached = bool(sla_result and sla_result.get("breached"))
    subject = f"[Data Validation] {'SLA BREACH -- ' if breached else ''}{job.get('name')} — {job.get('action')}"
    html = _html_report(job, result, sla_result)

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
    host = os.getenv("SMTP_HOST", "localhost")
    port = int(os.getenv("SMTP_PORT", "25"))
    with smtplib.SMTP(host, port, timeout=20) as s:
        s.send_message(msg)

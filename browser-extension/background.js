// AI DataValidator — Web Extract : background service worker
// Stores the most recent extraction (from the content script or a captured
// download) so the popup can show + send it. Handles download-capture: when
// "armed", the next download is fetched and, if it's CSV/JSON/text, parsed into
// rows for you. Binary exports (xlsx, pdf) are recorded with a note to upload
// them the normal way (extensions can't read arbitrary local files).

let armed = false;

function setLast(payload) {
  chrome.storage.local.set({ lastExtract: payload });
  chrome.action.setBadgeText({ text: "1" });
  chrome.action.setBadgeBackgroundColor({ color: "#2563eb" });
}

// -- messages from content script / popup --
chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  if (!msg) return;
  if (msg.type === "aidv-extract") {
    setLast(msg.payload);
    // If this extraction was triggered by an app "Test run", post it straight back.
    chrome.storage.local.get(["activeRun"], (r) => {
      if (r.activeRun) {
        const p = msg.payload || {};
        postRunResult(r.activeRun.runId, {
          status: "done", columns: p.columns || [], rows: p.rows || [],
          message: p.mode === "download-note" ? (p.note || "") : "",
        });
        chrome.storage.local.remove("activeRun");
      }
    });
    sendResponse && sendResponse({ ok: true });
    return;
  }
  if (msg.cmd === "armDownload") { armed = true; sendResponse && sendResponse({ ok: true }); return; }
  if (msg.cmd === "disarmDownload") { armed = false; sendResponse && sendResponse({ ok: true }); return; }
  // Append a recorded step to the in-progress recipe.
  if (msg.type === "aidv-rec-step") {
    chrome.storage.local.get(["recRecipe"], (r) => {
      const rec = r.recRecipe || { steps: [], startUrl: msg.step && msg.step.url };
      // Collapse consecutive "set" on the same field into the latest value.
      const last = rec.steps[rec.steps.length - 1];
      if (last && msg.step.type === "set" && last.type === "set" && last.selector === msg.step.selector) last.value = msg.step.value;
      else rec.steps.push(msg.step);
      chrome.storage.local.set({ recRecipe: rec });
    });
    sendResponse && sendResponse({ ok: true });
    return;
  }
  return true;
});

// -- minimal CSV/TSV parser --
function parseDelimited(text, delim) {
  const rows = [];
  let field = "", row = [], q = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (q) {
      if (c === '"') { if (text[i + 1] === '"') { field += '"'; i++; } else q = false; }
      else field += c;
    } else if (c === '"') q = true;
    else if (c === delim) { row.push(field); field = ""; }
    else if (c === "\n") { row.push(field); rows.push(row); field = ""; row = []; }
    else if (c === "\r") { /* skip */ }
    else field += c;
  }
  if (field.length || row.length) { row.push(field); rows.push(row); }
  return rows.filter(r => r.length && !(r.length === 1 && r[0] === ""));
}

function toTable(text, filename) {
  const isJson = /\.json($|\?)/i.test(filename) || /^\s*[\[{]/.test(text);
  if (isJson) {
    try {
      let j = JSON.parse(text);
      if (!Array.isArray(j)) j = j.data || j.rows || j.records || j.results || [];
      if (Array.isArray(j) && j.length && typeof j[0] === "object") {
        const columns = Array.from(j.reduce((s, o) => { Object.keys(o).forEach(k => s.add(k)); return s; }, new Set()));
        const rows = j.map(o => columns.map(k => (o[k] == null ? "" : String(o[k]))));
        return { columns, rows };
      }
    } catch (e) { /* fall through to delimited */ }
  }
  const delim = /\.tsv($|\?)/i.test(filename) || (text.indexOf("\t") > -1 && text.indexOf(",") === -1) ? "\t" : ",";
  const grid = parseDelimited(text, delim);
  if (!grid.length) return null;
  return { columns: grid[0], rows: grid.slice(1) };
}

// -- download capture --
chrome.downloads.onCreated.addListener(async (item) => {
  if (!armed) return;
  armed = false;
  const url = item.finalUrl || item.url || "";
  const filename = (item.filename || url.split("/").pop() || "download").split("?")[0];
  const textish = /\.(csv|tsv|json|txt)($|\?)/i.test(filename) || /(csv|json|plain|tab-separated)/i.test(item.mime || "");
  if (textish && /^https?:/i.test(url)) {
    try {
      const res = await fetch(url, { credentials: "include" });
      const text = await res.text();
      const t = toTable(text, filename);
      if (t) { setLast({ ...t, rows_count: t.rows.length, cols_count: t.columns.length, source_url: url, source_title: filename, mode: "download", captured_at: new Date().toISOString() }); return; }
    } catch (e) { /* fall to note */ }
  }
  setLast({ note: `Downloaded "${filename}". This file type can't be read in-browser — upload it to AI DataValidator the normal way.`, source_url: url, source_title: filename, mode: "download-note", captured_at: new Date().toISOString(), columns: [], rows: [] });
});

chrome.runtime.onInstalled.addListener(() => chrome.action.setBadgeText({ text: "" }));

// ---- Live "Test run" loop --------------------------------------------------
// Poll the app for runs the user queued (Workspace > UI Extraction > Test run).
// When one arrives, replay its steps in the ACTIVE tab (the page the user is
// already logged into) — or, with no steps, just grab the table on that page —
// then post the result back. Nothing here reaches out to the source site on its
// own; it only acts on the tab the user has open.
const POLL_MS = 3000;
const RUN_TIMEOUT_MS = 90000;

function apiBase(u) { return (u || "").replace(/\/+$/, ""); }

async function postRunResult(runId, payload) {
  const { appUrl, appToken } = await chrome.storage.local.get(["appUrl", "appToken"]);
  if (!appUrl || !appToken) return;
  try {
    await fetch(apiBase(appUrl) + "/api/ws/ui-extract/run-result", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Extract-Token": appToken },
      body: JSON.stringify({ run_id: runId, ...payload }),
    });
  } catch (e) { /* the app's own poll will time out and tell the user */ }
}

async function startRun(run) {
  await chrome.storage.local.set({ activeRun: { runId: run.id, started: Date.now() } });
  const [t] = await chrome.tabs.query({ active: true, currentWindow: true });
  if (!t || !/^https?:/i.test(t.url || "")) {
    await postRunResult(run.id, { status: "error", message: "Open your source web page in the active tab, then run Test again." });
    await chrome.storage.local.remove("activeRun");
    return;
  }
  const steps = run.steps || [];
  const stepsText = (run.steps_text || "").trim();
  if (steps.length) {
    // Recorded/structured steps -> replay directly.
    await chrome.storage.local.set({ replay: { steps, i: 0 } });
    if (run.start_url && run.start_url !== t.url) chrome.tabs.update(t.id, { url: run.start_url });
    else chrome.tabs.reload(t.id); // reload so the content script picks up the replay state
  } else if (stepsText) {
    // Plain-English steps -> snapshot the page, let the app's AI plan actions,
    // then run them. Planning happens on each run against the live DOM, so
    // selectors self-heal when the page changes.
    planAndRun(run, t, stepsText);
  } else {
    // No steps at all -> grab the table on the page the user is on.
    chrome.tabs.sendMessage(t.id, { cmd: "extractAuto" }, () => {
      if (chrome.runtime.lastError) {
        postRunResult(run.id, { status: "error", message: "Couldn't read that tab. Open a normal web page showing the data, then Test again." });
        chrome.storage.local.remove("activeRun");
      }
    });
  }
}

async function planAndRun(run, tab, stepsText) {
  const { appUrl, appToken } = await chrome.storage.local.get(["appUrl", "appToken"]);
  chrome.tabs.sendMessage(tab.id, { cmd: "snapshot" }, async (resp) => {
    if (chrome.runtime.lastError || !resp || !Array.isArray(resp.dom)) {
      await postRunResult(run.id, { status: "error", message: "Couldn't read the page. Open the data page in the active tab and try again." });
      await chrome.storage.local.remove("activeRun");
      return;
    }
    let actions;
    try {
      const res = await fetch(apiBase(appUrl) + "/api/ws/ui-extract/plan-steps", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Extract-Token": appToken },
        body: JSON.stringify({ steps_text: stepsText, dom: resp.dom }),
      });
      const j = await res.json().catch(() => ({}));
      if (!res.ok || j.error) {
        await postRunResult(run.id, { status: "error", message: j.error || ("Planner failed (HTTP " + res.status + ").") });
        await chrome.storage.local.remove("activeRun");
        return;
      }
      actions = j.actions || [];
    } catch (e) {
      await postRunResult(run.id, { status: "error", message: "Couldn't reach the app to plan the steps." });
      await chrome.storage.local.remove("activeRun");
      return;
    }
    if (!actions.length) {
      await postRunResult(run.id, { status: "error", message: "Couldn't map your steps to anything on this page. Open the data page and simplify the steps." });
      await chrome.storage.local.remove("activeRun");
      return;
    }
    chrome.tabs.sendMessage(tab.id, { cmd: "runActions", actions }, () => { /* result arrives via aidv-extract */ });
  });
}

async function pollRuns() {
  const { appUrl, appToken, activeRun } = await chrome.storage.local.get(["appUrl", "appToken", "activeRun"]);
  if (!appUrl || !appToken) return;
  if (activeRun) {
    // Watchdog: a claimed run that never produced a result -> report the failure.
    if (Date.now() - activeRun.started > RUN_TIMEOUT_MS) {
      await postRunResult(activeRun.runId, { status: "error", message: "No table was captured in time. Open the data page and try again." });
      await chrome.storage.local.remove("activeRun");
    }
    return; // one run at a time
  }
  try {
    const res = await fetch(apiBase(appUrl) + "/api/ws/ui-extract/next-run", { headers: { "X-Extract-Token": appToken } });
    if (!res.ok) return;
    const run = (await res.json()).run;
    if (run && run.id) startRun(run);
  } catch (e) { /* offline / app not reachable — keep polling */ }
}

setInterval(pollRuns, POLL_MS);
chrome.alarms.create("aidv-poll", { periodInMinutes: 0.5 }); // wakes the worker when idle
chrome.alarms.onAlarm.addListener((a) => { if (a.name === "aidv-poll") pollRuns(); });

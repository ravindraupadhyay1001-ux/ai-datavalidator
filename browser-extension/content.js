// AI DataValidator — Web Extract : content script
// Runs on every page. Three jobs: (1) table extraction (click-to-pick),
// (2) RECORD a multi-step flow, (3) REPLAY it. Recording/replay state lives in
// chrome.storage so it survives page navigations (multi-step flows change pages).
(() => {
  if (window.__aidvWebExtract) return;
  window.__aidvWebExtract = true;

  const HILITE = "__aidv_hilite__";
  let picking = false, lastHover = null, banner = null;

  // ---------- shared UI ----------
  function style() {
    if (document.getElementById(HILITE)) return;
    const s = document.createElement("style"); s.id = HILITE;
    s.textContent = `.aidv-outline{outline:3px solid #2563eb !important;outline-offset:2px !important;cursor:pointer !important;background:rgba(37,99,235,.06) !important;}
      #aidv-banner{position:fixed;z-index:2147483647;top:12px;left:50%;transform:translateX(-50%);background:#111a2e;color:#fff;font:500 13px/1.4 system-ui,sans-serif;padding:8px 14px;border-radius:10px;box-shadow:0 6px 20px rgba(0,0,0,.35);}
      #aidv-banner b{color:#7db0ff}`;
    document.documentElement.appendChild(s);
  }
  function showBanner(msg) { style(); if (!banner) { banner = document.createElement("div"); banner.id = "aidv-banner"; document.documentElement.appendChild(banner); } banner.innerHTML = msg; }
  function clearBanner() { if (banner) { banner.remove(); banner = null; } }
  function bannerTemp(m, ms = 2200) { showBanner(m); setTimeout(clearBanner, ms); }

  // ---------- robust selector ----------
  function sel(el) {
    if (!el || el.nodeType !== 1) return null;
    if (el.id && /^[A-Za-z][\w-]*$/.test(el.id)) return "#" + el.id;
    for (const a of ["data-testid", "data-test", "name", "aria-label"]) {
      const v = el.getAttribute && el.getAttribute(a);
      if (v && !/["\\]/.test(v)) return `${el.tagName.toLowerCase()}[${a}="${v}"]`;
    }
    const parts = [];
    let n = el;
    while (n && n.nodeType === 1 && n !== document.body && parts.length < 6) {
      let s = n.tagName.toLowerCase();
      const p = n.parentElement;
      if (p) { const same = Array.from(p.children).filter(c => c.tagName === n.tagName); if (same.length > 1) s += `:nth-of-type(${same.indexOf(n) + 1})`; }
      parts.unshift(s); n = n.parentElement;
    }
    return parts.join(" > ");
  }
  function waitFor(s, t = 9000) {
    return new Promise((res, rej) => { const st = Date.now(); (function poll() { let e; try { e = document.querySelector(s); } catch (_) {} if (e) return res(e); if (Date.now() - st > t) return rej("not found: " + s); setTimeout(poll, 150); })(); });
  }

  // ---------- table serialize (from v0.1) ----------
  function serializeTable(table) {
    const norm = (el) => (el ? el.innerText.replace(/\s+/g, " ").trim() : "");
    const trs = Array.from(table.querySelectorAll("tr")).filter(tr => tr.querySelector("th,td"));
    if (!trs.length) return null;
    const grid = trs.map(tr => { const out = []; Array.from(tr.children).forEach(c => { if (!/^(TD|TH)$/.test(c.tagName)) return; const span = Math.max(1, parseInt(c.getAttribute("colspan") || "1", 10)); for (let i = 0; i < span; i++) out.push(norm(c)); }); return out; });
    const w = Math.max(...grid.map(r => r.length)); grid.forEach(r => { while (r.length < w) r.push(""); });
    let columns, rows;
    if (trs[0].querySelector("th")) { columns = grid[0]; rows = grid.slice(1); } else { columns = grid[0].map((_, i) => "col" + (i + 1)); rows = grid; }
    const seen = {}; columns = columns.map((c, i) => { let nm = c || ("col" + (i + 1)); if (seen[nm] != null) nm = nm + "_" + (++seen[nm]); else seen[nm] = 0; return nm; });
    return { columns, rows };
  }
  function bestTable() { const t = Array.from(document.querySelectorAll("table")).filter(x => x.querySelectorAll("tr").length >= 2); if (!t.length) return null; t.sort((a, b) => b.querySelectorAll("tr").length - a.querySelectorAll("tr").length); return t[0]; }
  function deliver(payload) { payload.rows_count = payload.rows.length; payload.cols_count = payload.columns.length; payload.captured_at = new Date().toISOString(); chrome.runtime.sendMessage({ type: "aidv-extract", payload }, () => {}); bannerTemp(`Captured ${payload.rows_count} rows &times; ${payload.cols_count} cols &mdash; open the extension to send.`); }

  // ---------- click-to-pick table ----------
  function tableUnder(x, y) { let el = document.elementFromPoint(x, y); while (el && el !== document.body) { if (el.tagName === "TABLE") return el; el = el.parentElement; } return null; }
  function endPick() { picking = false; if (lastHover) lastHover.classList.remove("aidv-outline"); lastHover = null; clearBanner(); document.removeEventListener("mousemove", onMove, true); document.removeEventListener("click", onClickPick, true); document.removeEventListener("keydown", onKeyPick, true); }
  function onMove(e) { const t = tableUnder(e.clientX, e.clientY); if (t === lastHover) return; if (lastHover) lastHover.classList.remove("aidv-outline"); lastHover = t; if (t) t.classList.add("aidv-outline"); }
  function onClickPick(e) { const t = tableUnder(e.clientX, e.clientY); if (!t) return; e.preventDefault(); e.stopPropagation(); const d = serializeTable(t); endPick(); if (d) deliver({ ...d, source_url: location.href, source_title: document.title, mode: "table" }); else bannerTemp("No readable rows in that table."); }
  function onKeyPick(e) { if (e.key === "Escape") endPick(); }
  function startPick() {
    if (picking) return;
    const b = bestTable(); if (b && document.querySelectorAll("table").length === 1) { const d = serializeTable(b); if (d) return deliver({ ...d, source_url: location.href, source_title: document.title, mode: "table" }); }
    picking = true; showBanner("Click the <b>table</b> to extract &middot; <b>Esc</b> to cancel");
    document.addEventListener("mousemove", onMove, true); document.addEventListener("click", onClickPick, true); document.addEventListener("keydown", onKeyPick, true);
  }
  function extractAuto() { const b = bestTable(); if (b) { const d = serializeTable(b); if (d) return deliver({ ...d, source_url: location.href, source_title: document.title, mode: "table" }); } bannerTemp("No table found on this page — try 'Extract a table' and pick manually."); }

  // ---------- RECORD ----------
  function recClick(e) {
    const el = e.target.closest("a,button,[role=button],input[type=submit],input[type=button],td,th,li,span,div");
    if (!el) return;
    chrome.runtime.sendMessage({ type: "aidv-rec-step", step: { type: "click", selector: sel(el), text: (el.innerText || "").trim().slice(0, 40), url: location.href } }, () => {});
  }
  function recChange(e) {
    const el = e.target; if (!el || !/^(INPUT|SELECT|TEXTAREA)$/.test(el.tagName)) return;
    if (el.type === "password") return; // never record secrets
    chrome.runtime.sendMessage({ type: "aidv-rec-step", step: { type: "set", selector: sel(el), value: el.value, url: location.href } }, () => {});
  }
  function startRecordListeners() { document.addEventListener("click", recClick, true); document.addEventListener("change", recChange, true); showBanner("&#128308; Recording your steps &mdash; do the navigation, then Stop in the extension."); }
  function stopRecordListeners() { document.removeEventListener("click", recClick, true); document.removeEventListener("change", recChange, true); clearBanner(); }

  // ---------- REPLAY ----------
  async function runReplay(state) {
    // state = {steps, i}. Execute from i; persist i after each step so we resume
    // on the next page if a step triggered navigation.
    showBanner("&#9654; Replaying step " + (state.i + 1) + " / " + state.steps.length);
    for (let i = state.i; i < state.steps.length; i++) {
      const s = state.steps[i];
      try {
        const el = await waitFor(s.selector);
        if (s.type === "set") { el.focus(); el.value = s.value; el.dispatchEvent(new Event("input", { bubbles: true })); el.dispatchEvent(new Event("change", { bubbles: true })); }
        else { el.scrollIntoView({ block: "center" }); el.click(); }
      } catch (err) { bannerTemp("&#9888;&#65039; Step " + (i + 1) + " failed: " + err); chrome.storage.local.set({ replay: null }); return; }
      await chrome.storage.local.set({ replay: { steps: state.steps, i: i + 1 } });
      await new Promise(r => setTimeout(r, 600));  // let the page react / maybe navigate
      if (i + 1 >= state.steps.length) break;
    }
    chrome.storage.local.set({ replay: null });
    clearBanner();
    setTimeout(extractAuto, 800); // after the flow, grab the resulting table
  }

  // On every page load, resume a replay in progress.
  chrome.storage.local.get(["replay", "recording"], (r) => {
    if (r.replay && r.replay.steps) setTimeout(() => runReplay(r.replay), 700);
    if (r.recording) startRecordListeners();
  });

  chrome.runtime.onMessage.addListener((msg, _s, sendResponse) => {
    if (!msg) return;
    if (msg.cmd === "pickTable") { startPick(); sendResponse({ ok: true }); }
    else if (msg.cmd === "extractAuto") { extractAuto(); sendResponse({ ok: true }); }
    else if (msg.cmd === "startRecord") { startRecordListeners(); sendResponse({ ok: true }); }
    else if (msg.cmd === "stopRecord") { stopRecordListeners(); sendResponse({ ok: true }); }
    return true;
  });
})();

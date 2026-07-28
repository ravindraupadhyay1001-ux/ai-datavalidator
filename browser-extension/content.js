// AI DataValidator — Web Extract : content script
// Runs on every page. Idle until the popup asks it to extract a table.
// Because the popup closes the moment the user clicks the page to pick a table,
// the result is stored in chrome.storage.local (via the background worker) rather
// than returned to the popup — the popup reads it back when reopened.
(() => {
  if (window.__aidvWebExtract) return;      // avoid double-injection
  window.__aidvWebExtract = true;

  const HILITE = "__aidv_hilite__";
  let picking = false, lastHover = null, banner = null;

  function style() {
    if (document.getElementById(HILITE)) return;
    const s = document.createElement("style");
    s.id = HILITE;
    s.textContent = `
      .aidv-outline{outline:3px solid #2563eb !important; outline-offset:2px !important; cursor:pointer !important; background:rgba(37,99,235,.06) !important;}
      #aidv-banner{position:fixed;z-index:2147483647;top:12px;left:50%;transform:translateX(-50%);
        background:#111a2e;color:#fff;font:500 13px/1.4 system-ui,sans-serif;padding:8px 14px;border-radius:10px;
        box-shadow:0 6px 20px rgba(0,0,0,.35);}
      #aidv-banner b{color:#7db0ff}`;
    document.documentElement.appendChild(s);
  }

  function showBanner(msg) {
    style();
    if (!banner) { banner = document.createElement("div"); banner.id = "aidv-banner"; document.documentElement.appendChild(banner); }
    banner.innerHTML = msg;
  }
  function clearBanner() { if (banner) { banner.remove(); banner = null; } }

  // Serialize an HTML table -> {columns:[...], rows:[[...]]}. Pragmatic: reads
  // cells left-to-right, expands colspan, uses the first row (or <thead>) as header.
  function serializeTable(table) {
    const norm = (el) => (el ? el.innerText.replace(/\s+/g, " ").trim() : "");
    const trs = Array.from(table.querySelectorAll("tr")).filter(tr => tr.querySelector("th,td"));
    if (!trs.length) return null;
    const grid = trs.map(tr => {
      const out = [];
      Array.from(tr.children).forEach(cell => {
        if (!/^(TD|TH)$/.test(cell.tagName)) return;
        const span = Math.max(1, parseInt(cell.getAttribute("colspan") || "1", 10));
        const v = norm(cell);
        for (let i = 0; i < span; i++) out.push(v);
      });
      return out;
    });
    const width = Math.max(...grid.map(r => r.length));
    grid.forEach(r => { while (r.length < width) r.push(""); });
    // Header = first row if it holds <th>, else generated col names.
    const firstIsHeader = trs[0].querySelector("th");
    let columns, rows;
    if (firstIsHeader) { columns = grid[0]; rows = grid.slice(1); }
    else { columns = grid[0].map((_, i) => "col" + (i + 1)); rows = grid; }
    // Dedupe blank/duplicate headers so downstream stays clean.
    const seen = {};
    columns = columns.map((c, i) => {
      let name = c || ("col" + (i + 1));
      if (seen[name] != null) name = name + "_" + (++seen[name]); else seen[name] = 0;
      return name;
    });
    return { columns, rows };
  }

  function tableUnder(x, y) {
    let el = document.elementFromPoint(x, y);
    while (el && el !== document.body) { if (el.tagName === "TABLE") return el; el = el.parentElement; }
    return null;
  }

  function endPick() {
    picking = false;
    if (lastHover) lastHover.classList.remove("aidv-outline");
    lastHover = null; clearBanner();
    document.removeEventListener("mousemove", onMove, true);
    document.removeEventListener("click", onClick, true);
    document.removeEventListener("keydown", onKey, true);
  }
  function onMove(e) {
    const t = tableUnder(e.clientX, e.clientY);
    if (t === lastHover) return;
    if (lastHover) lastHover.classList.remove("aidv-outline");
    lastHover = t; if (t) t.classList.add("aidv-outline");
  }
  function onClick(e) {
    const t = tableUnder(e.clientX, e.clientY);
    if (!t) return;
    e.preventDefault(); e.stopPropagation();
    const data = serializeTable(t);
    endPick();
    if (data) deliver({ ...data, source_url: location.href, source_title: document.title, mode: "table" });
    else showBannerTemp("No readable rows in that table.");
  }
  function onKey(e) { if (e.key === "Escape") { endPick(); } }
  function showBannerTemp(m) { showBanner(m); setTimeout(clearBanner, 2000); }

  function startPick() {
    if (picking) return;
    // Auto-pick if there's exactly one substantial table.
    const tables = Array.from(document.querySelectorAll("table"))
      .filter(t => t.querySelectorAll("tr").length >= 2);
    if (tables.length === 1) {
      const data = serializeTable(tables[0]);
      if (data) { deliver({ ...data, source_url: location.href, source_title: document.title, mode: "table" }); return; }
    }
    picking = true;
    showBanner("Click the <b>table</b> you want to extract &middot; <b>Esc</b> to cancel");
    document.addEventListener("mousemove", onMove, true);
    document.addEventListener("click", onClick, true);
    document.addEventListener("keydown", onKey, true);
  }

  function deliver(payload) {
    payload.rows_count = payload.rows.length;
    payload.cols_count = payload.columns.length;
    payload.captured_at = new Date().toISOString();
    chrome.runtime.sendMessage({ type: "aidv-extract", payload }, () => {});
    showBannerTemp(`Captured ${payload.rows_count} rows &times; ${payload.cols_count} cols &mdash; open the extension to send.`);
  }

  chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
    if (msg && msg.cmd === "pickTable") { startPick(); sendResponse({ ok: true }); }
    return true;
  });
})();

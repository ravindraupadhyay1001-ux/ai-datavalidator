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
  if (msg.type === "aidv-extract") { setLast(msg.payload); sendResponse && sendResponse({ ok: true }); return; }
  if (msg.cmd === "armDownload") { armed = true; sendResponse && sendResponse({ ok: true }); return; }
  if (msg.cmd === "disarmDownload") { armed = false; sendResponse && sendResponse({ ok: true }); return; }
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

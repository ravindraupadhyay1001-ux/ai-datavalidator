// AI DataValidator — Web Extract : popup
const $ = (id) => document.getElementById(id);
let current = null;

function esc(s){ return String(s).replace(/[&<>]/g, c => ({ "&":"&amp;","<":"&lt;",">":"&gt;" }[c])); }

function renderPreview(x) {
  const box = $("preview");
  if (!x) { box.innerHTML = '<p class="muted" style="margin:12px 0 0">Nothing captured yet. Click <b>Extract a table</b>, pick a table on the page, then reopen this.</p>'; return; }
  if (x.mode === "download-note") {
    box.innerHTML = `<div class="card"><b>${esc(x.source_title)}</b><p class="muted" style="margin:6px 0 0">${esc(x.note)}</p></div>`;
    return;
  }
  const cols = x.columns || [], rows = x.rows || [];
  const head = "<tr>" + cols.slice(0, 6).map(c => `<th>${esc(c)}</th>`).join("") + (cols.length > 6 ? "<th>…</th>" : "") + "</tr>";
  const body = rows.slice(0, 5).map(r =>
    "<tr>" + r.slice(0, 6).map(v => `<td>${esc(v)}</td>`).join("") + (cols.length > 6 ? "<td>…</td>" : "") + "</tr>").join("");
  box.innerHTML = `<div class="card">
    <div style="display:flex;justify-content:space-between;margin-bottom:6px">
      <b>${esc(x.source_title || "Extracted table")}</b>
      <span class="muted">${rows.length} rows × ${cols.length} cols</span></div>
    <div style="overflow:auto"><table><thead>${head}</thead><tbody>${body}</tbody></table></div>
    ${rows.length > 5 ? `<p class="muted" style="margin:6px 0 0">showing 5 of ${rows.length} rows</p>` : ""}
  </div>`;
}

function refresh() {
  chrome.storage.local.get(["lastExtract"], (r) => {
    current = r.lastExtract || null;
    renderPreview(current);
    const sendable = current && current.rows && current.rows.length;
    $("btn-send").disabled = !sendable;
    $("btn-csv").disabled = !sendable;
  });
}

function toCsv(x) {
  const q = (v) => { v = v == null ? "" : String(v); return /[",\n]/.test(v) ? '"' + v.replace(/"/g, '""') + '"' : v; };
  return [x.columns.map(q).join(","), ...x.rows.map(r => r.map(q).join(","))].join("\r\n");
}

async function activeTab() { const [t] = await chrome.tabs.query({ active: true, currentWindow: true }); return t; }

$("btn-pick").onclick = async () => {
  const t = await activeTab();
  chrome.tabs.sendMessage(t.id, { cmd: "pickTable" }, () => {
    if (chrome.runtime.lastError) $("status").textContent = "Open a normal web page first (not a browser/system page).";
    else window.close(); // let the user click the page; result is stored for next open
  });
};

$("btn-dl").onclick = () => {
  chrome.runtime.sendMessage({ cmd: "armDownload" }, () => {
    $("status").innerHTML = "Armed. Now click the site's <b>Export/Download</b> button — reopen this after it downloads.";
  });
};

$("btn-csv").onclick = () => {
  if (!current) return;
  const blob = new Blob([toCsv(current)], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const name = (current.source_title || "extract").replace(/[^\w.-]+/g, "_").slice(0, 40) + ".csv";
  chrome.downloads.download({ url, filename: name });
};

$("btn-clear").onclick = () => { chrome.storage.local.remove("lastExtract"); chrome.action.setBadgeText({ text: "" }); refresh(); $("status").textContent = ""; };

$("btn-send").onclick = async () => {
  const { appUrl, appToken } = await chrome.storage.local.get(["appUrl", "appToken"]);
  if (!appUrl || !appToken) { $("status").textContent = "Set the app URL + pairing token in ⚙ settings first."; return; }
  if (!current) return;
  $("btn-send").disabled = true; $("status").textContent = "Sending…";
  try {
    const res = await fetch(appUrl.replace(/\/+$/, "") + "/api/ws/ui-extract/ingest", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Extract-Token": appToken },
      body: JSON.stringify({
        name: current.source_title || "Web extract",
        source_url: current.source_url || "",
        columns: current.columns, rows: current.rows,
      }),
    });
    const j = await res.json().catch(() => ({}));
    if (res.ok && !j.error) {
      $("status").innerHTML = `✅ Sent ${current.rows.length} rows. Use it in <b>Workspace → UI Extraction</b>.`;
    } else {
      $("status").textContent = "❌ " + (j.error || j.detail || ("HTTP " + res.status));
      $("btn-send").disabled = false;
    }
  } catch (e) { $("status").textContent = "❌ Could not reach the app: " + e.message; $("btn-send").disabled = false; }
};

$("btn-save").onclick = () => {
  chrome.storage.local.set({ appUrl: $("app-url").value.trim(), appToken: $("app-token").value.trim() }, () => {
    $("status").textContent = "Saved.";
  });
};

chrome.storage.local.get(["appUrl", "appToken"], (r) => {
  if (r.appUrl) $("app-url").value = r.appUrl;
  if (r.appToken) $("app-token").value = r.appToken;
});
refresh();

# AI DataValidator — Web Extract (browser extension)

Extract a **table** from any web app (or **capture its exported CSV/JSON**) and send
it straight into AI DataValidator — **without any backend access**. It runs in the
user's own already-logged-in browser session, so SSO/MFA and every multi-step
navigation just work, and **nothing runs on the server** (no Playwright, zero Railway
footprint).

## Why an extension (not server-side Playwright)
A multi-step, JavaScript-driven UI needs a real browser. Running one on the server
costs ~½ GB of image + hundreds of MB of RAM per run — too heavy for the app's plan.
So the browser is the one the user already has. The app only receives the finished
data via a tiny JSON endpoint.

## What it does today (v0.1 — "Extract & Send")
- **Extract a table** — click the extension, then click the table on the page. If the
  page has exactly one table it auto-selects. Preview appears in the popup.
- **Capture next download** — arm it, click the site's Export/Download button; if the
  file is CSV/TSV/JSON it's parsed for you (binary files like xlsx: upload normally).
- **Send to AI DataValidator** — posts the rows to your instance; it becomes a source.
- **CSV** — download the extracted table locally instead.

The user does their normal multi-step navigation (login → menus → filters → the data
page); the extension just grabs the final result. **Record/replay + AI "English →
steps" + scheduling are the next version.**

## Install (developer / unpacked)
1. Chrome or Edge → `chrome://extensions` (or `edge://extensions`).
2. Turn on **Developer mode**.
3. **Load unpacked** → select this `browser-extension/` folder.
4. Pin the extension.

## Pair with your app
1. In AI DataValidator: **Workspace → UI Extraction** → copy the **pairing token**.
2. In the extension popup → **⚙ Connection settings** → set the **app URL** (e.g.
   `https://www.ai-datavalidator.com`) and paste the **token** → Save.

## Use it
1. Navigate your source app to the page that shows the data (do any login/steps).
2. Click the extension → **Extract a table** → click the table (or **Capture next
   download** and click the site's export).
3. Reopen the extension → review the preview → **Send to AI DataValidator**.
4. In the app: **Workspace → UI Extraction** shows the received dataset.

## Privacy
Data flows only from your browser to **your own** AI DataValidator instance (the URL
you set). No third party, no external service. The pairing token authorises the
upload and maps it to your user.

## Files
- `manifest.json` — MV3 config
- `content.js` — table picker + serializer (runs in the page)
- `background.js` — stores the last extract, download capture, CSV/JSON parsing
- `popup.html` / `popup.js` — the UI, preview, and "Send"

## Automate — record & replay (v0.2, experimental)
In the popup → **🎬 Automate**: click **Record steps**, do your navigation once
(clicks + field entries; passwords are never recorded), then **Stop & save** and
name it. Later, **Run** it — the extension replays the steps (surviving page
navigations) and auto-extracts the resulting table. Selectors are best-effort;
if a step can't be found the run stops with a message. **Needs real-browser
testing** — report any step that misfires.

## Roadmap
- v0.3: AI turns plain-English steps into actions; self-healing selectors
- v0.4: scheduled unattended runs (server-side worker for public/API sources)

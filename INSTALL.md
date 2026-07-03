# Data Validation Agent — Installation Guide

A web-based reconciliation and data-quality tool. Upload two files (any format),
compare them intelligently, run quality checks, map columns, detect PII, trace
lineage, and chat with an AI assistant — all powered by Amazon Bedrock.

---

## 1. Prerequisites

- **Python 3.10+** (for running from source) — or just the shipped `.exe` (no Python needed)
- An **AWS account** with Amazon Bedrock access (Claude model enabled)
- For MSSQL data sources: **ODBC Driver 17/18 for SQL Server** on the host
- Windows only, for Outlook email: Microsoft Outlook installed

---

## 2. Run from source (development)

```bash
# 1. Install dependencies
pip install -r requirements.txt
# On Jefferies machines:
# pip install -r requirements.txt -i https://jfrog.corp.jefco.com/artifactory/api/pypi/Python-Remote/simple

# 2. Create your .env
copy .env.template .env          # Windows
# cp .env.template .env          # Linux/Mac
#   then edit .env and set at minimum:
#     AWS_PROFILE, AWS_DEFAULT_REGION, BEDROCK_MODEL_ID

# 3. Launch
python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# 4. Open http://localhost:8000
```

In development, if `LICENSE_PUBLIC_KEY` is left blank the app runs with **all
features unlocked** (dev mode).

---

## 3. Enabling login (optional)

Login auth turns on automatically once at least one user exists:

```bash
python make_user.py admin "your-password"
```

This writes a bcrypt hash to `users.json`. Delete that file to disable login.
Sessions use HttpOnly cookies signed with `JWT_SECRET` (set a strong value in `.env`).

---

## 4. Configuration reference (`.env`)

| Variable | Purpose |
|----------|---------|
| `AWS_PROFILE`, `AWS_DEFAULT_REGION` | AWS credentials/region for Bedrock |
| `BEDROCK_MODEL_ID` | Inference profile / model ARN |
| `LICENSE_KEY` / `LICENSE_PUBLIC_KEY` / `LICENSE_SERVER_URL` | Licensing |
| `LICENSE_GRACE_DAYS` | Offline grace period (default 7) |
| `JWT_SECRET` | Signs login session cookies |
| `WORKSPACE_DB` | `sqlite` (default) or `mssql` |
| `WORKSPACE_SQLITE_PATH` / `MSSQL_SERVER` / `MSSQL_DATABASE` | DB location |
| `EMAIL_FROM`, `SMTP_HOST`, `SMTP_PORT` | Email reports (blank SMTP_HOST = Outlook on Windows) |
| `WORKSPACE_DEV_USER` | Force a username for local dev |

---

## 5. Licensing

The provider issues an **RS256 JWT** from the license server:

```bash
# provider side, one-time key generation:
openssl genrsa -out license_private.pem 2048
openssl rsa -in license_private.pem -pubout -out license_public.pem

# run the license server:
set LICENSE_PRIVATE_KEY_PATH=license_private.pem
set LICENSE_ADMIN_TOKEN=<long-secret>
python -m uvicorn license.license_server:app --port 9000

# issue a token:
curl -X POST http://localhost:9000/api/issue ^
  -H "x-admin-token: <long-secret>" ^
  -H "Content-Type: application/json" ^
  -d "{\"client_id\":\"ACME\",\"tier\":\"professional\",\"valid_days\":365}"
```

Put the returned `token` into the client's `LICENSE_KEY` (or paste it in
**Settings → Activate**), and bake the **public** key into `LICENSE_PUBLIC_KEY`.

**Tiers:** `starter` (compare/quality/profile), `professional` (+ parse,
governance, mapping, lineage, workspace), `enterprise` (+ api, sso).

---

## 6. Building the distributable `.exe`

```bash
pip install pyarmor nuitka
python build.py                 # full build (PyArmor + Nuitka)
python build.py --no-pyarmor    # skip obfuscation
python build.py --version 1.2.0 # set version
```

Produces `dist/datavalidation-agent.exe` and a zipped package
`dist/datavalidation-agent-v<VERSION>.zip` containing the exe, `templates/`,
`web.config`, `.env.template`, and this guide.

---

## 7. IIS deployment (Windows)

1. Install the **HttpPlatformHandler** IIS module.
2. Unzip the package to `D:\Apps\datavalidation\`.
3. Edit `web.config` — set `processPath`, `APP_ROOT`, AWS and license values.
4. Create the site in IIS pointing at that folder.
5. For Windows Authentication, enable it on the site; IIS forwards the user via
   the `X-Remote-User` header, which the app reads automatically.

Logs go to `C:\Logs\datavalidation\app.log` (configurable in `web.config`).

---

## 8. Troubleshooting

- **402 Payment Required** → feature not in your license tier; check Settings.
- **AI chat shows `[AI unavailable …]`** → AWS/Bedrock not configured or no access.
- **MSSQL connection fails** → install ODBC Driver 17/18; verify network/firewall.
- **Workspace tab empty / 503** → the workspace module failed to import; check the
  startup log for the reason (a missing optional driver only matters at fetch time).

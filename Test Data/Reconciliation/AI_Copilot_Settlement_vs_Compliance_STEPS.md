# AI Copilot demo — Settlement export vs Compliance extract

**Same 8 trades, expressed completely differently.** This is the *AI Copilot*
scenario: the two files are NOT apple-to-apple, so you describe (in plain
English) how to align them, and the AI authors the rules; the deterministic
engine then reconciles.

- **File A — `settlement_export.csv`** (settlement system): `Deal Ref, Security, B/S, Units, Unit Price, Settlement Ccy, Settlement Date, Counterparty`
- **File B — `compliance_extract.csv`** (regulatory extract): `txn_id, isin, instrument_name, direction, quantity, gross_amount, ccy, trade_settle_dt, cpty_name`

## What differs (why Standard mode can't do this)

| # | Field | Settlement (A) | Compliance (B) | Cleanup needed |
|---|---|---|---|---|
| 1 | **Key** | `TRD-1001` | `1001` | Strip `TRD-` prefix to match |
| 2 | **Security / ISIN** | `Apple Inc (US0378331005)` (combined) | `isin` + `instrument_name` (split) | Extract ISIN from parentheses |
| 3 | **Side** | `B` / `S` | `BUY` / `SELL` | Map B→BUY, S→SELL |
| 4 | **Quantity** | `"10,000"` (comma) | `10000` | Strip commas |
| 5 | **Amount** | *(none — has Units & Unit Price)* | `gross_amount` `"1,855,000.00"` | Compute Units × Unit Price; strip commas |
| 6 | **Date** | `17-Mar-2026` (DD-Mon-YYYY) | `20260317` (YYYYMMDD) | Normalise to one format |
| 7 | **Instrument name** | `Apple Inc` | `APPLE INC` (upper) | Compare case-insensitively (or exclude) |
| 8 | **Counterparty** | `Goldman Sachs` | `Goldman Sachs & Co.` | **Fuzzy** match |
| 9 | **Currency** | `Settlement Ccy` | `ccy` | Rename to align |

## Cleanup steps — type these in the AI Copilot chat (Reconciliation → ⚡ AI Copilot)

Say it in one go, or teach each with `remember: ...`:

1. `remember: the key is Deal Ref with the "TRD-" prefix removed, matched to txn_id`
2. `remember: extract the ISIN from the Security column (the code inside the parentheses) and map it to isin`
3. `remember: map B/S to direction where B = BUY and S = SELL`
4. `remember: strip commas from Units and from gross_amount`
5. `remember: compute Amount = Units * Unit Price on the settlement file and compare it to gross_amount`
6. `remember: settlement Settlement Date is DD-Mon-YYYY and compliance trade_settle_dt is YYYYMMDD — normalise both to the same date`
7. `remember: map Settlement Ccy to ccy`
8. `remember: compare instrument names case-insensitively`
9. `remember: fuzzy match Counterparty against cpty_name`

Then type `run recon` (or click Run in AI Copilot mode).

## Expected result after cleanup

Matches on trades **1001, 1003, 1006** (clean). Real breaks surfaced:
- **1002** — amount break: settlement computes 2,051,000 vs compliance 2,052,000.
- **1004** — side break: settlement `S` (SELL) vs compliance `BUY`.
- **1005** — counterparty fuzzy: `BNP Paribas` vs `B.N.P. Paribas SA` (same entity — matches with fuzzy on, would break without it).
- **1007** — only in settlement (Nestle) — no compliance record.
- **1008** — only in compliance (Toyota) — no settlement record.

Once saved, these rules **auto-apply** next time this schema pair is uploaded.

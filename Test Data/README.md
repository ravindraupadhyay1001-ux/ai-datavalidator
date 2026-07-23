# Test Data — demo & testing datasets

Sample BFSI datasets for each module, in a **mix of formats** (CSV, Excel, JSON,
XML, TXT) so demos also prove the app ingests every format. All data is
**synthetic** — no real customer information.

---

## Reconciliation/  — `source_trades.csv` (CSV) vs `target_trades.xlsx` (Excel)
Two deliberately **misaligned** trade files — the ideal Standard-vs-AI-Copilot demo.
- Source has `First` + `Last`; target has `Full Name`.
- Dates: source `DD/MM/YYYY`, target `MM/DD/YYYY`.
- Amounts: source `"1,250,000"` (comma text), target `1250000` (plain).
- **AI Copilot rule to type:** `combine First and Last into Full Name and key on TradeID; strip commas from Notional; source dates are DD/MM, target MM/DD`.
- **Expected result:** most rows match; **T1002** notional break, **T1005** side break,
  **T1008** currency break; **T1004** only in source; **T1011** only in target.

## Data Quality/  — `customer_accounts.xlsx` (Excel)
One file seeded with issues to light up the DQ report:
- **Completeness:** blank name (ACC005), blank IBAN/balance (ACC003).
- **Uniqueness:** duplicate `ACC001`.
- **Validity:** bad email (ACC003), negative balances (ACC004, ACC009), bad date `2024-13-45` (ACC006), invalid currency `XYZ` (ACC007).
- **BFSI validators:** malformed ISIN `INVALID123` and LEI `ABC` (ACC004).
- **Consistency:** `Active` / `active` / `ACTIVE` casing.

## Data Profile/  — `transactions.json` (JSON)
20 clean transactions with varied column types — shows semantic-type inference,
cardinality, top values, and key-candidate detection (`TxnID` unique key;
`Currency`/`Channel`/`Status` low-cardinality categoricals; `Amount` numeric; `TxnDate` dates).

## Governance/  — `customer_pii.csv` (CSV)
PII-rich file to demonstrate detection, classification, regulatory mapping, and masking:
names, emails, phones, national IDs, DOB, addresses, credit-card numbers, IBANs.
(Card numbers are standard **test** numbers, e.g. `4111 1111 1111 1111`.)

## Parse/  — `trade_confirmations.txt` and `swift_mt103.txt` (unstructured)
- `trade_confirmations.txt` — narrative trade confirmations → parse into a structured table.
- `swift_mt103.txt` — SWIFT MT103 payment messages → extract fields (ref, amount, parties).

## Cross Reference/  — 4 sources, 4 formats
`front_office.csv` · `back_office.xlsx` · `custodian.json` · `exchange_feed.xml` —
match security **ISIN** across all four (great format-agnostic demo).
- **Full coverage:** `US0378331005` (all 4 sources).
- **Gaps:** MSFT missing custodian; Alphabet missing back+exchange; BAE only front+exchange; BNP missing front+exchange.
- **Conflict:** `DE0007164600` (SAP) quantity — 3000 in front/custodian vs **3500** in back office.

# Data Handling & AI Governance

How AI DataValidator processes data, what does and does not leave the server,
and how to configure it for regulated (BFSI) clients.

## 1. Core principle: the deterministic engine computes everything

The LLM's only role is to **author rules** — translate plain-English
instructions or schema hints into JSON parameters. **Every result** (every
reconciliation break, data-quality score, match, and profile statistic) is
computed on the application server by deterministic code
(`compare_dataframes`, the DQ / profile / governance engines).

**The full dataset — all rows and all values — is never sent to any AI
provider.** It is read, processed, and stored only within the application's own
infrastructure.

## 2. What actually reaches an LLM provider (and how it's protected)

Only two narrow things are ever sent to an external model, and both are
minimised and masked first:

1. **Schema + tiny samples for column mapping / suggestions** — column names
   plus **up to 5 sample values per column** (`_llm_lineage_mapping`). Before
   they are sent, a **local PII scan** runs (`_pii_columns_for_masking`) and any
   flagged column is redacted by `_mask_value`:
   - email → `j***@bank.com`
   - SSN → `***-**-1234`
   - credit card → `****-****-****-1234`
   - IBAN → `GB12-****-****-7890`
   - name → `[MASKED]`, address → `[ADDRESS MASKED]`
   - DOB → `1980-**-**`, phone → `***-***-4455`
2. **Unstructured file text in the Parse module** — swept by
   `_mask_raw_text_pii`, redacting email / phone / SSN / credit-card / IBAN
   patterns before parsing.

All other processing — fuzzy matching, every DQ validator, profiling, and the
governance PII detector — is 100% local and deterministic (no LLM).

**Reference-document semantic search runs locally too.** When users upload a data
dictionary / rulebook / mapping spec, the Copilot can search it semantically
(synonyms, paraphrases) using a small **on-server ONNX embedding model**
(`fastembed`). The reference text is embedded and matched entirely within the
application — **no embedding API, no data leaves the box**. It falls back to
keyword search if the model can't load. Cache the model on the persistent volume
via `FASTEMBED_CACHE`.

## 3. Provider governance — `DPA_ONLY`

The free LLM tiers (Groq, OpenRouter, Gemini free) may retain prompts or use
them for training and typically carry **no data-processing agreement (DPA)**.

Set the environment variable **`DPA_ONLY=true`** for regulated data. When on,
the app will only ever contact a DPA-capable provider — **AWS Bedrock**
(runs inside your own AWS tenancy), **OpenAI**, or **Anthropic** (both offer
enterprise DPAs / zero-retention) — regardless of what `LLM_PROVIDER` or the
Settings page selects. If none is configured, AI features fail closed with a
clear error rather than silently falling back to a free tier.

Recommended for BFSI: **Bedrock in the client's own AWS account** keeps even the
masked schema samples inside their cloud tenancy.

## 4. Other controls in place

- **Per-user data isolation** — each user's workspace and Dataset Memory rules
  are fully segregated.
- **Authentication & access control** — bcrypt-hashed passwords, role-based
  access (Admin / Workspace User / Run Only), subscription-expiry gating.
- **Audit trail** — admin actions (role changes, blocks, data clean-up,
  deletes, token-cap changes, user creation) are logged.
- **Right-to-erasure** — admin "Clean up" purges a user's workspace + rules;
  user delete removes the account and its data.

## 5. Honest caveats (deployment responsibilities)

- **PII masking is regex/heuristic** — strong, but not a guarantee; an unusual
  custom identifier could appear in a sample value.
- **Encryption at rest** for the SQLite/workspace volume depends on the hosting
  platform, not the application.
- **Encryption in transit** — the hosting platform (e.g. Railway) terminates
  TLS (HTTPS).
- **Retention** — no automatic deletion schedule beyond manual clean-up; define
  a retention policy per client contract.

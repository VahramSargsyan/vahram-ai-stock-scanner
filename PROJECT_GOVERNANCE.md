# Vahram AI Stock Scanner Project Governance

GOVERNANCE_VERSION: **VAHRAM_APP_GOVERNANCE v1.1.0**  
CANONICAL_SOURCE: `VahramSargsyan/vbos-app/docs/governance/VAHRAM_APP_UNIVERSAL_GOVERNANCE_v1.1.0.md`  
LAST_SYNC: **2026-10-05**

**Universal authority override:** v1.1.0 is the current cross-project baseline. Any v0.4.9 language below is retained only as historical/local provenance. Local rules may be stricter but must not silently weaken v1.1.0.

Local profile: **STOCK_STRATEGY_SCANNER**  
Status: ACTIVE

## Mandatory workflow modes

Every task selects one mode before changes:

- AUDIT_ONLY
- STRESS_TEST_ONLY
- DIAGNOSTIC_ONLY
- PATCH_FIX
- IMPLEMENT_FEATURE
- BUILD_NEW_APP
- ECOSYSTEM_PLANNING
- PRODUCTION_OBSERVATION
- FULL_REBUILD_ALLOWED

FULL_REBUILD_ALLOWED requires explicit Vahram approval.

## P0 rules

- Understand the current project before modifying it.
- Preserve working legacy behavior.
- Audit/stress-test/diagnostic requests do not authorize fixes.
- PATCH_FIX stays bounded to the declared problem.
- No full rewrite without explicit permission.
- Data schema, IDs or relations require MIGRATION_PLAN + rollback when an operational database/backend is affected.
- Never claim runtime verification from static inspection.
- Never commit secrets, API keys, broker credentials or Telegram tokens.
- Unknown/ambiguous OSS license means no direct code copying.
- Keep canonical data/evidence reproducible.

## Documentation-first gate

Before a major feature, strategy family, shared engine, architecture change or cross-module integration define:

1. objective;
2. source of truth;
3. files/modules allowed to change;
4. dependencies;
5. data/schema impact;
6. acceptance tests/evidence;
7. rollback/fallback.

Small isolated fixes may keep this contract in the PR description.

## REUSE_FIRST -> VERIFY_BEFORE_ADOPT

For generic functionality, inspect mature OSS before building from zero.

Before adoption record:

- source repository;
- commit/tag/path;
- license;
- architecture fit;
- security/dependencies;
- adoption mode: REUSE_DIRECTLY / ADAPT / REFERENCE_ONLY / REJECT;
- local tests;
- notices and residual risks.

Default:
- MIT/BSD/Apache -> possible ADAPT after review.
- GPL/AGPL/custom/copyleft -> REFERENCE_ONLY unless compatibility is explicitly accepted.
- no license -> REFERENCE_ONLY.

## Analytical evidence rules

Every result that may influence an investment strategy should record enough provenance to reproduce it:

- code/strategy version;
- data source;
- retrieval/test period;
- parameters;
- benchmark where relevant;
- assumptions;
- known gaps;
- exact validation level.

Do not overwrite historical conclusions silently. Material methodology changes create a new version.

## Future Google Sheets / Apps Script integration

If this project later becomes part of the Investment Lab backend:

- Google Sheet is treated as a database, not a manual-edit workflow;
- stable IDs and ID-only relations are preferred;
- known-ID paths must not use unbounded full scans;
- Sheets I/O is batch + bounded;
- canonical/index/cache/derived layers remain separate;
- diagnostics exist from day one;
- production schema changes require migration + backup + rollback + rehearsal.

## Testing honesty

Use exact evidence labels such as:

- STATIC_ONLY
- LOCAL_TESTED
- DATA_PIPELINE_TESTED
- BACKTEST_EXECUTED
- OUT_OF_SAMPLE_TESTED
- GOOGLE_APPS_SCRIPT_TESTED
- LIVE_RUNTIME_VERIFIED

Do not report a generic PASS that implies a stronger test than actually ran.

## Project-specific scope

Current project purpose: configurable stock screening using market/company data and YAML criteria.

Protect:
- configuration-driven filtering;
- separation of downloader / analyzer / reporter;
- historical filter versions;
- exact criteria used for every result.

When strategies are added, each material filter/strategy change must be versioned and evidence-backed before it is treated as accepted.

## Final task report

Report:
- workflow mode;
- what changed;
- files to copy/deploy if any;
- TEST_LEVEL;
- migration requirement;
- post-install/promotion checks;
- residual risks.

## Bounded work adoption — v1.1.0

LOCAL_DELIVERY_PROFILE: RESEARCH_LOCAL
CANONICAL_ADOPTION_DEPENDENCY: VahramSargsyan/vbos-app/docs/governance/VAHRAM_APP_UNIVERSAL_GOVERNANCE_v1.1.0.md
Read `CHAT_WORK_CONTRACT.md` for bounded execution, ordinary in-scope repairs, technical verification before user acceptance, stop/resume and budget rules. Adoption is effective only after the canonical v1.1.0 exists on main and this adapter is merged. Until then use the previous adopted baseline and treat this candidate as a proposal. Repository-specific privacy, source-recovery, research and PROD safeguards remain in force.

`DIAGNOSTIC_ONLY` means read-only investigation. Explicit diagnostic instrumentation uses `PATCH_FIX` with subtype `DIAGNOSTIC_PATCH`; `IMPLEMENT_FEATURE` covers one approved capability in an existing product. NEW_APP_DISCOVERY is an ECOSYSTEM_PLANNING phase. A documentation task does not start product implementation or a pilot automatically.

Check `GOVERNANCE_ADOPTION.md`; document presence is not enforcement verification. Resolve live environments through the local registry. Do not treat historical project attachments or old next-step notes as current permissions.

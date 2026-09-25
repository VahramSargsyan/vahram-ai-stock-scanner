# Governance Source

Local governance version: **VAHRAM_APP_GOVERNANCE v0.4.9**

Canonical investment overlay:
`VahramSargsyan/investment-cases` -> `PROJECT_GOVERNANCE.md`

Upstream origin:
- Vahram APP v0.4.9 workflow/guardrail package
- VBOS `PROJECT_GOVERNANCE.md`
- VBOS `AGENTS.md`
- VBOS Golden Architecture and data-access rules

This repository keeps a local self-contained governance adapter so it can be developed safely without requiring a cross-repository runtime dependency.

When governance changes materially:
1. update canonical Investment Lab governance;
2. bump governance version;
3. sync affected repositories;
4. record project-specific deviations;
5. never allow silent rule drift.

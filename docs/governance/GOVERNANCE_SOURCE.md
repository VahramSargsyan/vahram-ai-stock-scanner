# Governance Source

Local governance version: **VAHRAM_APP_GOVERNANCE v0.4.9**

Origin:
- Vahram APP v0.4.9 workflow/guardrail package
- VBOS `PROJECT_GOVERNANCE.md`
- VBOS `AGENTS.md`
- VBOS Golden Architecture and data-access rules

This repository keeps a local self-contained governance adapter so it can be developed safely without requiring a cross-repository runtime dependency.

When the governance family changes materially:
1. bump the governance version;
2. sync affected repositories;
3. record project-specific deviations;
4. never allow silent rule drift.

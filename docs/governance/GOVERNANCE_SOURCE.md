# Governance Source

Universal governance version: **VAHRAM_APP_GOVERNANCE v1.0.0**

Canonical universal source:
`VahramSargsyan/vbos-app/docs/governance/VAHRAM_APP_UNIVERSAL_GOVERNANCE_v1.0.0.md`

Canonical investment-specific overlay:
`VahramSargsyan/investment-cases` -> `PROJECT_GOVERNANCE.md`

Local profile: **STOCK_STRATEGY_SCANNER**

Historical origin includes the v0.4.9 APP workflow/Golden Architecture package plus later VBOS/HMT governance improvements consolidated into v1.0.0.

This repository keeps a small local governance adapter so it remains safe and understandable when opened independently.

Local rules may be stricter than v1.0.0 but must not silently weaken it.

## Sync rule

When universal governance changes materially:
1. update the canonical universal governance;
2. bump the governance version;
3. update the central GOVERNANCE_REGISTRY;
4. sync affected active repositories;
5. record project-specific deviations.

Silent divergence is not allowed.

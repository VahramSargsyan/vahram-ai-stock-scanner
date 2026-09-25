# Instructions for AI maintainers

GOVERNANCE_VERSION: **VAHRAM_APP_GOVERNANCE v1.0.0**  
CANONICAL_SOURCE: `VahramSargsyan/vbos-app/docs/governance/VAHRAM_APP_UNIVERSAL_GOVERNANCE_v1.0.0.md`  
LAST_SYNC: **2026-09-25**

Read the universal governance when accessible, then apply this repository's local rules. If the private canonical source is unavailable, the local P0 rules below remain fail-safe authority.

Read `PROJECT_GOVERNANCE.md` before implementation work.

Mandatory:
- select workflow mode first;
- preserve legacy behavior;
- AUDIT/STRESS/DIAGNOSTIC do not authorize fixes;
- no full rewrite without explicit permission;
- use REUSE_FIRST with license/provenance review;
- do not commit secrets;
- keep analytical outputs reproducible;
- report exact TEST_LEVEL and residual risks.

Project-specific: keep YAML/filter criteria versioned. Scanner results must identify which filter/strategy version produced them.
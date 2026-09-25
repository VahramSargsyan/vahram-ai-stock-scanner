# Instructions for AI maintainers

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

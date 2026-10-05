# Instructions for AI maintainers

GOVERNANCE_VERSION: **VAHRAM_APP_GOVERNANCE v1.1.0**  
CANONICAL_SOURCE: `VahramSargsyan/vbos-app/docs/governance/VAHRAM_APP_UNIVERSAL_GOVERNANCE_v1.1.0.md`  
LAST_SYNC: **2026-10-05**

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

## Mandatory HI_V1 producer telemetry

Every AI maintainer chat that creates or updates a development PR must act as an HI_V1 telemetry producer.

At task start create one stable `SESSION_ID` for the current chat/task and reuse it across every PR/repository touched by that chat/task. For each Asia/Yerevan local date, keep cumulative counts of the user's actual interventions in this chat:

- `NEXT = 1`
- `QUICK_ACCEPT = 1`
- `CLARIFY = 2`
- `TEST = 3`
- `TEST_DEEP_OR_BUG_EVIDENCE = 4`
- `LOGIC_OR_DESIGN_CHANGE = 5`

Classification is contextual. A one-word "готово"/"ок" after a requested runtime test inherits TEST/TEST_DEEP. Reading/re-reading without advancing, validating, or changing development is not an intervention.

When creating a PR, include:

`<!-- HI_V1 {"date":"YYYY-MM-DD","SESSION_ID":"<stable-chat-task-id>","REVISION":1,"NEXT":0,"QUICK_ACCEPT":0,"CLARIFY":0,"TEST":0,"TEST_DEEP_OR_BUG_EVIDENCE":0,"LOGIC_OR_DESIGN_CHANGE":0,"ACCEPTED_CAPABILITIES":0} -->`

If later user interactions change the counts, increment `REVISION` and update the PR body before merge/final completion. Reuse the same SESSION_ID across all PRs from the same chat/task; Analytics Hub deduplicates by SESSION_ID/date and highest REVISION.

If required earlier chat context is unavailable, never invent zero. Include:

`<!-- HI_V1_CONTEXT_GAP {"date":"YYYY-MM-DD","SESSION_ID":"<stable-chat-task-id>","reason":"CHAT_CONTEXT_UNAVAILABLE"} -->`

Never silently omit telemetry. A PR with a failing HI_V1 telemetry gate must not be merged; fix the marker or declare an explicit HI_V1_CONTEXT_GAP first. Do not create a separate analytics PR, commit, or GitHub Action just to send HI_V1 data.


## Bounded work adoption — v1.1.0

LOCAL_DELIVERY_PROFILE: RESEARCH_LOCAL
CANONICAL_ADOPTION_DEPENDENCY: VahramSargsyan/vbos-app/docs/governance/VAHRAM_APP_UNIVERSAL_GOVERNANCE_v1.1.0.md
Read `CHAT_WORK_CONTRACT.md` for bounded execution, ordinary in-scope repairs, technical verification before user acceptance, stop/resume and budget rules. Adoption is effective only after the canonical v1.1.0 exists on main and this adapter is merged. Until then use the previous adopted baseline and treat this candidate as a proposal. Repository-specific privacy, source-recovery, research and PROD safeguards remain in force.

`DIAGNOSTIC_ONLY` means read-only investigation. Explicit diagnostic instrumentation uses `PATCH_FIX` with subtype `DIAGNOSTIC_PATCH`; `IMPLEMENT_FEATURE` covers one approved capability in an existing product. NEW_APP_DISCOVERY is an ECOSYSTEM_PLANNING phase. A documentation task does not start product implementation or a pilot automatically.

Check `GOVERNANCE_ADOPTION.md`; document presence is not enforcement verification. Resolve live environments through the local registry. Do not treat historical project attachments or old next-step notes as current permissions.

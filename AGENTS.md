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

Never silently omit telemetry. Do not create a separate analytics PR, commit, or GitHub Action just to send HI_V1 data.

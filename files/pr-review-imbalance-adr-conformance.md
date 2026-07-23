<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# ADR-conformance sweep — apache/airflow open PRs

Read-only demonstration of the review-imbalance step's §2c ADR-conformance check:
each PR's diff judged against its area's `adr/` Architecture Decision Records.
**Nothing was posted or changed.** 18 actionable PRs sampled from the areas most
likely to test an architectural decision.

## Results

| # | area | verdict | ADR(s) | note |
|---|---|---|---|---|
| 69403 | execution_api | CONFORMS | exec 1/4/5 | new endpoint with `.didnt_exist` VersionChange + full end-to-end wiring; reuses existing scope |
| 69585 | execution_api | CONFORMS | — | route-matching bugfix (`{id:path}`), no wire-shape change |
| 67637 | serialization | **CONTRADICTS** | ser ADR-3 (fixable) | orphan `inlets` on `SerializedDAG` only (parity drift); success callback writes to DB directly (→ new exec ADR-6) |
| 69811 | execution_api | CONFORMS | exec 1/4/5 | lazy-router factory; no endpoint/field/scope change |
| 67713 | execution_api | CONFORMS | exec 1/2/3 | new field + VersionChange + downgrade converter + back-compat test |
| 69679 | execution_api | **CONTRADICTS** | exec ADR-1/2/3 (fixable) | adds `execution_timeout` to a payload with NO Cadwyn VersionChange |
| 68863 | models/exec_api | N/A | — | field-validator only; no schema/wire change |
| 69766 | serialization | CONFORMS | ser 3 | exception-arg round-trip fidelity restored |
| 69933 | serialization | N/A | — | topo-sort bugfix; mirrored core+SDK, no serialized-shape change |
| 69091 | serialization | CONFORMS | ser 2 | stable `DAG_PARAM` encoding replacing a `0x…` repr leak (determinism) |
| 68588 | serialization | CONFORMS | ser 1/2/3 | optional field + parity + roundtrip test |
| 70030 | jobs | CONFORMS | — | restores buffer then re-raises; public attr use; (no test — criteria, not ADR) |
| 69973 | jobs | CONFORMS (borderline) | jobs 2 | keeps skip_locked; unlocked metadata write is the shape ADR-2 warns of |
| 69556 | jobs | **CONTRADICTS** | jobs ADR-4 | retries `40P01` deadlocks w/o fixing lock ordering, mocked tests only |
| 69557 | jobs | CONFORMS | jobs 2 | extends advisory-lock coordination to CockroachDB; no weakening |
| 66405 | models/listeners | N/A | — | AIP-97 value object; no state/schema/session concern |
| 70019 | models | CONFORMS | models 2 | composite index via new unreleased revision, SQLite-safe, downgrade present |
| 69270 | executors | CONFORMS | exec(utor) 1 | additive `BaseExecutor.__init__` call; no signature change |

## Summary

- CONFORMS: 12 · CONTRADICTS: 3 · N/A: 3

## Escalations — ADR contradiction changes the disposition

These read as mergeable on a normal review but break an architectural decision:

1. **#69679** — clean, tested feature; missing Cadwyn VersionChange breaks the
   independent-deploy contract (exec ADR-1/2/3). Fixable in place → **draft-back**.
2. **#69556** — retrying `40P01` deadlocks instead of fixing lock ordering, no
   live repro → jobs ADR-4 → **discuss** (the 40001-only half is fine; split it).
3. **#67637** — orphan `inlets` parity drift (ser ADR-3) + a DB-writing success
   callback (motivated new exec **ADR-6**) → **discuss/draft-back**.

## Takeaway

The ADR layer catches "goes against the grain" changes the criteria checklist
alone would pass — the difference between *misses a checkbox* and *breaks an
architectural decision*.

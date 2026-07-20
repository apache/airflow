<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# 3. Client library and API version changes are contained behind the hook

Date: 2026-07-20

## Status

Accepted

## Context

`providers/google/pyproject.toml` declares roughly 44 separate `google-cloud-*`
client libraries — `bigquery`, `dataproc`, `storage`, `aiplatform`,
`orchestration-airflow`, `run`, `batch`, `container`, `pubsub`, `spanner`, `dlp`,
`managedkafka` and the rest — plus `google-api-core`, `google-auth`,
`google-api-python-client`, `google-genai`, `google-ads`, the `gcloud-aio-*`
family, `gcsfs`, `looker-sdk`, `pandas-gbq`, `pyarrow` and `ray`. Each of those
moves on Google's release schedule, not Airflow's, and each can change
request/response shapes, rename fields, or introduce an import-time regression on
a minor release.

The provider is also released independently of core, on its own cadence, to a
very large user base. A dependency floor raised in this file is not a development
convenience — it is shipped to everyone who upgrades the provider, and it is
frequently the *only* change they receive in a given release. The existing floor
comments make this concrete: `google-api-core` is pinned at `>=2.30.3` because
versions 2.28.1–2.30.2 scan the whole virtualenv on import and trip the Dag import
timeout, with the upstream issue linked at the pin site.

The pressure that follows is to let a library change surface upward. A
`google-cloud-*` major renames a request field; the shortest diff renames the
matching operator keyword argument. That diff is small, it makes CI green, and it
is a breaking change for every Dag author using that operator — who did not choose
the upgrade, cannot see it in the operator's documentation, and will discover it
when their Dag fails to parse after a routine provider bump.

The same applies to service API versions the provider addresses explicitly
(Campaign Manager, Display & Video, the Discovery-based clients): the version is a
detail of how the hook talks to Google, not part of the contract the operator
offers.

The hook layer is where this containment belongs, because ADR 1 already puts every
client construction there. `GCSHook.get_conn()` is the only place in the package
that knows `storage.Client` exists; if the response shape changes, that is the
place that should have to care.

## Decision

Third-party client libraries and service API versions are implementation details
of the hook layer. The operator, sensor, transfer and trigger surfaces are the
provider's public contract and change only for reasons a Dag author would
recognise.

- **Absorb library shape changes inside the hook.** When a `google-cloud-*`
  upgrade renames a request field, restructures a response, or moves a symbol, the
  hook adapts and keeps returning what it returned before. Operator arguments,
  XCom payloads and trigger event shapes stay put.
- **A dependency floor bump states what it fixes**, in the PR body, and — where the
  behaviour is user-visible — in `providers/google/docs/changelog.rst`. "CI is
  green now" is not a reason to raise a floor for every user of the provider.
- **A floor raised to work around a known upstream defect carries the reason and
  the upstream issue URL as a comment at the pin site**, as the `google-api-core`
  pin does, so the cap or floor can be revisited when upstream fixes it.
- **Do not upper-bound by default.** A cap follows the parent `providers/adr/`
  rule: a deliberate decision with a tracking issue and the full issue URL at the
  cap site.
- **Service API versions are hook-level defaults**, exposed as an operator
  parameter with a stable default when users need to pin one — not something an
  operator hard-codes in a way that forces a breaking change to move.
- **When an operator surface genuinely must break**, it goes through the
  provider's deprecation process — deprecated with a removal date, kept working
  for at least one release, and called out in the changelog — never as a
  side-effect of a dependency bump.
- **Dependencies declared under `[project.optional-dependencies]` are imported
  lazily or under `TYPE_CHECKING`** (`apache-beam`, `ray`, `cncf.kubernetes`,
  `looker-sdk`). A top-level import of one turns a missing extra into a Dag parse
  failure for everyone. Required dependencies — `google-cloud-*` and friends —
  are imported normally at module top; the test is the `pyproject.toml`
  declaration, not a judgement about how heavy the library feels.

## Consequences

- A user can upgrade the provider to pick up a fix in one service without their
  Dags breaking because an unrelated Google library shipped a major.
- Upgrade work concentrates in the hooks, where it is testable against a mocked
  client and reviewable by someone who knows that one service — rather than being
  spread across the 47 operator modules under `cloud/operators/`.
- Some hooks carry translation code that exists only to preserve an older external
  shape. That is the cost of the guarantee and is accepted.
- Floors move more slowly and with more justification than "the latest works on my
  machine", which occasionally delays picking up an upstream improvement.

A change **violates** this decision when it:

- renames, retypes or removes an operator / sensor / transfer parameter, or changes
  an XCom or trigger event shape, because a `google-cloud-*` library changed its
  own;
- raises a dependency floor with no statement of what it fixes, or adds an upper
  bound with no tracking issue and no issue URL at the pin site;
- hard-codes a service API version in an operator such that changing it becomes a
  user-visible break;
- removes or repurposes a public operator symbol in the same change that
  introduces its replacement, skipping the deprecation period;
- imports a dependency declared under `[project.optional-dependencies]` in
  `providers/google/pyproject.toml` at module top level, in a module that is
  imported during Dag parsing.

## Evidence

- #67774 — "Bump minimum `google-api-core` to 2.30.3 to fix Dag import timeout": a
  floor raised for a stated, user-visible reason, with the upstream issue recorded
  at the pin site. The model this decision asks for.
- #69032 — "Fix GKE provider 401 on kubernetes client 36.x": a third-party client
  major breaking a working provider, fixed inside the provider rather than by
  changing what users pass to the operator.
- #66632 — "Bump `google-cloud-aiplatform` to force upgrade of litellm" and #64786 —
  "Bump `google-cloud-aiplatform[evaluation]>=1.145.0`": transitive-dependency
  pressure landing on the provider's floors, with no operator-surface change.
- #65130 — "Add engine flag support to Dataproc `ClusterGenerator` and bump
  `google-cloud-dataproc` to 5.27.0": a library bump paired with the feature it
  enables, added as a new option rather than by reshaping the existing surface.
- #62510 — "Upgrade version of Campaign Manager API to v5" and #64265 — "Update
  default api version of campaign manager sensor": a service API version moved as a
  hook/operator default rather than as a break.
- #61124 — "Move exception handling from Google Bigtable operators to hooks":
  service-library error handling relocated to the layer that owns the client.
- #67113 — "Deprecate implicit legacy SQL default in BigQuery operators", with
  #62802 — "Delete google provider deprecated items scheduled for Jan 2026",
  #64098 — "Remove deprecated classes scheduled for March 2026" and #66526 —
  "Remove MLEngine from google provider": the deprecate-then-remove path a genuine
  surface break is required to take.
- #65659 — "Fix Google Dataflow hook failing import when apache-beam not
  installed": an optional dependency imported where it broke import for users
  without the extra.

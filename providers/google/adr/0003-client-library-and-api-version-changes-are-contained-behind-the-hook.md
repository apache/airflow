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

`providers/google/pyproject.toml` declares ~44 `google-cloud-*` client libraries
plus `google-api-core`, `google-auth`, `google-api-python-client`, `google-genai`,
`google-ads`, the `gcloud-aio-*` family, `gcsfs`, `looker-sdk`, `pandas-gbq`,
`pyarrow` and `ray`. Each moves on Google's schedule, not Airflow's, and can change
request/response shapes, rename fields, or introduce an import-time regression on a
minor release. The provider is released independently of core to a very large user
base, so a floor raised in this file ships to everyone who upgrades — often the
*only* change they receive. The `google-api-core` `>=2.30.3` pin is the concrete
example: 2.28.1–2.30.2 scan the whole virtualenv on import and trip the Dag import
timeout, with the upstream issue linked at the pin site.

The pressure is to let a library change surface upward — a `google-cloud-*` major
renames a request field and the shortest diff renames the matching operator
keyword. That is small and green, and a silent breaking change for every Dag author
using the operator after a routine bump. The same holds for explicitly-addressed
service API versions (Campaign Manager, Display & Video, Discovery clients): the
version is how the hook talks to Google, not the operator's contract. Containment
belongs in the hook because ADR 1 already puts every client construction there —
`GCSHook.get_conn()` is the only place that knows `storage.Client` exists.

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

- A user can upgrade the provider for a fix in one service without their Dags
  breaking because an unrelated Google library shipped a major.
- Upgrade work concentrates in the hooks — testable against a mocked client,
  reviewable by someone who knows that service — not spread across the 47 operator
  modules under `cloud/operators/`.
- Some hooks carry translation code only to preserve an older external shape — the
  accepted cost of the guarantee.
- Floors move more slowly and with more justification, occasionally delaying an
  upstream improvement.

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

- #67774 — bumped `google-api-core` to 2.30.3 to fix the Dag import timeout, for a
  stated user-visible reason with the upstream issue at the pin site: the model.
- #69032 — a kubernetes-client 36.x major breaking GKE, fixed inside the provider,
  not by changing what users pass to the operator.
- #66632, #64786 — transitive-dependency pressure landing on floors, no
  operator-surface change.
- #65130 — a `google-cloud-dataproc` bump paired with its feature, added as a new
  option rather than reshaping the existing surface.
- #62510, #64265 — a Campaign Manager API version moved as a hook/operator default,
  not a break.
- #61124 — Bigtable error handling relocated to the layer that owns the client.
- #67113, #62802, #64098, #66526 — the deprecate-then-remove path a genuine surface
  break must take.
- #65659 — an optional dependency (`apache-beam`) imported where it broke import
  for users without the extra.

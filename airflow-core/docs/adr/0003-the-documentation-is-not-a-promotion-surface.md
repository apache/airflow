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

# 3. The core documentation is not a promotion surface for third-party products

Date: 2026-07-20

## Status

Accepted

## Context

`airflow.apache.org` is one of the highest-authority documentation domains in the
data ecosystem. A how-to page there, with a working example Dag and a product link,
is worth a great deal to whoever owns the product — referral traffic, search
ranking, and the implicit endorsement of appearing in a vendor-neutral ASF
project's manual — whether or not the page is useful to Airflow's readers. That is
the pressure the project holds a line against.

The submissions arrive in a recognisable form and are not always cynical: a
"tutorial" for calling a commercial API gateway from a TaskFlow Dag, a "failure
analysis guide" walking through one external project filed against an issue its own
author opened, a guide whose Airflow content (connections, retries, pools, XCom) is
correct but wrapped around a named product that could have been any product —
sometimes coordinated across repositories, sometimes one enthusiast who does not see
the problem. The project's answer is structural, not editorial, and predates any
case: **third-party integrations live in providers.** A provider has a maintainer, a
release cadence, a test suite, a changelog, its own docs site, and an acceptance bar
including demonstrated demand — the mechanism that keeps a vendor how-to accurate as
that vendor's API drifts, which nobody in the core project can. Core documentation
documents Airflow: its concepts, configuration, operation, and the patterns that
hold regardless of the external system on the other end. The softer version — a
product name added as an example, a link into a list where a link to Airflow's own
docs would do — is individually small; the accumulation turns a manual into a
directory.

## Decision

Core documentation stays vendor-neutral:

- **No how-to, tutorial, or example Dag whose subject is a specific third-party
  product or service.** Integrations are documented in the relevant provider,
  which brings its own maintenance and release process, or not at all.
- **A pattern is documented generically.** If the useful content is an Airflow
  pattern — connections and secrets, retries, pools, safe XCom payloads,
  observability — write it without a brand attached, using an existing example
  or a neutral placeholder.
- **New third-party integrations follow the provider acceptance path**,
  including demonstrating demand, rather than entering through a documentation
  page.
- **Outbound links earn their place.** Link to Airflow's own documentation
  first; a link to an external project needs a reason a reader would follow it
  that is not "so they know it exists".
- **Coordinated promotional submissions are closed as spam**, without a review
  of their technical content, and repeat campaigns are escalated.

## Consequences

- The manual stays about Airflow, and a tool named in it is named because it is the
  answer, not because someone submitted a PR.
- Genuinely useful vendor-specific material has a higher barrier — writing a
  provider, or publishing on the vendor's own site — and some never gets written;
  the project accepts that, because an unmaintained integration page is a liability
  with a long half-life.
- A good-faith contributor can find the refusal abrupt, since the Airflow parts are
  often correct; redirecting them to a provider, a discussion thread, or the generic
  pattern is the appropriate response.
- Reviewers can decline this class of PR quickly, without arguing the product's
  merits.

A change **violates** this decision when it:

- adds a page, tutorial, or example Dag built around a named commercial or
  third-party product;
- documents an integration with an external system in the core tree rather than
  in a provider;
- inserts a product name, company name, or external project link into existing
  documentation where a generic description or an internal link would serve;
- is one of several coordinated submissions across issues, PRs, or repositories
  promoting the same external project;
- introduces a new integration by way of documentation, bypassing the provider
  acceptance process.

## Evidence

- #62296 (closed) — "Add RAG failure analysis guide and example Dag", closed as part
  of a coordinated promotional campaign, on an issue opened by the project's author.
- #67358 (closed) — a how-to for calling a commercial AI gateway from a TaskFlow
  Dag; the author moved it to a discussion thread and then to an Airflow-native
  pattern framing.
- #63401 (closed) — a Stripe provider withdrawn by its author for lack of other
  adopters: the demand test the provider path applies and a docs page would bypass.
- The existence and structure of `providers/` and its per-provider documentation
  sites: the standing mechanism through which every supported third-party
  integration is documented and maintained.

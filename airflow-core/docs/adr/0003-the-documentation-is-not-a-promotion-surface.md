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

`airflow.apache.org` is one of the highest-authority documentation domains in
the data ecosystem. A how-to page there, with a working example Dag and a link
to a product, is worth a great deal to whoever owns the product — in referral
traffic, in search ranking, and in the implicit endorsement that comes from
appearing in a vendor-neutral ASF project's own manual. That value exists
whether or not the page is useful to Airflow's readers, which is exactly what
makes it a pressure the project has to hold a line against.

The submissions arrive in a recognisable form and are not always cynical. A
"tutorial" for calling a commercial API-gateway product from a TaskFlow Dag. A
"failure analysis guide" that is a walkthrough of one external project, filed
against an issue opened by that project's own author. A guide whose Airflow
content — connections, retries, pools, XCom — is real and correct, wrapped
around a named product that could have been any product. Sometimes several such
PRs and issues appear across repositories in a coordinated pattern; sometimes it
is one enthusiastic contributor who does not see the problem.

The project's answer is structural rather than editorial, and it predates any
individual case: **third-party integrations live in providers.** A provider has
a maintainer, a release cadence, a test suite, a changelog, a documentation
site of its own, and an acceptance bar that includes demonstrating that other
people want it. That is the mechanism through which Airflow documents talking
to an external system, and it is the mechanism that makes the documentation
maintainable — nobody in the core project can keep a vendor how-to accurate as
that vendor's API drifts.

Core documentation, by contrast, documents Airflow: its concepts, its
configuration, its operation, and the patterns that hold regardless of which
external system is on the other end of the connection. When the useful content
of a proposed page is "use a Connection, set retries, keep secrets out of the
Dag", that content belongs in the generic guidance, unattached to a brand.

The same reasoning applies to the softer version — a product name added as an
example, a link inserted into a list, a reference to an external blog or tool in
a place where a link to Airflow's own documentation would do. Individually
these are small; the accumulation is what turns a manual into a directory.

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

- The manual stays about Airflow, and readers can trust that a tool named in it
  is named because it is the answer, not because someone submitted a PR.
- Genuinely useful vendor-specific material has a higher barrier than a docs PR
  — writing a provider, or publishing on the vendor's own site — and some of it
  never gets written. The project accepts that: an unmaintained integration page
  in the core manual is a liability with a long half-life.
- A contributor acting in good faith can find the refusal abrupt, since the
  Airflow parts of their page are often correct. Redirecting them to a provider,
  to a discussion thread, or to the generic pattern is the appropriate response,
  and the project does that.
- Reviewers can decline this class of PR quickly and consistently, without
  arguing the merits of the product.

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

- #62296 (closed) — "Add RAG failure analysis guide and example Dag", closed as
  part of a coordinated promotional campaign for an external project, on an
  issue opened by that project's own author.
- #67358 (closed) — a how-to for calling a commercial AI gateway from a TaskFlow
  Dag; the maintainer response was that this kind of material belongs elsewhere,
  and the author moved it to a discussion thread and then to an
  Airflow-native pattern framing.
- #63401 (closed) — a Stripe provider withdrawn by its own author for lack of
  other adopters: the demand test that the provider path applies and that a
  documentation page would have bypassed.
- The existence and structure of `providers/` and its per-provider
  documentation sites: the standing mechanism through which every supported
  third-party integration is documented and maintained.

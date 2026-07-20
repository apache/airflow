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

# 6. Input safety is the Dag author's responsibility, not a hardened operator variant

Date: 2026-07-20

## Status

Accepted

## Context

`BashOperator` runs a shell command. `PythonOperator` runs a Python callable.
Both are given their content by a Dag author, and both may render templates into
it — including templates that resolve to values a Dag *run* supplied, such as
`params` or `dag_run.conf`. Interpolating an untrusted value into a shell command
is an injection, and it is a real way for a Dag to be exploited.

Airflow's documented security model already answers who owns that. Dag authors
execute arbitrary code on workers by design; they are a trusted role, responsible
for the safety of the code they submit. Nothing an operator does can change that,
because the same Dag author can call `subprocess` directly on the next line.

The proposal that arrives against this is a "secure" variant of an existing
operator — a `SecureBashOperator` that escapes, quotes, or parameterises what the
author passed. It has been submitted more than once, framed as mitigating a
published CVE identifier.

It is rejected on three grounds, and all three matter. First, the name is a
claim: shipping `SecureBashOperator` asserts that `BashOperator` is insecure,
which is false — it is safe when used correctly, exactly like every other tool
that runs a command. Second, escaping logic of this kind does not cover every
case, so it delivers a false sense of security, which is worse than none: an
author who believes the operator sanitises input will stop checking. Third, the
concern is not local to Bash. Dozens of operators and hooks across the ecosystem
interpolate author-supplied values into SQL, into API payloads, into paths.
A single place that *possibly* prevents one class of injection is worse than a
consistent, project-wide position that Dag authors are responsible for the values
they interpolate.

Related, and equally firm: attaching a CVE identifier to a PR that is not a fix
for that CVE misrepresents severity, alarms users, and is treated as a serious
conduct problem rather than an over-enthusiastic contribution.

None of this means the project declines to fix injection bugs. The same
security-model section this decision rests on —
"Dag author code passing unsanitized input to operators and hooks", under "What
is NOT considered a security vulnerability" in
`airflow-core/docs/security/security_model.rst` — carries two explicit
exceptions, and they are the cases where a sanitising change is exactly the
right change:

1. the injection is triggerable by a **non-Dag-author** role — an authenticated
   UI user, say — *without* the Dag author having written code that passes that
   input unsafely; and
2. official Airflow documentation recommends the pattern that leads to the
   injection, in which case the guidance itself may warrant an advisory.

A coordinated fix for either of those is a security fix, goes through the ASF
security process, and lands as a normal change to the operator that exists. What
this ADR rejects is the *other* shape: a parallel "secure" class, or unsolicited
laundering of author-supplied content, offered against a risk the security model
already assigns to the Dag author.

## Decision

**Operators in this provider do not attempt to sanitise author-supplied content,
and no "secure" or "hardened" variant of an existing operator is added** —
except where the security model already classifies the exposure as a
vulnerability, in which case the fix goes to the operator that exists, through
the ASF security process.

- **No `Secure*` / `Safe*` / `Hardened*` sibling classes.** Improve the operator
  that exists, or document the safe pattern. A parallel class splits the
  ecosystem and implies the original is defective.
- **Do not escape, quote, or rewrite what the author passed.** `BashOperator`
  runs the command it is given; `PythonOperator` calls the callable it is given.
  Silently transforming that content breaks legitimate Dags and hides the risk
  rather than removing it.
- **Validation that catches authoring mistakes is welcome; input laundering is
  not.** Rejecting a malformed argument with a clear error at the earliest point
  it can be detected is good operator design. Accepting dangerous input and
  quietly making it "safe" is not.
- **The answer to injection risk is documentation and deployment controls** —
  who may author Dags, what reaches `params` / `dag_run.conf`, and the guidance
  in the security model — not an operator feature.
- **Security concerns follow the ASF security process.** Do not open a PR or an
  issue claiming a vulnerability, and never attach a CVE identifier to a change
  that is not the coordinated fix for it.

## Consequences

- One `BashOperator`, with one documented contract, and no implied two-tier
  ecosystem where "the safe one" exists but almost nobody uses it.
- Users are not given a guarantee the implementation cannot keep. The risk stays
  where it can actually be managed: with the person writing the Dag and the
  deployment manager deciding who that person is.
- The honest cost: an author who interpolates untrusted input into a command gets
  no help from Airflow, and this decision declines to give them any. That is a
  deliberate trade — partial protection here would be relied upon as complete.
- Contributors who arrive wanting to improve Airflow's security posture have to
  be redirected to where it is actually decided: the security model
  documentation, the deployment guidance, and the ASF security process.

**Carve-out, applied before every bullet below:** none of these fire on a
coordinated fix for a genuine vulnerability under either exception in the
"Dag author code passing unsanitized input to operators and hooks" section of
`airflow-core/docs/security/security_model.rst` — an injection reachable by a
non-Dag-author role without the author passing input unsafely, or a pattern
official Airflow documentation recommends. Such a change is handled through the
ASF security process, and the bullets on rewriting rendered values and on
single-operator sanitisation do not apply to it.

A change **violates** this decision when it:

- adds an operator whose name or docstring positions it as the secure
  alternative to an existing one;
- escapes, quotes, tokenises, or otherwise rewrites a templated field's rendered
  value before executing it, absent the carve-out above;
- adds a parameter that switches an operator between "safe" and "unsafe"
  handling of author-supplied content;
- claims in its title, body, or changelog that an existing standard operator is
  insecure, or attaches a CVE identifier to a change that is not that CVE's
  coordinated fix;
- introduces sanitisation in one operator for a risk that is equally present in
  many others, without a project-wide decision — again, absent the carve-out
  above, which is scoped to one identified vulnerability by definition.

## Evidence

- #66457 — "Add `SecureBashOperator` with automatic template parameterization",
  citing a CVE identifier in the title: closed, with an explicit warning about
  misrepresenting severity by attaching a CVE identifier to an unrelated change.
- #66478 — the same operator resubmitted: closed with the full reasoning — the
  name implies `BashOperator` is insecure when it is not, the escaping logic does
  not cover all cases and so gives a false sense of security, and singling out one
  operator is worse than consistently relying on Dag authors knowing what they are
  doing when the same exposure exists across many operators and hooks.
- `airflow-core/docs/security/security_model.rst`, section "Dag author code
  passing unsanitized input to operators and hooks" under "What is NOT
  considered a security vulnerability" — the Dag-author trust boundary this
  decision follows, *and* the two exceptions that define when an injection is a
  vulnerability after all (non-Dag-author trigger; documentation recommending
  the unsafe pattern). Read that section before applying any bullet above.

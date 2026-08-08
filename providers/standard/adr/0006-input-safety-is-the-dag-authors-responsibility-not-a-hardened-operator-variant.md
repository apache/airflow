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

`BashOperator` runs a shell command and `PythonOperator` runs a callable, both
given their content by a Dag author and both able to render templates into it —
including values a Dag *run* supplied (`params`, `dag_run.conf`). Interpolating an
untrusted value into a shell command is an injection and a real way to exploit a
Dag. Airflow's documented security model already assigns that: Dag authors execute
arbitrary code on workers by design, are a trusted role, and are responsible for
the safety of what they submit — nothing an operator does changes that, because the
same author can call `subprocess` on the next line.

The proposal against this is a "secure" variant — a `SecureBashOperator` that
escapes or parameterises what the author passed, submitted more than once framed as
mitigating a published CVE. It is rejected on three grounds: the name is a false
claim that `BashOperator` is insecure (it is safe used correctly); escaping logic
does not cover every case, so it gives a false sense of security worse than none;
and the concern is not local to Bash — dozens of operators and hooks interpolate
author values into SQL, payloads, paths, so one partial guard is worse than a
consistent project-wide position. Equally firm: attaching a CVE identifier to a PR
that is not that CVE's fix misrepresents severity and is a conduct problem.

None of this means the project declines to fix injection bugs. The same
security-model section this rests on — "Dag author code passing unsanitized input
to operators and hooks", under "What is NOT considered a security vulnerability" in
`airflow-core/docs/security/security_model.rst` — carries two explicit exceptions,
the cases where a sanitising change *is* the right change:

1. the injection is triggerable by a **non-Dag-author** role — an authenticated
   UI user, say — *without* the Dag author having written code that passes that
   input unsafely; and
2. official Airflow documentation recommends the pattern that leads to the
   injection, in which case the guidance itself may warrant an advisory.

A coordinated fix for either is a security fix, goes through the ASF security
process, and lands as a normal change to the operator that exists. What this ADR
rejects is the *other* shape: a parallel "secure" class, or unsolicited laundering
of author-supplied content, against a risk the security model assigns to the author.

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

- One `BashOperator`, one documented contract, no implied two-tier ecosystem where
  "the safe one" exists but almost nobody uses it.
- Users are not given a guarantee the implementation cannot keep; the risk stays
  where it can be managed — with the Dag author and the deployment manager deciding
  who that is.
- Honest cost: an author who interpolates untrusted input gets no help from Airflow,
  by deliberate trade — partial protection would be relied on as complete.
- Contributors wanting to improve security posture are redirected to where it is
  decided: the security model docs, deployment guidance, and the ASF security
  process.

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

- #66457 — `SecureBashOperator` citing a CVE in the title, closed with an explicit
  warning about misrepresenting severity by attaching a CVE to an unrelated change.
- #66478 — the same operator resubmitted, closed with the full reasoning: the name
  implies `BashOperator` is insecure when it is not, the escaping does not cover all
  cases (false security), and singling out one operator is worse when the exposure
  spans many operators and hooks.
- `airflow-core/docs/security/security_model.rst`, section "Dag author code passing
  unsanitized input to operators and hooks" under "What is NOT considered a security
  vulnerability" — the Dag-author trust boundary this follows, *and* the two
  exceptions defining when an injection is a vulnerability after all (non-Dag-author
  trigger; documentation recommending the unsafe pattern). Read it before applying
  any bullet above.

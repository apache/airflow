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

# 6. A UI change is proven by running it, with a reproduction on current main and visual evidence

Date: 2026-07-20

## Status

Accepted

## Context

Almost everything this directory produces is *seen* rather than *computed*. A
tooltip one pixel off, a switch outline that vanishes against a dark table, a
calendar cell whose green means "queued" instead of "succeeded" — none of these
show in a diff or are caught by `tsc`, ESLint, or a Vitest prop assertion. The
reviewer either imagines the pixels or checks out the branch and runs a full
Airflow stack, twenty minutes per PR, in an area where a handful of people supply
all the review capacity.

Two failure modes dominate the closed-PR record. The first is **an unproven fix**:
a contributor finds a plausible cause, changes a CSS property or column id, and
opens the PR without ever seeing the symptom or fix in a browser — sometimes it is
not the fix at all, sometimes it fixes the symptom and breaks the same component,
sometimes the defect was already fixed on `main` and reproduced nothing. The second
is **evidence asserted rather than shown**: "I verified the logic locally", "my
Docker environment is failing so I could not grab a screenshot but static checks are
green", or a unit test standing in for a visual claim. An inability to run the UI is
not a caveat; it is the reason the change cannot be reviewed, and help is available
in the project's Slack channels. Reproduction and screenshot are the same
requirement from two ends: *you ran this*.

**This does not extend to string-only and locale-catalogue changes.** Translations
touch no layout, colour, or interaction — there are no pixels to judge.
[ADR-3](0003-user-facing-strings-go-through-the-locale-catalogues.md) calls them "a
routine, welcome contribution" (#68574, #67918) run as an express lane. The i18n
README asks for a UI check only where rendering genuinely changes: "For RTL
languages, languages with significantly different word order, or languages that
typically require much longer text, a UI check is strongly recommended in addition
to file-level review." That carve-in — not this ADR's default — is the rule for
translations, and it is what makes the newcomer guidance ("a locale gap in a
language they actually speak") coherent for contributors who cannot run the UI.

## Decision

Every change to this directory that alters **layout, colour, or interaction** is
demonstrated, not described:

- **Reproduce the defect on current `main` before fixing it.** An issue reported
  against an older release is a hypothesis until it is confirmed against today's
  build. If it does not reproduce, say so on the issue — that is the useful
  contribution — and do not open the PR.
- **Attach before/after screenshots, or a short recording,** for anything with a
  visual or interactive effect. A recording is required where the change is about
  behaviour over time: hover, refresh, polling, navigation, form submission. The
  PR description is the right place, because that is where a reviewer looks first;
  but evidence supplied in a comment on request is evidence, and the PR is not
  rejected for having arrived at it that way. #69741 is exactly this case — it was
  closed *after* the screenshots arrived and showed the defect was not
  reproducible, which is the process working.
- **Show the fix working in a browser, not only in a test.** Tests are still
  required by the area's normal standards; they do not substitute for the
  visual evidence.
- **Check the surrounding surface for regressions your change introduces** —
  alignment, dark mode, right-to-left layout, and the same component at other
  sizes — and show that too when it is plausibly affected.
- **Exempt: string-only and locale-catalogue diffs.** A PR that only adds,
  completes, or corrects entries under `public/i18n/locales/` needs no screenshot.
  The exception to the exception is the i18n README's own carve-in: an RTL locale,
  a language with significantly different word order, or one whose text is
  typically much longer, where the rendering *is* the thing under review and a UI
  check is strongly recommended.
- **If you cannot run the UI locally, that constrains what you can contribute
  here, not whether you can contribute.** A layout, colour, or interaction change
  needs a browser; open it after getting the environment working, and the
  contributor channels exist for exactly that. A locale gap in a language you
  actually speak needs no browser and is open to you today.

## Consequences

- Reviewers can judge most UI PRs from the description alone — the only way this
  area's review capacity stretches to its PR volume.
- The bar turns away a correct one-line fix from someone who could not get Breeze
  running. That cost is accepted: the project cannot distinguish it from the much
  larger population of unverified changes without doing the verification itself.
- Issues get closed as already-fixed earlier, because reproduction happens before
  the code rather than during review.
- Screenshots age. They are evidence for the review, not documentation.

A change **violates** this decision when it:

- alters layout, colour, or interaction and carries no screenshot or recording
  anywhere on the PR — description or comment. A diff confined to
  `public/i18n/locales/` does not trigger this bullet, unless the locale is RTL,
  has significantly different word order, or renders substantially longer text;
- offers a written description of the visual result, a unit-test reference, or a
  claim of local verification in place of an image, for a layout, colour, or
  interaction change;
- states that the environment could not be built or run, and asks for a layout,
  colour, or interaction change to be reviewed or merged anyway;
- fixes an issue reported against an earlier release without confirming the
  behaviour still occurs on current `main`;
- asks a reviewer or maintainer to build, run, or screenshot the change on the
  author's behalf.

## Evidence

- #67054 — a connection-form fix withdrawn by its author after browser testing
  showed current `main` already behaved correctly; the reviewer had asked for a
  video, and the cited test file did not exist in the branch.
- #69741 — the successor PR, closed after screenshots arrived: the defect could not
  be reproduced on 3.x and the change added complexity for no demonstrated gain.
- #57603, #52765 — closed with a maintainer recording, and with a pointer to the
  release that already contained the fix, respectively.
- #65095 — "changing overflow doesn't fix this issue": a plausible CSS change that
  did not address the cause.
- #60584 — a column id proposed as a sorting fix; the maintainer tested it, found
  the behaviour unchanged, and fixed it differently.
- #69159 — closed over a centering regression the change introduced.
- #69114, #64387 — closed after the author reported being unable to run the UI and
  offered a logical argument in place of a screenshot.
- #67314 — an RTL layout fix with no recording, where the reviewer had asked for
  before and after.
- #67402, #67366 — colour and percentage changes where regression tests were offered
  instead of the requested before/after images.
- #64267, #65017, #60105, #66219, #68547, #67594 — reviewers asking for screenshots
  before engaging; #60105 also carries the standing offer of Slack help to run the
  UI locally.

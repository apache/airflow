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

# 3. User-facing strings go through the locale catalogues, and the locale set is a maintained contract

Date: 2026-07-20

## Status

Accepted

## Context

The UI is translated into more than twenty languages. Catalogues live under
`public/i18n/locales/<lang>/`, split into namespaces mirroring the application's
areas (`common.json`, `dag.json`, `dags.json`, and so on). English (`en`) is both
the source and the runtime fallback; other locales load on demand via
`i18next-http-backend`, resolved through `react-i18next`.

Two things make this more than a formatting convention. First, **a hardcoded
string is invisible damage** — it compiles, renders, and passes review in English,
and its only symptom is an untranslated fragment the other locales' users see. The
failure is silent, so it is enforced mechanically: `i18next/no-literal-string`
(`rules/i18next.js`) rejects literals in the component tree, and the key-parity
rules in `rules/i18n.js` compare catalogue structure across locales. Second, **the
locale set is a maintained contract**: `public/i18n/README.md` defines a supported
locale as one with a named *translation owner* (language quality) and a *code owner*
in `.github/CODEOWNERS` (technical review), with an explicit notion of completeness
and of an inactive owner. Nobody can assess a Hebrew, Korean, or Taiwanese Mandarin
string except that locale's owner. Two recurring review issues follow: placeholder
translations — English copied into a non-English catalogue — are worse than a
missing key, which falls back to English *and* stays visible to coverage tooling and
the owner; and adding a string is cheap and required in English, whereas adding or
changing a *locale* is a governance action with its own dev-list process.

## Decision

User-facing text is a translated resource, and the locale set is owned:

- **Every user-facing string goes through `translate(...)` and a key in the
  appropriate English namespace** under `public/i18n/locales/en/`. Literals in the
  component tree are a defect, and the ESLint rule that catches them is not
  disabled to land a diff.
- **English is the source and the fallback.** New keys are added to `en` at
  minimum; a locale that lacks a key falls back to English by design.
- **Non-English catalogues contain real translations only** — never English text
  used as a placeholder, and never machine output in a language the author cannot
  verify.
- **Locale files are edited by (or with the agreement of) their owners**, as
  listed per language in `.github/CODEOWNERS` and governed by the policy in
  `public/i18n/README.md`.
- **Key structure stays in parity across catalogues**, so the shared namespace and
  nesting remain a single reviewable shape rather than diverging per language.
- **Adding or removing a supported locale follows the documented dev-list
  process**, including identifying a translation owner and a code owner.

## Consequences

- Non-English users get a coherent interface, and untranslated text degrades to
  English instead of appearing as a missing or broken string.
- Translation gaps are measurable against the English catalogue, which is why
  locale top-up PRs are a routine, welcome contribution.
- Reviewers who do not speak a language are not asked to approve its text; the
  CODEOWNERS mapping routes each locale to someone who can judge it.
- Each new string creates ongoing work for every locale owner, and the locale set
  grows deliberately, since an unowned locale decays into a permanent gap.

A change *violates* this decision when it:

- renders a hardcoded user-facing literal instead of a translated key, or
  disables `i18next/no-literal-string` to allow one;
- adds a new user-facing string without adding its key to the English
  catalogue, or puts it in a namespace it does not belong to;
- copies English text into a non-English catalogue as a placeholder — **except**
  where `public/i18n/README.md` permits it: an LLM-assisted update to non-English
  locales carried in the *same* PR as the English change is explicitly allowed.
  Note what this bullet no longer says: whether the author speaks the language is
  not visible in a diff, and a PR that discloses machine assistance is following
  the project's AI-disclosure rule, not confessing to a violation. Machine-assisted
  locale content is judged by its locale owner under the next bullet, on the text
  itself;
- edits a locale's files against the wishes of, or without the review of, that
  locale's owners in `.github/CODEOWNERS` — **subject to the same carve-out**, for
  which the README states that separate per-locale owner approval "is not
  required". Owner review governs standalone locale PRs, not same-PR updates
  following an English change;
- restructures or renames keys in one catalogue without carrying the same shape
  through the others, breaking key parity. Note that no lint enforces this:
  `check-translations-completeness` is registered at severity `warn` and
  `eslint --quiet` discards warnings, so the rule never fails a build, and it only
  reports keys missing relative to English. Parity is checked by
  `breeze ui check-translation-completeness` and by review;
- adds or removes a supported locale without the ownership and dev-list process
  described in `public/i18n/README.md` — a draft or RFC that names a proposed
  translation owner and asks for the code owner / dev-list step *is that process
  starting*, not a violation of it, and the right response is to run the process,
  not to close the PR.

A reviewer should reject any diff that introduces user-visible text which cannot
be translated, and should route locale content to that locale's owners.

## Evidence

- #65630 — the current i18n policy in `public/i18n/README.md`: supported locales,
  translation owners, code owners, completeness.
- #62763, #65115 — per-locale (Dutch, German) agent guidance owned by the people who
  maintain those languages.
- #67691 — translations the locale's owners judged wrong were reverted: locale
  content is reviewed by its owners, not accepted by default.
- #68285 — key structure treated as a shared, maintained shape across catalogues.
- #65720 — catalogues are runtime-loaded resources; serving a stale one is a
  user-facing defect.
- #68258 — locale selection (browser auto-detect) is part of the contract, not only
  the file contents.
- #68574, #67918 — routine gap-closing (Hindi, Hebrew) by locale maintainers, the
  intended way catalogues stay current.

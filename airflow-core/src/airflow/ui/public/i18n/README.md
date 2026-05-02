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

# Internationalization (i18n) Policy

## Purpose and scope

This document defines the minimal policy for UI translations in Apache Airflow core. It exists to keep
translations maintainable, reviewable, and consistent without adding unnecessary process.

It applies to:

- All supported locales in `airflow-core/src/airflow/ui/public/i18n/locales`.
- Contributors changing the default locale (`en`).
- Contributors proposing a new supported locale.
- Translation owners, code owners, and committers reviewing translation-related PRs.

> [!NOTE]
> This policy currently applies only to Airflow core UI translations.

## Core terms

- **Default locale**: English (`en`), the source and fallback for all other locales.
- **Supported locale**: A locale that is present in the repository and listed in the UI configuration.
- **Translation owner**: The person responsible for language quality and ongoing maintenance of a supported locale.
- **Code owner**: The committer responsible for technical review and merge decisions for that locale.
- **Translation sponsor**: A committer supporting a non-committer translation owner.
- **Complete translation**: A locale that covers at least 90% of the terms in the default locale.
- **Inactive owner**: A translation owner or code owner who is no longer actively maintaining the locale, for
  example because they have not contributed to Airflow for more than 12 months or the locale has remained
  incomplete for two consecutive major or minor releases.

## Ownership

Each supported non-English locale must have:

- At least one translation owner.
- At least one code owner listed in `.github/CODEOWNERS`.

A single committer may serve in both roles. When the translation owner is not a committer, a committer must
act as translation sponsor.

Translation owners are responsible for language quality, consistency, and keeping the locale reasonably current.
Code owners are responsible for technical review, merge decisions, and making sure the locale still has active
maintainers.

Having more than one translation owner for a locale is recommended, but not required.

## Communication

Translation-related decisions that affect supported locales, ownership, or policy should happen in public
through the dev list.

Locale owners and other regular translation contributors should also join the `#i18n` Slack channel for
day-to-day coordination and for release-time notifications about last-minute string changes.

## Adding or removing a supported locale

### Adding a locale

A new locale may be added when all of the following are true:

- The locale has an identified translation owner and code owner.
- The ownership arrangement has been approved through the dev list process described below before merge.
- The locale is added to the repository and UI configuration.
- The locale follows the translation guidance in this document and the locale-specific guide, if one exists.

The PR for a new locale should include:

- A locale-specific translation guide file at `.github/skills/airflow-translations/locales/<locale>.md`
- Locale files under `airflow-core/src/airflow/ui/public/i18n/locales/<locale>`.
- Updates to `airflow-core/src/airflow/ui/src/i18n/config.ts`.
- Updates to `dev/breeze/src/airflow_breeze/commands/ui_commands.py`.
- Updates to `.github/CODEOWNERS`.
- Updates to the table at the bottom of this file.

### Relinquishing translation/code ownership

- When a code owner asks to relinquish their role, or they become inactive, any other committer should:

  - Raise a PR for removal of the previous code owner from the `.github/CODEOWNERS` file.
  - Post a thread in the dev list that they step in as the code owner.

- When a translation owner asks to relinquish their role, or they become inactive, and there are no other active translation owners, the code owner should:

  - Raise a PR for removal of the translation owner from the `.github/CODEOWNERS` file.
  - Post a thread in the dev list that they are looking for assigning someone else as the translation owner within 30 days.
  - If a replacement is found within this time, they should be approved according to the ownership approval procedure below.
  - Otherwise, the code owner should raise a vote in the dev list for the removal of the translation from the codebase, with PMC and committers' votes counted as binding.

## Procedures

### Approval of ownership candidates

- The designated code owner should post a thread to the dev list that includes the following details:
  - The locale being suggested, including a link to the PR.
  - Designated code owner(s) and translation owner(s) in the suggested locale.
  - If the code owner is sponsored, they should indicate this as well. Specifically, if there is only one translation owner, the code owner should also declare how they plan to approve the language aspects of PRs.
- Within the thread, the code owner should demonstrate that the translation owner is suitable for the role,
  including sufficient proficiency in the target language and, for non-committers, the ability to maintain
  translation files through the normal PR process.
- Approval of any translation owner who is not a committer requires at least one binding vote of 1 PMC member, and no objections from other committers or PMC.
- Approval of any translation owner who is also a code owner (committer) does not need to be voted on.

## Expectations for translation changes

### Default locale changes

English is the source of truth. When contributors add, rename, or substantially rephrase English terms, they
should make downstream translation work easy.

- New or materially changed strings should be easy to detect in other locales.
- If a key is rephrased significantly, renaming the key is preferred so missing translations are visible.
- If a key is only moved or renamed without changing meaning, the contributor making that refactor should update
  the locale files rather than leaving cleanup to translation owners.
- In some cases, it may be more cost-effective to update the non-English locales in the same PR as the default-locale change, for example by using LLM-assisted translation. In that case, separate explicit approval from translation owners or code owners for each locale update is not required.

### Non-Default locale changes

Translations should:

- Preserve the meaning and intent of the English source.
- Keep technical terminology consistent.
- Preserve placeholders, formatting, and interpolation variables.
- Follow local language conventions without changing product behavior.
- Stay at or above the completeness threshold over time.

Locale-specific guideline files take precedence over the general guidance in this document.

> [!CAUTION]
> Translation content must comply with the Apache Airflow Code of Conduct.

## Review and merge expectations

Before merging a translation-related PR:

- Language changes should be approved by a translation owner for that locale.
- Technical changes may be approved and merged by any committer. However, the code owner has an added responsibility for the locale, so it is preferable to involve them.
- Completeness should be checked with the available tooling.

If a locale has only one translation owner and that person authors the PR, an additional reviewer is preferred.
When that is not practical, the code owner may use a trusted third-party review method, including LLM-assisted
review, before merging (for example, GitHub Copilot or Claude).

When review relies on a single translation owner together with a translation sponsor, the code owner should use
a trusted neutral third-party opinion for language questions when needed.

When a translation dispute cannot be resolved in the PR discussion:

- A translation owner resolves language questions for their locale.
- If multiple translation owners disagree, the code owner decides, using a trusted neutral third-party opinion
  when needed.
- If code owners disagree, a PMC member should be involved to resolve the conflict.

For RTL languages, languages with significantly different word order, or languages that typically require much longer text, a UI check is strongly recommended in addition to file-level review.

## Tools

Check completeness for all locales:

```bash
breeze ui check-translation-completeness
```

Check a specific locale:

```bash
breeze ui check-translation-completeness --language <language_code>
```

Add missing entries for a locale:

```bash
breeze ui check-translation-completeness --language <language_code> --add-missing
```

Remove unused entries for a locale:

```bash
breeze ui check-translation-completeness --language <language_code> --remove-unused
```

Remove unused entries for all locales:

```bash
breeze ui check-translation-completeness --remove-unused
```

## Locale-specific guidance

Before translating, read the locale-specific guide if one exists. Those files contain glossary and style choices
for the target language and override the general guidance in this document when there is a conflict. They are
primarily intended for LLM-assisted translation work, but can also be used by human contributors.

| Locale Code | Language                | Guideline File                  |
| ----------- | ----------------------- | ------------------------------- |
| `ar`        | Arabic                  | [.github/skills/airflow-translations/locales/ar.md](../../../../../../.github/skills/airflow-translations/locales/ar.md)  |
| `ca`        | Catalan                 | [.github/skills/airflow-translations/locales/ca.md](../../../../../../.github/skills/airflow-translations/locales/ca.md)  |
| `de`        | German                  | [.github/skills/airflow-translations/locales/de.md](../../../../../../.github/skills/airflow-translations/locales/de.md)  |
| `el`        | Greek                   | [.github/skills/airflow-translations/locales/el.md](../../../../../../.github/skills/airflow-translations/locales/el.md)  |
| `es`        | Spanish                 | [.github/skills/airflow-translations/locales/es.md](../../../../../../.github/skills/airflow-translations/locales/es.md)  |
| `fr`        | French                  | [.github/skills/airflow-translations/locales/fr.md](../../../../../../.github/skills/airflow-translations/locales/fr.md)  |
| `he`        | Hebrew                  | [.github/skills/airflow-translations/locales/he.md](../../../../../../.github/skills/airflow-translations/locales/he.md)  |
| `hi`        | Hindi                   | [.github/skills/airflow-translations/locales/hi.md](../../../../../../.github/skills/airflow-translations/locales/hi.md)  |
| `hu`        | Hungarian               | [.github/skills/airflow-translations/locales/hu.md](../../../../../../.github/skills/airflow-translations/locales/hu.md)  |
| `it`        | Italian                 | [.github/skills/airflow-translations/locales/it.md](../../../../../../.github/skills/airflow-translations/locales/it.md)  |
| `ja`        | Japanese                | [.github/skills/airflow-translations/locales/ja.md](../../../../../../.github/skills/airflow-translations/locales/ja.md)  |
| `ko`        | Korean                  | [.github/skills/airflow-translations/locales/ko.md](../../../../../../.github/skills/airflow-translations/locales/ko.md)  |
| `nl`        | Dutch                   | [.github/skills/airflow-translations/locales/nl.md](../../../../../../.github/skills/airflow-translations/locales/nl.md)  |
| `pl`        | Polish                  | [.github/skills/airflow-translations/locales/pl.md](../../../../../../.github/skills/airflow-translations/locales/pl.md)  |
| `pt`        | Portuguese              | [.github/skills/airflow-translations/locales/pt.md](../../../../../../.github/skills/airflow-translations/locales/pt.md)  |
| `th`        | Thai                    | [.github/skills/airflow-translations/locales/th.md](../../../../../../.github/skills/airflow-translations/locales/th.md)  |
| `tr`        | Turkish                 | [.github/skills/airflow-translations/locales/tr.md](../../../../../../.github/skills/airflow-translations/locales/tr.md)  |
| `zh-CN`     | Simplified Chinese      | [.github/skills/airflow-translations/locales/zh-CN.md](../../../../../../.github/skills/airflow-translations/locales/zh-CN.md) |
| `zh-TW`     | Traditional Chinese     | [.github/skills/airflow-translations/locales/zh-TW.md](../../../../../../.github/skills/airflow-translations/locales/zh-TW.md) |

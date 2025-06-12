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

## 1. Purpose & scope

This document outlines the policy for internationalization (i18n) in Apache Airflow, detailing the lifecycle of translations within the project.
This policy aims to avoid inconsistencies, maintenance issues, unclear ownership, and to ensure translation quality.

### Scope

This policy applies to:

- Each supported locale included in `airflow-core/src/airflow/ui/src/i18n/locales`.
- Contributors making changes in the default locale (English).
- Contributors suggesting new locales to be added to the codebase.
- Maintainers of supported locales in any role defined below.
- Committers and PMC.
- Release managers.

> [!NOTE]
> This policy currently applies only to changes made in Apache Airflow core, as i18n is not yet implemented for providers (including auth managers). When such support is added, this policy should be updated to reflect the expanded scope.

## 2. Definitions

**Internationalization (i18n)** - The process of designing a software application so that it can be adapted to various languages and regions without engineering changes (see also the [Wikipedia article](https://en.wikipedia.org/wiki/Internationalization_and_localization)).

**Supported locale** - An officially accepted locale in `airflow-core/src/airflow/ui/src/i18n/locales`.

**Default locale** - English (`en`), the primary locale and fallback for all other locales.

**Translation owner** - Designated contributor responsible for maintaining a supported locale.

**Code owner** - Apache Airflow committer with write permissions, listed in  `.github/CODEOWNERS`.

**Translation sponsor** - Apache Airflow committer supporting a non-committer translation owner (e.g., by communicating in the dev list or merging Pull Requests on their behalf).

**Engaged translator** - Active contributor participating in translation without formal ownership.

**Inactive translation/code owner** — A translation/code owner is considered inactive if they meet either of the following criteria:

- The locale under their responsibility has remained incomplete for at least 2 consecutive releases.
- They have not participated in the Apache Airflow project for more than 12 months.

**Dev list** - The Apache Airflow development mailing list: dev@airflow.apache.org.

## 3. Wording/Phrasing

- Unless explicitly stated otherwise, all references to directories and files in this document pertain to those in the `main` branch.
- Where emphasised by capital letters, the keywords "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD",
"SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in RFC 2119.

## 4. Roles & responsibilities

### 4.1. Translation owner

- Translation owners are responsible for the following, in their assigned supported locale, according to the established quality standards and procedures stated below:
  - Ensuring locale remains up-to-date with source code changes in the default locale.
  - Reviewing the language aspects of translation-related Pull Requests (PRs).
  - Resolving translation-related conflicts in PRs.
  - Ensuring translation reflects current language usage and terminology.
  - Resolving translation-related GitHub issues and discussions.

### 4.2. Code owner

- Code owners are responsible for the following, in their assigned supported locale, according to the procedures stated below:
  - Reviewing the technical aspects of translation-related PRs (e.g., linting, formatting, etc.).
  - Merging translation-related PRs approved by the translation owner.
  - Resolving translation-related conflicts in PRs, when there's a conflict between translation owners.
  - Managing translation-related GitHub issues and discussions, when needed (e.g., closing GitHub issues).
- Code owners who act as translation sponsors are also responsible for:
  - Ensuring that the translation owner is active and able to maintain the translation.
  - Act according to section 6.4 when the translation owner relinquishes their role or become inactive.

### 4.3. Engaged translator

- Engaged translators do not have any formal responsibilities, but they are encouraged to contribute to supported locales by:
  - Suggesting improvements.
  - Reviewing PRs.
  - Reporting issues or inconsistencies in translations.
  - Participating in discussions related to translations.
  - Assisting translation owners with their tasks.
  - Being 3rd party reviewers for translation-related conflicts, when needed.
- Engaged translators may be mentioned in a comment in the `.github/CODEOWNERS` file.
- Suitable candidates for translation ownership may be suggested from engaged translators, upon their consent and approval by the procedure in section 6.1.

## 5. Requirements

### 5.1. Translation ownership and code ownership

- Each supported locale, except for the default language, MUST have at least one translation owner and at least one code owner assigned at all times, with these considerations:
  - Ownership for both roles MUST be approved according to the process discussed in section 6.1.
  - A single Apache Airflow committer MAY serve as both code owner and translation owner for the same locale.
  - If none of the translation owners are code owners - there MAY be a translation sponsor assigned as a code owner.
- When the above is not met, steps mentioned in section 6.4 SHOULD be taken by the appropriate roles.

> [!NOTE]
> It is welcomed and desired to have more than one translation owner to enable peer reviews and provide coverage during absences.

### 5.2. Adding new locales

To accept a new supported locale to the codebase, it MUST be approved through the process discussed in section 6.2.

### 5.3. Translation owners candidates

- Translation owners candidates MUST declare and demonstrate a sufficient level of proficiency in the target language for translation purposes, including technical terminology (as detailed in section 6.5).
- Translation owners candidates, who are non-committers, MUST also meet the following criteria:
     - They are active long-term contributors to the Apache Airflow project at the time of request.
     - They have basic skills of working with Git and GitHub, as well as modifying JSON translation files within their target language.
     - They have the support of an Apache Airflow committer who will act as a translation sponsor.

### 5.4. Resolution of translation conflicts

Translation conflicts MUST be resolved according to the procedures outlined in section 6.3.

### 5.5. Adding or rephrasing terms

- When new terms are added to the default locale, all translation owners SHOULD create a follow-up PR to comply with the changes in their assigned locale.
- When existing terms are rephrased in the default language (key is the same but value changed), all translation owners SHOULD do the same as above, if the change in the intent or meaning affects the translation.
- In busy times with many parallel UI changes it is acceptable to batch changes together. Differences SHOULD be cleared prior to a release at the latest.

> [!NOTE]
> Tooling for detecting missing terms is available (see Tools & Resources section below).

### 5.6. Deprecating / refactoring terms

- When existing terms are deprecated or refactored in the default locale (key renamed/relocated but value unchanged), **the contributor initiating the change holds responsible for updating all relevant locale files, and not any of the locale's owners**. When such available, automation through Breeze tooling SHOULD be used.

### 5.7. Merging of translation-related Pull Requests (PRs)

- Before merging any translation-related PR, it MUST be:
   - Approved by a translation owner of the respective locale for language aspects, according to the standards and guidelines.
     - When a translation owner initiates a PR and is the only one assigned to the locale, they SHOULD instead ask for approval from a third party (e.g., engaged translator), or if such is not available, declare their self-approval for the language aspects.
   - Approved by a code owner, or another committer on their behalf, for technical aspects (e.g., linting, formatting, etc.).
- Before merging a translation-related PR, the translation SHOULD be checked for completeness using the provided tools (see section 8).

> [!WARNING]
> In languages with different word order than English, or in Right-To-Left (RTL) languages, it is important to validate that the changes are properly reflected in the UI.
> If they are not, please raise a GitHub issue or a PR for fixing it (separately from the translation PR).

### 5.8. Version release

- Release managers MUST follow the requirements for releasing changes in supported locales defined in the [Release Management Policy](../../../../../../dev/README_RELEASE_AIRFLOW.md).

## 6. Procedures

### 6.1. Approval of ownership candidates

- The designated code owner, should post a thread to the dev list, requesting the approval of:
  - Themselves as the code owner in the suggested locale. If they are a translation sponsor, they should indicate it as well.
  - Other code owner(s) in the suggested locale, if applicable.
  - The identity of the translation owner(s) in the suggested locale (which could be themselves as well)
- When introducing a new locale, the thread should announce that on the same thread (including a link to the PR)
- Within the thread, the code owner should demonstrate that the translation owner is suitable for the role, according to the requirements in section 5.3.
- Approval of a code owner who is also the translation owner can be done by a lazy consensus.
- Approval of any translation owner who is not a committer requires at least one binding vote of 1 PMC member, and no objections from other committers/PMC.

### 6.2. Approval of a new locale

The following steps outline the process for approving a new locale to be added to the supported locales:

- Creating a PR for adding the suggested locale to the codebase ([see example](https://github.com/apache/airflow/pull/51258/files)), which includes:
    - The locale files (translated according to the guidelines) in the `airflow-core/src/airflow/ui/src/i18n/locales/<LOCALE_CODE>` directory, where `<LOCALE_CODE>` is the code of the language according to ISO 639-1 standard (e.g., `fr` for French). Languages with regional variants should be handled in separate directories, where the name is suffixed with `-<VARIANT>`, and `<VARIANT>` is the variant that follows ISO 3166-1 or UN M.49 codes in lowercase (e.g., `zh-tw` for Taiwanese Mandarin).
    - Making the required modifications in `airflow-core/src/airflow/ui/src/i18n/config.ts` ([see example](https://github.com/apache/airflow/pull/51258/files#diff-bfb4d5fafd26d206fb4a545a41ba303f33d15a479d21e0a726fd743bdf9717ff)).
    - Changes to the `.github/CODEOWNERS` file to include the designated code owner(s) and translation owner(s) for the new locale, considering the following:
        - A code owner who is also a translation sponsor should be indicated in a comment as well.
        - If the PR author is neither eligible nor willing to become both of these roles, they should suggest relevant candidates for the missing role(s), or call for volunteers.
- Applying the procedure in section 6.1. to approve the identities of the code owner(s) and the translation owner(s).
- Only after the steps above are completed, the PR for the new translation may be merged (by the requirements in section 5.7).

### 6.3. Translation conflict resolution

When a translation conflict arises in a locale-related PR, the following steps will be taken in order:

- The involved parties should first try to reach a consensus through discussion in the PR.
- If no consensus is reached, a translation owner may decide the outcome.
- If multiple translation owners are involved and cannot reach consensus, the code owner will decide. If the code owner is sponsored,
they should base their decision on a neutral source (e.g., a third-party opinion, translation tool, or LLM).
- If the conflict is between code owners, a PMC member will be involved to resolve the conflict.

### 6.4. Relinquishing translation/code ownership

 - When a code owner asks to relinquish their role, or they become inactive, any another committer should:
    - Raise a PR for removal of the previous code owner from the `.github/CODEOWNERS` file.
    - Post a thread in the dev list that they step in as the code owner (either as a translation sponsor, or a translation owner according to steps discussed in section 6.1).
 - When a translation owner asks to relinquish their role, or they become inactive, and there are no other active translation owners, the code owner should:
    - Raise a PR for removal of the translation owner from the `.github/CODEOWNERS` file.
    - Post a thread in the dev list that they are looking for assigning someone else as the translation owner within 30 days.
    - If a replacement is found within this time, they should be approved according to section 6.1.
    - Otherwise, the code owner should raise a vote in the dev list for the removal of the translation from the codebase (7 days vote, PMC and committers votes are counted as binding).

### 6.5 Demonstrating language proficiency

Language proficiency for translation owners can be demonstrated through any of the following means:

- Communications in open-source projects, social media, mailing lists, forums, or any other platforms in the target language.
- Direct communication with a proficient committer in the target language.
- Official language certifications (this is not a mandatory requirement).

## 7. Standards & guidelines

> [!CAUTION]
> Usage of language that defies Apache Airflow's [code of conduct](http://airflow.apache.org/code-of-conduct/) is prohibited in any circumstances.

- Translations should be based on the default language (English). When translating a language that has already a similar translation supported
(e.g., Portuguese vs. Spanish), the other language might be used as a reference, but still the default language (English) should be the primary source for translations.
- Translations should be accurate, maintaining original meaning and intent.
- Translations should be complete, covering all terms and phrases in the default language.
- Translation of technical terminology should be consistent (for example: Dag, Task, Operator, etc.).
- Language should be polite and neutral in tone.
- Local conventions should be considered (e.g., date formats, number formatting, formal vs. informal tone, etc.).
  - In case that local conventions requires deviation from any of these guidelines, exceptions may be requested via PR or a thread in the dev list.
- Formatting, placeholders, and variable substitutions must be preserved.

## 8. Tools & resources

### 8.1. Checking completeness of i18n files

All files:

```bash
uv run ./check_translations_completeness.py
```

Files for specific languages:

```bash
uv run ./check_translations_completeness.py --language <language_code>
```

Where `<language_code>` is the code of the language you want to check, e.g., `en`, `fr`, `de`, etc.

Adding missing translations (with `TODO: translate` prefix):

```bash
uv run ./check_translations_completeness.py --language <language_code> --add-missing
```

## 9. Compliance & enforcement

> [!NOTE]
> As of the time of writing, this policy is not enforced by any automated checks.
> The following describe the desired future state of compliance and enforcement.

- Automated checks SHOULD verify once in a while that all languages have corresponding entries for new terms in the default language. When translations are missing, relevant code owners should be notified.
- Automated checks SHOULD allow a person doing translation to select the language and aid them in adding new translations so that they do not have to compare them manually. Possibly it can be done by adding `-–add-missing` to the verifying script that will add new entries with `TODO: translate: ENGLISH VERSION` and add pre-commit to not allow such `TODO:` entries to be committed.

## 10. Exceptions

If any exceptions to this policy are needed, they MUST be discussed and approved by voting in the dev list beforehand.

## 11. Review and updates

This policy will be reviewed and updated as needed to ensure it remains relevant and effective.
Depending on the nature of the change, suggested updates might need to be discussed and approved by voting in the dev list.

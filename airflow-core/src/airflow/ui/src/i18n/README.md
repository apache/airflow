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
The scope of this policy is applied to:

- Each supported language included in the `airflow-core/src/airflow/ui/src/i18n/locales` directory of the Apache Airflow project, or any suggested new translation.
- Contributors responsible for maintaining these translations by any of the roles defined below.
- Contributors who apply changes in the default language (English) that may affect translations.

> [!NOTE]
> This policy currently applies only to changes made in Apache Airflow core, as i18n is not yet implemented for providers (including auth managers). When such support is added, this policy should be updated to reflect the expanded scope.

## 2. Definitions

**Internationalization (i18n)** - The process of designing a software application so that it can be adapted to various languages and regions without engineering changes (see also the [Wikipedia article](https://en.wikipedia.org/wiki/Internationalization_and_localization)).

**Supported translation** - A translation that has been officially accepted into the project, located in `airflow-core/src/airflow/ui/src/i18n/locales`.

**Default language** - The language used by default, and as a fallback to all other languages (English).

**Translation owner** - A designated contributor responsible for the maintenance and quality for a supported translation.

**Code owner** - An Apache Airflow committer designated in the `.github/CODEOWNERS` file for a supported translation. Only Code owners have write permissions to the repository and can be listed in `.github/CODEOWNERS`.

**Translation sponsor** - An Apache Airflow committer who supports a non-comitter translation owner (e.g., by merging Pull Requests on their behalf).

**Engaged translator** - An Apache Airflow contributor who actively participates in the translation process, yet is not a translation owner.

**Release manager** - according to the definition in the [Release Management Policy](../../../../../../dev/README_RELEASE_PROVIDERS.md#what-the-provider-distributions-are).

**Dev. list** - The Apache Airflow development mailing list: dev@airflow.apache.org.

**Inactive translation/code owner** — A translation owner/code owner is considered inactive if they meet either of the following criteria:

- The translation under their responsibility has remained incomplete for at least two consecutive releases.
- They have not participated in the Apache Airflow project for more than 12 months.

## 3. Wording/Phrasing

- Unless specified explicitly, references to directories and files in the document refer to files in the `main` branch.
- Where emphasised by capital letters, The keywords "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD",
"SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in the RFC 2119.

## 4. Roles & responsibilities

### 4.1. Translation owner

- Translation owners are responsible for the following, in their assigned supported translation, according to established quality standards and procedures stated below:
  - Ensuring translation remains up-to-date with source code changes in the default language.
  - Reviewing the language aspects of translation-related Pull Requests (PRs).
  - Resolving translation-related conflicts in PRs.
  - Ensuring translation reflects current language usage and terminology.
  - Resolving translation-related GitHub issues and discussions.

### 4.2. Code owner

- Code owners are responsible for the following, in their assigned supported translation, according to the procedures stated below:
  - Reviewing the technical aspects of translation-related PRs (e.g., linting, formatting, etc.).
  - Merging translation-related PRs approved by the translation owner.
  - Resolving translation-related conflicts in PRs, when needed.
  - Managing translation-related GitHub issues and discussions, when needed (e.g., closing issues).
- Translations sponsors who function as code owners, are also responsible for ensuring that the translation owner is active and able to maintain the translation. If not, they should act according to section 6.4.

### 4.3. Engaged translator

- Engaged translators do not have any formal responsibilities, but they are encouraged to contribute to translations by:
  - Suggesting improvements to existing translations.
  - Reporting issues or inconsistencies in translations.
  - Participating in discussions related to translations.
  - Assisting translation owners with their tasks, when needed.
  - Being 3rd party reviewers for translation-related PRs and conflicts, when needed.
- They may be mentioned in a comment in the `.github/CODEOWNERS` file.
- Suitable candidates for translation ownership may be suggested from engaged translators, upon their consent and approval by the procedure in section 6.1.

## 5. Requirements

### 5.1. Translation ownership and code ownership

Each supported translation, except for the default language, MUST meet one of the following conditions:

- Have at least one translation owner who is also a code owner.
- Have at least one translation owner, with a translation sponsor assigned as a code owner.

> [!NOTE]
> It is welcomed and desired to have more than one individual translation owner to enable peer reviews and provide coverage during individual absences.

### 5.2. Approval of translation owners candidates

- Translation owners candidates MUST declare and demonstrate a sufficient level of proficiency in the target language for translation purposes (as detailed in section 6.5), including technical terminology.
- Translation owners candidates, who are non-committers, MUST also meet the following criteria:
     - They are active long-term contributors to the Apache Airflow project at the time of request.
     - They have basic skills of working with GIT and GitHub, as well as modifying JSON translation files within their target language.
     - They have the support of an Apache Airflow committer who will act as a translation sponsor.
- Translation owners candidates MUST go through the approval process detailed in section 6.1.

### 5.3. Approval of new translations

To accept a new translation to the codebase, it MUST be approved through a review process discussed in section 6.2.

### 5.4. Resolution of translation conflicts

Translation conflicts MUST be resolved according to the procedures outlined in section 6.3.

### 5.5. Adding / rephrasing terms

- When new terms are added to the default language, all translation owners SHOULD create a follow-up PR to comply with the changes in their assigned language within a reasonable time.
- When rephrasing terms in the default language, all translation owners SHOULD do the same as above, **if needed**.
- In busy times with many parallel UI changes it is acceptable to batch changes together, latest prior a release the differences SHOULD be cleared.

> [!NOTE]
> Tooling for detecting missing translations is available (see Tools & Resources section below).

### 5.6. Deprecating / refactoring terms

When existing terms are deprecated or refactored (key renamed/relocated but value unchanged), **the contributor initiating the change is responsible for updating all language files, and not the translation/code owner**. Automation through Breeze tooling should be used when available.

### 5.7. Approval and merging of translation-related Pull Requests (PRs)

- If the code owner is also a translation owner of the respective translation:
    - Others' PRs should be approved and merged normally by them.
    - Their own PRs should be approved and merged normally by another committer, preferably another code owner in their locale (if such exists).
- Otherwise, if the code owner is sponsored:
    - They should merge the translation-related PRs only after they are approved by the translation owner.
- Other committers may review and approve translation-related PRs in any aspects, but they SHOULD NOT merge them without the approval of the translation owner and the consent of the code owner.
- Before merging a translation-related PR, the translation should be checked for completeness using the provided tools (see section 8).

> [!WARNING]
> In languages with different word order than English, or in Right-To-Left (RTL) languages, it is important to validate that the changes are properly reflected in the UI.
> If they are not, please raise a GitHub issue or a PR for fixing it (separately from the translation PR).

### 5.8. Version release

- Requirements for release managers are defined in the [Release Management Policy](../../../../../../dev/README_RELEASE_AIRFLOW.md).


## 6. Procedures

### 6.1. Approval process of a translation owner

- The designated code owner should post a thread in the dev. list to request for approval of the translation owner(s) for a supported translation:
    - Approvals of a translation owner who is also the code owner can be done by a lazy consensus.
    - Approvals of translation owners who are non-committers require at least one binding vote of at least 1 PMC member, and no objections from committers/PMC.
- Within the thread, the code owner should demonstrate that the translation owner is suitable for the role, according to the requirements in section 4.2.

### 6.2. Approval process of a new translation

The following steps outline the process for approving a new translation:

- Creating a PR to add a new translation to the codebase ([see example](https://github.com/apache/airflow/pull/51258/files)), according to the standard and guidelines, which includes:
    - The translation files in the `airflow-core/src/airflow/ui/src/i18n/locales/<LOCALE_CODE>` directory, where `<LOCALE_CODE>` is the code of the language according to ISO 639-1 standard (e.g., `fr` for French). Languages with regional variants should be handled in separate directories, where the name is suffixed with `-<VARIANT>`, and `<VARIANT>` follows ISO 3166-1 or UN M.49 codes in lowercase for the variant (e.g., `zh-tw` for Taiwanese Chinese).
    - Making the required modifications in `airflow-core/src/airflow/ui/src/i18n/config.ts` ([see example](https://github.com/apache/airflow/pull/51258/files#diff-bfb4d5fafd26d206fb4a545a41ba303f33d15a479d21e0a726fd743bdf9717ff)).
    - Updating the `.github/CODEOWNERS` file to include the code owner(s).
      > When assigning a translation sponsor, there should be a comment in the `.github/CODEOWNERS` indicating who the translation owner(s) and the translation sponsor are.
- Apply procedure 6.1. to approve the translation owner, while announcing the introduction of the new language (including a link to the PR).
- Only after the steps above are completed, the PR for the new translation could be merged (by the requirements in section 5.7).

### 6.3. Translation conflict resolution

When a conflict arises in a translation-related PR, the following steps will be taken in order:

- The involved parties should first try to reach a consensus through discussion in the PR.
- If no consensus is reached, a translation owner may decide the outcome.
- If multiple translation owners are involved and cannot reach consensus, the code owner will decide. If the code owner is sponsored,
they should base their decision on a neutral source (e.g., a third-party opinion, translation tool, or LLM such as ChatGPT or Claude).
- If the conflict is between code owners, a PMC member will be involved to resolve the conflict.

### 6.4. Relinquishing translation/code ownership

 - When a code owner asks to relinquish their role, or they become inactive, any another committer might:
    - Raise a PR for removal of the previous code owner from the `.github/CODEOWNERS` file.
    - Post a thread in the dev. list that they step in as the code owner (either as translation sponsor, or translation owner according to section 6.1).
 - When a translation owner asks to relinquish their role, or they become inactive, and there are no other active translation owners, the code owner should:
    - Raise a PR for removal of the translation owner from the `.github/CODEOWNERS` file.
    - Post a thread in the dev. list that they are looking for assigning someone else as the translation owner within 30 days.
    - If a replacement is found within this time, they should be approved according to section 6.1.
    - Otherwise, the code owner should raise a vote in the dev. list for the removal of the translation from the codebase (7 days vote, PMC and committers votes are counted as binding).

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
> Currently, there is no automated enforcement, nor notifications, as part of the CI.

- Automated checks SHOULD verify once in a while that all languages have corresponding entries for new terms in the default language. When translations are missing, relevant code owners should be notified.
- Automated checks SHOULD allow a person doing translation to select the language and aid them in adding new translations so that they do not have to compare them manually. Possibly it can be done by adding `-–add-missing` to the verifying script that will add new entries with `TODO: translate: ENGLISH VERSION` and add pre-commit to not allow such `TODO:` entries to be committed.

## 10. Exceptions and escalation

If any exceptions to this policy are needed, they MUST be discussed and approved by the Apache Airflow PMC (Project Management Committee) before implementation.

## 11. Review and updates

This policy will be reviewed and updated as needed to ensure it remains relevant and effective.
Suggested updates will be approved by voting in the dev. list (lazy consensus or binding voting, depending on the nature of the change).

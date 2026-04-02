<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Korean (ko)

This document provides locale-specific instructions for translating English Airflow UI strings into Korean. It inherits all global rules from the parent [SKILL.md](../SKILL.md).

## Translation Style

Use wording already established in existing `ko` locale files first. If a term has no established translation yet, prefer natural Korean UI phrasing over literal transliteration.

**English source:**

```json
"lastDagRun_one": "Last Dag Run",
"deleteConnection_other": "Delete {{count}} connections"
```

**Correct** — natural Korean UI wording:

```json
"lastDagRun_one": "마지막 Dag 실행",
"deleteConnection_other": "커넥션 {{count}}개 삭제"
```

**Incorrect** — overly literal or awkward:

```json
"lastDagRun_one": "마지막 Dag 런",
"deleteConnection_other": "{{count}} 연결들을 삭제"
```

## Plural Forms

Korean often uses the same wording for singular and plural. Follow established usage in existing `ko` locale files first. If an existing key pair already distinguishes `_one` and `_other`, keep that distinction. If no established wording exists, use the same translation for both.

**English source:**

```json
"taskCount_one": "{{count}} Task",
"taskCount_other": "{{count}} Tasks"
```

**Correct** — identical for both when no established distinction exists:

```json
"taskCount_one": "{{count}}개 작업",
"taskCount_other": "{{count}}개 작업"
```

## Counters and Spacing

Use counters consistent with existing `ko` locale usage and keep spacing readable:

- Insert a single space between Korean and adjacent English technical terms where needed (`Dag 실행`, `커넥션 ID`).
- Do not insert a space between numbers/placeholders and counters such as `개` (for example, `{{count}}개`).

```json
"deleteConnection_other": "커넥션 {{count}}개 삭제",
"taskCount_one": "{{count}}개 작업",
"taskCount_other": "{{count}}개 작업",
"lastDagRun_one": "마지막 Dag 실행",
"connectionId": "커넥션 ID"
```

## Particles and Placeholders

Preserve all `{{variable}}` placeholders exactly. Attach Korean particles outside placeholders and reorder phrases only when needed for natural Korean word order.

**English source:**

```json
"confirmation": "Are you sure you want to delete {{resourceName}}? This action cannot be undone.",
"description": "{{count}} {{resourceName}} have been successfully deleted. Keys: {{keys}}"
```

**Correct** — placeholders preserved and particles outside:

```json
"confirmation": "{{resourceName}}을(를) 삭제하시겠습니까? 이 작업은 되돌릴 수 없습니다.",
"description": "{{count}}개 {{resourceName}}이(가) 성공적으로 삭제되었습니다. 키: {{keys}}"
```

**Incorrect** — variable names translated:

```json
"confirmation": "{{리소스이름}}을(를) 삭제하시겠습니까? 이 작업은 되돌릴 수 없습니다.",
"description": "{{개수}}개 {{리소스이름}}이(가) 성공적으로 삭제되었습니다. 키: {{키들}}"
```

## Tone and UI Voice

- Use neutral, slightly formal tone.
- Keep labels and messages concise for UI.
- Use polite confirmations in destructive actions:
  `"{{resourceName}}을(를) 삭제하시겠습니까? 이 작업은 되돌릴 수 없습니다."`
- Avoid colloquial phrasing.

## Terminology and Casing

- Keep `Dag` casing exactly as `Dag` (never `DAG`).
- Reuse established Korean role terms from existing `ko` locale files (for example, `스케줄러`, `오퍼레이터`).
- Prefer established `ko` glossary by key context (for example, `Dag 실행`, `커넥션`, `변수`), and keep stable technical tokens in English: `XCom`, `REST API`, `JSON`, `URL`, `ID`, `UTC`.

## Terminology Reference

The established Korean translations are defined in existing locale files. Before translating, **read the existing ko JSON files** to learn the established terminology:

```
airflow-core/src/airflow/ui/public/i18n/locales/ko/
```

Use the translations found in these files as the authoritative glossary. When translating a term, check how it has been translated elsewhere in the locale to maintain consistency. If a term has not been translated yet, refer to the English source in `en/` and apply the rules in this document.

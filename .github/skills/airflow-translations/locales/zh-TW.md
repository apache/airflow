<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Traditional Chinese (zh-TW)

This document provides locale-specific instructions for translating English
Airflow UI strings into Traditional Chinese. It inherits all global rules from
the parent [SKILL.md](../SKILL.md).

## Translation Styles

Prioritize terms commonly used in the Taiwan tech ecosystem.

**Correct:**

```json
"user": "使用者",
"message": "訊息",
"support": "支援"
```

**Incorrect:**

```json
"user": "用戶",
"message": "信息"
"support": "支持"
```

## Plural Forms

Traditional Chinese **does not** distinguish between singular and plural forms.
Use the **same translation** for both `_one` and `_other` suffixes:

**English source:**

```json
"dagRun_one": "Dag Run",
"dagRun_other": "Dag Runs"
```

**Correct** — identical for both:

```json
"dagRun_one": "Dag 執行",
"dagRun_other": "Dag 執行"
```

## Spacing Rules

Insert a **half-width space** between Chinese characters and adjacent
English words, numbers, or symbols:

**Correct:**

```json
"Dag 執行"        // space between English and Chinese
"最近 12 小時"     // space around numbers
"連線 ID"          // space before abbreviation
"{{count}} 個連線" // space after placeholder
```

**Incorrect:**

```json
"Dag執行"          // missing space
"最近12小時"       // missing space around numbers
```

## Punctuation

- Use **full-width** punctuation for Chinese sentences: `，` `。` `：` `？` `！`
- Use **half-width** punctuation for content within English terms, JSON, or
  code: `,` `.` `:` `?`
- Use **full-width** parentheses for Chinese context: `（` `）`
- Use **half-width** parentheses when wrapping English or variables: `(` `)`

**Correct** — full-width for Chinese sentences:

```json
"confirmation": "您確定要刪除這個 {{resourceName}} 嗎？此動作無法復原。"
```

**Correct** — half-width for English/variable context:

```json
"tooltip": "按下 {{hotkey}} 切換展開"
```

## Measure Words (量詞)

Chinese requires **measure words** (量詞) between numbers and nouns. Use the
appropriate measure word for each context:

| Measure Word | Usage | Example |
|---|---|---|
| `個` | General objects (connections, variables, errors) | `刪除 {{count}} 個連線` |
| `次` | Occurrences (runs, executions, attempts) | `最近 {{count}} 次 Dag 執行` |
| `項` | List items | `+ 其他 {{count}} 項` |

## Tone and Formality

- Use **neutral, slightly formal** register.
- Use `您` (formal "you") in confirmations and destructive actions:
  `"您即將刪除以下連線："`.
- Avoid colloquial or overly casual expressions.
- Keep translations **concise** — these are UI labels and button text.

## Variable and Placeholder Examples

Preserve all `{{variable}}` placeholders. Reorder as needed for natural Chinese
word order:

**English source:**

```json
"title": "Mark {{type}} as {{state}}"
```

**Correct** — placeholders preserved:

```json
"title": "標記 {{type}} 為 {{state}}"
```

**Incorrect** — variable names translated:

```json
"title": "標記 {{類型}} 為 {{狀態}}"
```

## Terminology Reference

The established zh-TW translations are defined in the existing locale files.
Before translating, **read the existing zh-TW JSON files** to learn the
established terminology:

```
airflow-core/src/airflow/ui/public/i18n/locales/zh-TW/
```

Use the translations found in these files as the authoritative glossary. When
translating a term, check how it has been translated elsewhere in the locale
to maintain consistency. If a term has not been translated yet, refer to the
English source in `en/` and apply the rules in this document.

### Additional Terminology

Use the following established translations in the Airflow UI:

- **Triggerer** → 觸發者
- **trigger** → 觸發器

Ensure these terms are used consistently across the locale.

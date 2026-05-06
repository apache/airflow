<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Simplified Chinese (zh-CN)

This document provides locale-specific instructions for translating English
Airflow UI strings into Simplified Chinese. It inherits all global rules from
the parent [SKILL.md](../SKILL.md).

## Plural Forms

Simplified Chinese **does not** distinguish between singular and plural forms.
Use the **same translation** for both `_one` and `_other` suffixes:

**English source:**

```json
"dagRun_one": "Dag Run",
"dagRun_other": "Dag Runs"
```

**Correct** — identical for both:

```json
"dagRun_one": "Dag 执行",
"dagRun_other": "Dag 执行"
```

## Spacing Rules

Insert a **half-width space** between Chinese characters and adjacent
English words, numbers, or symbols:

**Correct:**

```json
"Dag 执行"        // space between English and Chinese
"最近 12 小时"     // space around numbers
"连接 ID"          // space before abbreviation
"{{count}} 个连接" // space after placeholder
```

**Incorrect:**

```json
"Dag执行"          // missing space
"最近12小时"       // missing space around numbers
```

## Punctuation

- Use **full-width** punctuation for Chinese sentences: `，` `。` `：` `？` `！`
- Use **half-width** punctuation for content within English terms, JSON, or
  code: `,` `.` `:` `?`
- Use **full-width** parentheses for Chinese context: `（` `）`
- Use **half-width** parentheses when wrapping English or variables: `(` `)`

**Correct** — full-width for Chinese sentences:

```json
"confirmation": "确定要删除 {{resourceName}} 吗？此操作无法还原。"
```

**Correct** — half-width for English/variable context:

```json
"tooltip": "按下 {{hotkey}} 切换展开"
```

## Measure Words (量词)

Chinese requires **measure words** (量词) between numbers and nouns. Use the
appropriate measure word for each context:

| Measure Word | Usage | Example |
|---|---|---|
| `个` | General objects (connections, variables, errors) | `删除 {{count}} 个连接` |
| `次` | Occurrences (runs, executions, attempts) | `最近 {{count}} 次 Dag 执行` |
| `项` | List items | `+ 其他 {{count}} 项` |

## Tone and Formality

- Use **neutral, slightly formal** register.
- Use `您` (formal "you") in confirmations and destructive actions:
  `"您即将删除以下连接："`.
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
"title": "标记 {{type}} 为 {{state}}"
```

**Incorrect** — variable names translated:

```json
"title": "标记 {{类型}} 为 {{状态}}"
```

## Terminology Reference

The established zh-CN translations are defined in the existing locale files.
Before translating, **read the existing zh-CN JSON files** to learn the
established terminology:

```
airflow-core/src/airflow/ui/public/i18n/locales/zh-CN/
```

Use the translations found in these files as the authoritative glossary. When
translating a term, check how it has been translated elsewhere in the locale
to maintain consistency. If a term has not been translated yet, refer to the
English source in `en/` and apply the rules in this document.

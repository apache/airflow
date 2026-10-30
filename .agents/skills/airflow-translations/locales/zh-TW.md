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

Traditional Chinese **does not** distinguish between singular and plural forms,
so both suffixes use the same noun. i18next currently never selects `_one`
for `zh-TW` (the locale has a single plural category, `other`), but the key
must still exist for translation completeness.

Some keys are plain nouns rather than counts — use the same translation for
both suffixes:

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

When the English `_one` does carry a count, write it as a literal `1` instead
of interpolating `{{count, number}}`; keep the placeholder in `_other`:

**English source:**

```json
"taskCount_one": "{{count, number}} Task",
"taskCount_other": "{{count, number}} Tasks"
```

**Correct** — literal `1` in `_one`:

```json
"taskCount_one": "1 個任務",
"taskCount_other": "{{count, number}} 個任務"
```

**Incorrect** — interpolating a count that can only be 1:

```json
"taskCount_one": "{{count, number}} 個任務",
"taskCount_other": "{{count, number}} 個任務"
```

## Spacing Rules

Insert a **half-width space** between Chinese characters and adjacent
English words, numbers, or symbols:

**Correct:**

```json
"Dag 執行"        // space between English and Chinese
"最近 12 小時"     // space around numbers
"連線 ID"          // space before abbreviation
"{{count, number}} 個連線" // space after placeholder
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
| `個` | General objects (connections, variables, errors) | `刪除 {{count, number}} 個連線` |
| `次` | Occurrences (runs, executions, attempts) | `最近 {{count, number}} 次 Dag 執行` |
| `項` | List items | `+ 其他 {{count, number}} 項` |

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

## Translation Workflow

Work through these steps in order before settling on a wording.

1. **Understand the string.** Decide what it denotes: an action, an object, a
   status, or a field value. What it denotes decides the wording. A state or
   field name should be checked against its backend definition rather than
   guessed from the label.

2. **Find where it is used.** Follow the key from the `en/` locale file to the
   component that renders it, and note how it is shown — a button, a heading, a
   tooltip, a filter option, or a state label. Check the commit that added the
   string to `en/`; it usually carries the component change as well. If the key
   is composed at runtime from a prefix and a variable, search for the prefix
   and work out which values the variable can take; the component tests often
   render one of them. If the key does not appear in the UI at all, fall back
   to the neighboring keys.

3. **Reuse existing zh-TW wording.** Search the locale files under
   `airflow-core/src/airflow/ui/public/i18n/locales/zh-TW/` for the same or
   similar terms; they are the authoritative glossary.

4. **Translate from the English source** in `en/`, the primary reference for
   meaning.

5. **Compare with the other locales.** Check how the same key was translated
   in the other `locales/` folders. A different approach there is not a reason
   to switch on its own, but it is a reason to think again: go back to the
   English string and decide whether the difference points at something you
   missed.

### Additional Terminology

Use the following established translations in the Airflow UI:

- **Triggerer** → 觸發者
- **trigger** → 觸發器
- **partition** → 分區

Ensure these terms are used consistently across the locale.

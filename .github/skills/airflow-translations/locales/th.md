 <!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

# Thai (th)

This document provides Thai-specific guidelines for translating Apache Airflow terminology and documentation.

This document inherits all global rules from the parent SKILL.md.

## Terms to Keep in English

The following technical terms should **remain in English** in Thai translations:

### Core Technical Terms (คำศัพท์ทางเทคนิค)

- **DAG** (Directed Acyclic Graph) - Keep as "DAG"
- **DAG Run** - Keep as "DAG Run"
- **Task Instance** - Keep as "Task Instance"
- **XCom** - Keep as "XCom"
- **Asset** - Keep as "Asset"
- **Backfill** - Keep as "Backfill"
- **Dataset** - Keep as "Dataset"
- **Pool** - Keep as "Pool"
- **Sensor** - Keep as "Sensor"
- **Hook** - Keep as "Hook"
- **Operator** - Keep as "Operator" (โอเปอเรเตอร์) or in English
- **DAGBag** - Keep as "DAGBag"

### UI Components (ส่วนประกอบของอินเทอร์เฟซ)

- **Tree View** - Keep as "Tree View" or translate as "มุมมองต้นไม้"
- **Graph View** - Keep as "Graph View" or translate as "มุมมองกราฟ"
- **Grid View** - Keep as "Grid View" or translate as "มุมมองกริด"
- **Calendar View** - Keep as "Calendar View" or translate as "มุมมองปฏิทิน"
- **Gantt Chart** - Keep as "Gantt Chart" or translate as "แผนภูมิแกนต์"

### Code and Technical References

- All Python class names, function names, and variables
- Configuration keys (e.g., `dag_id`, `task_id`)
- Command-line arguments and flags
- File paths and URLs

## Common Airflow Terms in Thai

### Core Concepts (แนวคิดหลัก)

| English | Thai | Notes |
|---------|------|-------|
| Task | งาน | Standard translation |
| Workflow | เวิร์กโฟลว์ | Transliterated |
| Pipeline | ไปป์ไลน์ | Transliterated |
| Connection | การเชื่อมต่อ | Standard translation |
| Variable | ตัวแปร | Standard translation |
| Provider | ผู้ให้บริการ | Standard translation |
| Trigger | ทริกเกอร์ | Transliterated |
| Scheduler | ตัวกำหนดการ | Standard translation |
| Executor | ผู้ดำเนินการ | Standard translation |
| Worker | ผู้ปฏิบัติงาน | Standard translation |
| Webserver | เว็บเซิร์ฟเวอร์ | Transliterated |
| Database | ฐานข้อมูล | Standard translation |

### Actions (การกระทำ)

| English | Thai | Notes |
|---------|------|-------|
| Run | รัน / ทำงาน | Use "รัน" (transliterated) or "ทำงาน" |
| Execute | ดำเนินการ | Standard translation |
| Clear | ล้าง | Standard translation |
| Retry | ลองใหม่ | Standard translation |
| Fail | ล้มเหลว | Standard translation |
| Mark as Failed | ทำเครื่องหมายว่าล้มเหลว | Phrase |
| Success | สำเร็จ | Standard translation |
| Mark as Success | ทำเครื่องหมายว่าสำเร็จ | Phrase |
| Pause | หยุดชั่วคราว | Standard translation |
| Unpause | ยกเลิกการหยุดชั่วคราว | Phrase |

### States (สถานะ)

| English | Thai | Notes |
|---------|------|-------|
| success | สำเร็จ | Standard translation |
| running | กำลังดำเนินการ | Standard translation |
| failed | ล้มเหลว | Standard translation |
| upstream_failed | ต้นน้ำล้มเหลว | Literal translation |
| skipped | ถูกข้าม | Standard translation |
| queued | อยู่ในคิว | Standard translation |
| scheduled | มีกำหนดการ | Standard translation |
| deferred | เลื่อนเวลา | Standard translation |

## Pluralization Patterns in Thai

Thai language does not have grammatical plural forms like English. Nouns remain the same regardless of quantity. Numbers and quantifiers are used to indicate plurality.

### Using Numerals with Thai

In Airflow UI and messages, numerals are typically formatted as:

- **1 Task**: 1 งาน (1 task)
- **2 Tasks**: 2 งาน (2 tasks) - noun form remains the same
- **Multiple Tasks**: งานหลายงาน (multiple tasks) - using classifier "หลาย"
- **All Tasks**: งานทั้งหมด (all tasks)

### Quantity Indicators

- ไม่มี (none) - 0 items
- หนึ่ง (one) - 1 item
- สอง (two) - 2 items
- หลาย (multiple/several) - 3+ items
- ทั้งหมด (all) - all items
- บางส่วน (some) - some items

## Script and Writing System

### Thai Script

1. **Direction**: Thai text is written from left to right (LTR) like English
2. **Script**: Use Thai script (ตัวอักษรไทย) for Thai translations
3. **Mixed Content**: When mixing Thai with English terms, maintain proper spacing
4. **Punctuation**: Thai uses specific punctuation marks alongside standard punctuation
5. **Numbers**: Arabic numerals (0-9) are commonly used in technical contexts

Example:

```text
งาน DAG รันสำเร็จ (DAG run successful)
```

## Translation Style Guidelines

### 1. Technical Terminology

Keep technical terms like DAG, XCom, Operator in English when:

- They appear in code or configuration examples
- No clear Thai equivalent exists
- The term is widely used in English in the technical community
- Transliteration would make the term less clear

### 2. Transliteration vs Translation

Prefer transliteration for:

- Proper nouns and brand names (Python, Apache, GitHub)
- Technical terms with no direct translation (Workflow, Pipeline, Plugin)

Prefer translation for:

- Common concepts (Task = งาน, Variable = ตัวแปร, Connection = การเชื่อมต่อ)
- UI elements (Button = ปุ่ม, Menu = เมนู, View = มุมมอง)

### 3. UI Labels

- Keep UI labels concise and consistent
- Use standard Thai technical translations where available
- Example: "Tree View" → "มุมมองต้นไม้" or keep "Tree View"
- For technical terms, English is often preferred for clarity

### 4. Verbs and Actions

- Use polite form (รูปคำเป็นทางการ) for UI elements:
  - "Run" → "รัน" (imperative) or "ดำเนินการ" (formal)
  - "Clear" → "ล้าง" (imperative)
  - "Trigger" → "เรียก" (imperative) or "ทริกเกอร์" (transliterated)

### 5. Documentation

- Use formal Thai language (ภาษาไทยรูปแบบทางการ)
- Maintain consistency with terminology throughout
- Provide English terms in parentheses when introducing new technical terms
- Use appropriate honorifics and polite particles when applicable

### 6. Error Messages

- Keep error messages clear and actionable
- Include technical details in English when necessary
- Example: "การรันงานล้มเหลว: Task instance not found"
- Use polite but direct language for errors

## Common Translation Patterns

### 1. "Run" Context

- "Run DAG" → "รัน DAG" or "ดำเนินการ DAG"
- "DAG run" (noun) → "การรัน DAG" or "DAG Run"
- "Run ID" → "รันไอดี" or "Run ID"

### 2. "Task" Context

- "Task failed" → "งานล้มเหลว"
- "Task instance" → "Task Instance" or "อินสแตนซ์งาน"
- "Task ID" → "Task ID" or "ไอดีงาน"

### 3. "DAG" Context

- "DAG run" → "การรัน DAG" or "DAG Run"
- "DAG ID" → "DAG ID" or "ไอดี DAG"
- "Sub DAG" → "DAG ย่อย" or "Sub DAG"

### 4. Configuration

- "Airflow Config" → "การกำหนดค่า Airflow" or "Airflow Config"
- "Connection ID" → "Connection ID" or "ไอดีการเชื่อมต่อ"
- "Pool name" → "Pool name" or "ชื่อพูล"

## Thai Linguistic Considerations

### 1. Word Order

Thai follows SVO (Subject-Verb-Object) word order, similar to English:

- English: "Task runs successfully"
- Thai: "งานรันสำเร็จ" (Task run successful)

### 2. No Articles

Thai language does not use articles (a, an, the):

- "the task" → "งาน" (task)
- "a connection" → "การเชื่อมต่อ" (connection)

### 3. No Tense Inflection

Thai does not conjugate verbs for tense. Time words and context indicate when actions occur:

- "ran" → "รันแล้ว" (ran already) or "รันไปแล้ว" (ran in the past)
- "will run" → "จะรัน" (will run)

### 4. Politeness Markers

In formal documentation and UI, polite particles may be used:

- ครับ (khrap) - for male speakers
- ค่ะ (kha) - for female speakers

However, these are typically omitted in technical documentation to maintain conciseness. In UI text, polite particles are generally not used to maintain a gender-neutral tone.

## Resources for Thai Translators

1. **Thai Technical Terms**: Use established Thai computing terminology
2. **Style Guide**: Follow formal Thai language conventions
3. **Glossary**: Maintain consistency with previously translated Airflow content
4. **Testing**: Test translations to ensure proper rendering and readability
5. **Community**: Refer to Thai technical documentation communities for consistency
6. **Existing Thai Locale Files**: Reference the existing Thai locale files as the authoritative terminology source for consistency with established translations

## Examples

### UI Label Examples

```text
"Tree View" → "มุมมองต้นไม้" or "Tree View"
"Graph View" → "มุมมองกราฟ" or "Graph View"
"Task Instances" → "Task Instances" or "อินสแตนซ์งาน"
"DAG Runs" → "DAG Runs" or "การรัน DAG"
```

### Message Examples

```text
"Task failed" → "งานล้มเหลว"
"DAG run successful" → "การรัน DAG สำเร็จ"
"XCom pushed" → "ดัน XCom แล้ว" or "XCom pushed"
"Connection test failed" → "การทดสอบการเชื่อมต่อล้มเหลว"
```

### Code Examples (Keep in English)

```python
# Don't translate code comments unless necessary
dag = DAG("my_dag", schedule_interval="@daily")
```

## Pluralization in gettext (for .po files)

Thai uses simple pluralization pattern with only two forms:

```text
# Plural expression: 0
# Forms: _one, _other

msgid "%d task"
msgid_plural "%d tasks"
msgstr[0] "%d งาน"
msgstr[1] "%d งาน"
```

Both singular and plural forms use the same translation in Thai. The number suffix remains the same regardless of quantity. For plurality context, words like "ทั้งหมด" (all) can be used to express plurality when appropriate.

## Notes

- This document is based on analysis of existing Airflow locale files and Thai translation patterns
- As translations evolve, update these guidelines to reflect community consensus
- When in doubt, prefer keeping technical terms in English with Thai explanations
- Consistency is key: use the same translation for the same term throughout the UI
- Thai users are generally comfortable with English technical terms, so keeping terms in English is often acceptable

 <!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

# Arabic (ar) Translation Guidelines for Apache Airflow

This document provides Arabic-specific guidelines for translating Apache Airflow terminology and documentation.

## Terms to Keep in English

The following technical terms should **remain in English** in Arabic translations:

### Core Technical Terms (مصطلحات تقنية أساسية)

- **DAG** (Directed Acyclic Graph) - Keep as "DAG"
- **XCom** (Cross-Communication) - Keep as "XCom"
- **Operator** - Keep as "Operator" (المُشغِّل) or in English
- **Task Instance** - Keep as "Task Instance" or translate as "مثيل المهمة"
- **Pool** - Keep as "Pool" (المجموعة) or in English
- **Hook** - Keep as "Hook" or translate as "خطاف"
- **Sensor** - Keep as "Sensor" (المستشعر) or in English

### UI Components (مكونات واجهة المستخدم)

- **Tree View** - Keep as "Tree View" or translate as "عرض الشجرة"
- **Graph View** - Keep as "Graph View" or translate as "عرض المخطط"
- **Gantt Chart** - Keep as "Gantt Chart" or translate as "مخطط جانت"
- **Grid View** - Keep as "Grid View" or translate as "عرض الشبكة"

### Code and Technical References

- All Python class names, function names, and variables
- Configuration keys (e.g., `dag_id`, `task_id`)
- Command-line arguments and flags
- File paths and URLs

## Common Airflow Terms in Arabic

### Core Concepts (المفاهيم الأساسية)

| English | Arabic | Notes |
|---------|--------|-------|
| Task | مهمة | Feminine noun (مهمة) |
| DAG | DAG | Keep in English |
| XCom | XCom | Keep in English |
| Workflow | سير العمل | Masculine noun |
| Pipeline | خط أنابيب | Masculine noun |
| Dataset | مجموعة بيانات | Feminine noun |
| Connection | اتصال | Masculine noun |
| Variable | متغير | Masculine noun |
| Provider | موفر | Masculine noun |
| Trigger | تشغيل / مُشغِّل | Can be verb or noun |
| Scheduler | الجدولة / المجدول | For scheduler component |
| Executor | المنفذ / المُنفِّذ | For executor component |
| Worker | العامل | For worker process |
| Webserver | خادم الويب | Standard translation |
| Database | قاعدة البيانات | Standard translation |

### Actions (الإجراءات)

| English | Arabic | Notes |
|---------|--------|-------|
| Run | تشغيل | Verb: يشغِّل، تشغيل |
| Execute | تنفيذ | Verb: ينفِّذ، تنفيذ |
| Trigger | تشغيل | Verb: يشغِّل، تشغيل |
| Clear | مسح | Verb: يمسح، مسح |
| Retry | إعادة المحاولة | Noun phrase |
| Retry | أعاد المحاولة | Verb phrase |
| Fail | فشل | Verb: يفشل، فشل |
| Mark as Failed | تعليم كمفشل | Phrase |
| Success | نجح | Verb: ينجح، نجاح |
| Mark as Success | تعليم كناجح | Phrase |
| Pause | إيقاف مؤقت | Verb: يوقف مؤقتاً |
| Unpause | إلغاء الإيقاف المؤقت | Verb phrase |

### States (الحالات)

| English | Arabic | Notes |
|---------|--------|-------|
| success | ناجح | Adjective: ناجح (masc.), ناجحة (fem.) |
| running | قيد التشغيل | Phrase |
| failed | فاشل | Adjective: فاشل (masc.), فاشلة (fem.) |
| upstream_failed | فشل المنبع | Phrase |
| skipped | تم التخطي | Phrase |
| upstream_skipped | تم تخطي المنبع | Phrase |
| queued | في قائمة الانتظار | Phrase |
| scheduled | مجدول | Adjective |
| removed | تمت الإزالة | Phrase |
| deferred | مؤجل | Adjective |

## Pluralization Patterns in Arabic

Arabic has complex pluralization rules with different forms:

### Dual Form (المثنى) - for exactly 2

Used with the suffix "ـان" (ān) or "ـين" (ayn):

- مهمة → مهمتان (2 tasks)
- DAG → 2 DAGs (keep English with number)

### Sound Masculine Plural (جمع المذكر السالم)

Suffix "ـون" (ūn) in nominative, "ـين" (īn) in accusative/genitive:

- موفر → موفرون (providers)
- عامل → عمالون (workers)
- متغير → متغيرون (variables)

### Sound Feminine Plural (جمع المؤنث السالم)

Suffix "ـات" (āt):

- مهمة → مهمات (tasks)
- مجموعة → مجموعات (datasets/groups)
- اتصال → اتصالات (connections)

### Broken Plural (الجمع التكسيري)

Irregular patterns:

- خط أنابيب → خطوط أنابيب (pipelines)
- قاعدة → قواعد (databases)

### Using Numerals with Arabic

In Airflow UI and messages, numerals are typically formatted as:

- **Singular**: 1 مهمة (1 task)
- **Dual**: 2 مهمتان (2 tasks) or 2 DAGs
- **Plural (3-10)**: 5 مهام (5 tasks)
- **Plural (11+)**: 15 مهمة (15 tasks)

Note: For numbers 3-10, the noun is typically in plural form. For numbers 11+, the noun uses singular form.

## RTL (Right-to-Left) Considerations

1. **Direction**: Arabic text is written from right to left (RTL)
2. **Mixed Content**: When mixing Arabic with English terms, maintain proper spacing
3. **Punctuation**: Periods, commas, and other punctuation should be placed appropriately
4. **Code Blocks**: Keep code blocks in English/LTR direction
5. **Numbers**: Numbers (0-9) are written left-to-right even in Arabic text

Example:

```text
تم تشغيل DAG بنجاح (DAG run successful)
```

## Gender Agreement

Arabic nouns have grammatical gender that affects adjectives and verbs:

- **Task (مهمة)**: Feminine
  - Singular: مهمة واحدة (one task)
  - Plural: مهمات متعددة (multiple tasks)

- **DAG**: Keep in English, treat as masculine or feminine based on context (لفظ "مخطط" is masculine)
- **Pipeline (خط أنابيب)**: Masculine
- **Connection (اتصال)**: Masculine

## Translation Style Guidelines

### 1. Technical Terminology

- Keep technical terms like DAG, XCom, Operator in English when:
  - They appear in code or configuration examples
  - No clear Arabic equivalent exists
  - The term is widely used in English in the technical community

### 2. UI Labels

- Keep UI labels concise and consistent
- Use standard Arabic technical translations where available
- Example: "Tree View" → "عرض الشجرة" or keep "Tree View"

### 3. Verbs and Actions

- Use imperative form for buttons and actions:
  - "Run" → "شغِّل" (imperative)
  - "Clear" → "امسح" (imperative)
  - "Trigger" → "شغِّل" (imperative)

### 4. Documentation

- Use formal Modern Standard Arabic (MSA)
- Maintain consistency with terminology throughout
- Provide English terms in parentheses when introducing new technical terms

### 5. Error Messages

- Keep error messages clear and actionable
- Include technical details in English when necessary
- Example: "فشل تشغيل المهمة: Task instance not found"

## Common Translation Patterns

### 1. "Run" Context

- "Run DAG" → "شغِّل DAG"
- "DAG run" (noun) → "تشغيل DAG" or "تنفيذ DAG"
- "Run ID" → "معرف التشغيل"

### 2. "Task" Context

- "Task failed" → "فشلت المهمة" (feminine agreement)
- "Task instance" → "مثيل المهمة" or "Task instance"
- "Task ID" → "معرف المهمة" or "Task ID"

### 3. "DAG" Context

- "DAG run" → "تشغيل DAG" or "DAG run"
- "DAG ID" → "معرف DAG" or "DAG ID"
- "Sub DAG" → "DAG فرعي" or "Sub DAG"

### 4. Configuration

- "Airflow Config" → "إعدادات Airflow"
- "Connection ID" → "معرف الاتصال" or "Connection ID"
- "Pool name" → "اسم المجموعة" or "Pool name"

## Resources for Arabic Translators

1. **Arabic Technical Terms**: Use established Arabic computing terminology
2. **Style Guide**: Follow Modern Standard Arabic (MSA) conventions
3. **Glossary**: Maintain consistency with previously translated Airflow content
4. **Testing**: Test translations in RTL layout to ensure proper rendering
5. **Community**: Refer to Arabic technical documentation communities for consistency

## Examples

### UI Label Examples

```text
"Tree View" → "عرض الشجرة" or "Tree View"
"Graph View" → "عرض المخطط" or "Graph View"
"Task Instances" → "أمثلة المهام" or "Task Instances"
"DAG Runs" → "عمليات تشغيل DAG" or "DAG Runs"
```

### Message Examples

```text
"Task failed" → "فشلت المهمة"
"DAG run successful" → "تم تشغيل DAG بنجاح"
"XCom pushed" → "تم دفع XCom"
"Connection test failed" → "فشل اختبار الاتصال"
```

### Code Examples (Keep in English)

```python
# Don't translate code comments unless necessary
dag = DAG("my_dag", schedule_interval="@daily")
```

## Notes

- This document is based on analysis of existing Airflow locale files and Arabic translation patterns
- As translations evolve, update these guidelines to reflect community consensus
- When in doubt, prefer keeping technical terms in English with Arabic explanations
- Consistency is key: use the same translation for the same term throughout the UI

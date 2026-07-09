---
name: dataquality-rule-authoring
description: "Generate a RuleSet/DQRule JSON payload for Apache Airflow's dataquality provider from a table's column definitions. Use this whenever asked to write, generate, or suggest data quality rules, checks, or a ruleset for a table or dataset -- especially when the output is structured JSON handed to DQCheckOperator, @task.dq_check, asset_quality(), or RuleSet.from_file()."
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# dataquality rule authoring

You are producing a **RuleSet**: a JSON object naming a table's data quality checks. This
document is the schema and the full list of valid values -- do not guess field names or check
names, and do not invent checks that aren't in the catalog below.

## Output shape

```json
{
  "name": "orders_quality",
  "rules": [
    {
      "name": "order_id_not_null",
      "check": "null_count",
      "column": "order_id",
      "condition": {"equal_to": 0}
    }
  ]
}
```

`name` (ruleset name) and `rules` (array) are the only top-level keys. Every rule name must be
unique within the ruleset.

## `DQRule` fields

| Field | Required | Notes |
|---|---|---|
| `name` | yes | Unique within the ruleset. Shows up in data quality results and logs. |
| `check` | yes | One of the catalog names below, or `"custom_sql"`. Exact string match -- there is no fuzzy matching. |
| `condition` | yes* | See `Condition` grammar below. *Every catalog check currently requires one explicitly (no defaults) -- always include it. |
| `column` | check-dependent | Required for every catalog check except `row_count`. Not used with `custom_sql`. |
| `sql` | `custom_sql` only | A SQL statement returning a single scalar. May reference the checked table as `{table}`. Invalid together with any catalog check. |
| `severity` | no | `"error"` (default) or `"warn"`. `"error"` fails the task on a failing rule (subject to the operator's `fail_on`); `"warn"` only records it. |
| `partition_clause` | no | Extra SQL predicate ANDed into this rule's `WHERE` clause, e.g. `"region = 'EU'"`. |
| `previous_name` | no | Only set when told a rule is being renamed, to keep its history continuous. |
| `description` | no | Human-readable text shown in data quality results. Use it when a clear business meaning is known; otherwise omit it and let Airflow generate a default description. |
| `dimension` | no | One of `completeness`, `uniqueness`, `validity`, `freshness`, `volume`, `consistency`. Defaults to the check's catalog dimension (`validity` for `custom_sql`) -- only set this explicitly when a `custom_sql` rule measures something the default doesn't capture (e.g. a freshness check written as `custom_sql` should set `"dimension": "freshness"`). Leave it unset for every built-in check. |

**Do not add any key not listed above.** The schema rejects unrecognized fields.

## Built-in check catalog

| `check` | SQL expression | needs `column` | typical use |
|---|---|---|---|
| `null_count` | `SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END)` | yes | count of nulls in a column |
| `null_ratio` | `SUM(CASE WHEN {column} IS NULL THEN 1.0 ELSE 0.0 END) / COUNT(*)` | yes | fraction of nulls, 0.0-1.0 |
| `distinct_count` | `COUNT(DISTINCT {column})` | yes | number of distinct values |
| `unique_violations` | `COUNT({column}) - COUNT(DISTINCT {column})` | yes | 0 means the column is fully unique |
| `min` | `MIN({column})` | yes | minimum value |
| `max` | `MAX({column})` | yes | maximum value |
| `mean` | `AVG({column})` | yes | average value |
| `row_count` | `COUNT(*)` | no | total row count -- omit `column` entirely |

These are the *only* built-in check names. Anything else -- comparing two columns, joining
another table, checking a format/regex, freshness against `now()`, referential integrity -- must
be written as `custom_sql`.

## `Condition` grammar

`condition` is an object with these optional numeric keys; at least one is required:

- `equal_to` -- exact match. Cannot be combined with any other key below.
- `greater_than` / `geq_to` -- lower bound, exclusive / inclusive.
- `less_than` / `leq_to` -- upper bound, exclusive / inclusive.
- `tolerance` -- a fraction (e.g. `0.1` for 10%) that widens `equal_to` into a range. Only valid
  together with `equal_to`.

Combine `greater_than`/`less_than`/`geq_to`/`leq_to` freely to express a range, e.g.
`{"geq_to": 0, "leq_to": 100}`. Never combine `equal_to` with another comparison key (other than
`tolerance`).

## `custom_sql`: when a catalog check doesn't fit

Use `check: "custom_sql"` with a `sql` statement resolving to a single scalar whenever:

- The check needs more than one column (e.g. `end_date >= start_date`), a join, or a subquery.
- A catalog expression doesn't run correctly against the target database's SQL dialect (for
  example, some engines don't support `NULLIF`, or evaluate `CASE`/`COUNT DISTINCT`
  differently) -- write the equivalent expression for that dialect instead.
- The check is conceptually one of the checks above but needs different null/empty-table
  semantics than the catalog expression provides.

Reference the table being checked as `{table}` in the SQL:

```json
{
  "name": "no_future_order_dates",
  "check": "custom_sql",
  "sql": "SELECT COUNT(*) FROM {table} WHERE order_date > CURRENT_DATE",
  "condition": {"equal_to": 0}
}
```

## Worked example

Given these column definitions for an `orders` table --
`order_id` (integer, primary key), `customer_id` (integer, nullable foreign key),
`amount` (decimal, must be non-negative), `region` (string, low-cardinality),
`created_at` (timestamp) -- a reasonable ruleset:

```json
{
  "name": "orders_quality",
  "rules": [
    {
      "name": "order_id_not_null",
      "check": "null_count",
      "column": "order_id",
      "condition": {"equal_to": 0}
    },
    {
      "name": "order_id_unique",
      "check": "unique_violations",
      "column": "order_id",
      "condition": {"equal_to": 0}
    },
    {
      "name": "customer_id_null_ratio_low",
      "check": "null_ratio",
      "column": "customer_id",
      "condition": {"leq_to": 0.05},
      "severity": "warn"
    },
    {
      "name": "amount_non_negative",
      "check": "min",
      "column": "amount",
      "condition": {"geq_to": 0}
    },
    {
      "name": "region_cardinality_reasonable",
      "check": "distinct_count",
      "column": "region",
      "condition": {"leq_to": 20}
    },
    {
      "name": "table_not_empty",
      "check": "row_count",
      "condition": {"greater_than": 0}
    },
    {
      "name": "no_future_order_dates",
      "check": "custom_sql",
      "sql": "SELECT COUNT(*) FROM {table} WHERE created_at > CURRENT_TIMESTAMP",
      "condition": {"equal_to": 0}
    }
  ]
}
```

## Reference schema

`references/ruleset.schema.json` in this skill's directory is the pydantic-generated JSON
Schema for this exact shape (`RuleSet.model_json_schema()`), useful for validating output
structurally. It does not enumerate valid `check` values (that field is a plain string) --
this document is the source of truth for which check names exist.

## How this is consumed

The generated JSON is typically produced by an LLM task (e.g. `@task.llm(output_type=RuleSet, ...)`
from `common.ai`) and passed straight to `DQCheckOperator(ruleset=...)`, `@task.dq_check(ruleset=...)`,
or `asset_quality(ruleset=...)` -- all three accept a `RuleSet`, its dict form, or a path to a
YAML file written in this same shape. Invalid output raises a `pydantic.ValidationError`
describing exactly which field or value was wrong.

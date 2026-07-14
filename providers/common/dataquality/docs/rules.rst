 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

.. _dq:rules:

Rules, rule sets, and checks
============================

Rules are data, not code. A :class:`~airflow.providers.common.dataquality.rules.RuleSet` is a named tuple of
:class:`~airflow.providers.common.dataquality.rules.DQRule` -- plain, serializable objects with no behavior of
their own. They describe *what* to check; :class:`~airflow.providers.common.dataquality.operators.dq_check.DQCheckOperator`
(see :doc:`operators`) is what actually runs them. Rules are usually written by hand, but they
can also be proposed by an LLM -- see :doc:`agents`.

.. code-block:: python

    from airflow.providers.common.dataquality.rules import DQRule, RuleSet

    orders_ruleset = RuleSet(
        name="orders_quality",
        rules=(
            DQRule(
                name="order_id_not_null",
                check="null_count",
                column="order_id",
                condition={"equal_to": 0},
            ),
            DQRule(
                name="row_count_present",
                check="row_count",
                condition={"greater_than": 0},
            ),
        ),
    )

``DQRule`` fields
------------------

- ``name`` -- unique within its ruleset. Shows up in data quality results and logs.
- ``check`` -- one of the built-in checks below, or ``custom_sql``.
- ``condition`` -- the pass/fail condition for the observed value; see `Conditions`_.
- ``column`` -- target column. Required for column-level built-in checks, unused for
  ``row_count`` and ``custom_sql``.
- ``sql`` -- a SQL statement returning a single scalar. Required for, and only valid with,
  ``check="custom_sql"``. May reference the table being checked as ``{table}``.
- ``severity`` -- ``error`` (default) or ``warn``. Controls whether a failing rule fails the
  task, subject to the operator's ``fail_on`` (see :doc:`operators`).
- ``partition_clause`` -- extra SQL predicate ANDed into this rule's ``WHERE`` clause, e.g.
  ``"region = 'EU'"``. Combines with the operator-level ``partition_clause``, if any.
- ``previous_name`` -- set when renaming a rule so its execution history stays continuous
  (see `Identity and history`_).
- ``id`` -- explicit, stable identity for this rule's history, used verbatim as ``rule_uid``
  instead of the derived hash (see `Identity and history`_).
- ``description`` -- optional human-readable text shown in data quality results.
  When omitted, the provider generates a short default description from the rule and condition.
- ``dimension`` -- one of ``completeness``, ``uniqueness``, ``validity``, ``freshness``,
  ``volume``, ``consistency``. Defaults to the check's catalog dimension (see the table below;
  ``validity`` for ``custom_sql``) when left unset. Only set this explicitly for a ``custom_sql``
  rule that measures something the default dimension doesn't capture.

Built-in checks
----------------

Each built-in check is backed by a plain SQL expression, rendered with ``{column}`` for
column-level checks:

.. list-table::
    :header-rows: 1

    * - Check
      - SQL expression
      - Column required
      - Default dimension
    * - ``null_count``
      - ``SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END)``
      - yes
      - ``completeness``
    * - ``null_ratio``
      - ``SUM(CASE WHEN {column} IS NULL THEN 1.0 ELSE 0.0 END) / COUNT(*)``
      - yes
      - ``completeness``
    * - ``distinct_count``
      - ``COUNT(DISTINCT {column})``
      - yes
      - ``uniqueness``
    * - ``unique_violations``
      - ``COUNT({column}) - COUNT(DISTINCT {column})``
      - yes
      - ``uniqueness``
    * - ``min``
      - ``MIN({column})``
      - yes
      - ``validity``
    * - ``max``
      - ``MAX({column})``
      - yes
      - ``validity``
    * - ``mean``
      - ``AVG({column})``
      - yes
      - ``validity``
    * - ``row_count``
      - ``COUNT(*)``
      - no
      - ``volume``

.. note::

    ``null_ratio`` divides by ``COUNT(*)`` with no guard against an empty table (or an empty
    partition, when combined with ``partition_clause``); running it against zero rows is a
    division-by-zero at the database level, not a clean "not applicable" result. Guard with a
    ``row_count`` rule upstream, or a ``partition_clause`` that guarantees a non-empty set. A
    portable guard would wrap the denominator in ``NULLIF(COUNT(*), 0)``, but ``NULLIF`` isn't
    supported by every ``DbApiHook`` (see `Supported checks and databases`_) -- this is one
    instance of the general rule below: if a built-in check's SQL doesn't work against your
    database, express it as ``custom_sql`` instead.

Conditions
-----------

A :class:`~airflow.providers.common.dataquality.rules.Condition` is the pass/fail rule applied to the observed
value, using the same grammar as the ``common.sql`` check operators:

- ``equal_to`` -- exact match. Cannot be combined with any other comparison.
- ``greater_than`` / ``geq_to`` -- lower bound, exclusive/inclusive.
- ``less_than`` / ``leq_to`` -- upper bound, exclusive/inclusive.
- ``tolerance`` -- a percentage that widens the comparison bounds.

``greater_than``/``less_than``/``geq_to``/``leq_to`` may be combined to express a range
(e.g. ``{"geq_to": 0, "leq_to": 10}``).

``custom_sql``: the escape hatch
----------------------------------

Built-in checks are all single-column. The moment a rule needs to compare two columns, join
another table, or use a function the catalog doesn't cover, use ``custom_sql`` -- any SQL
statement that resolves to a single scalar, evaluated exactly like a built-in check:

.. exampleinclude:: /../src/airflow/providers/common/dataquality/example_dags/example_dq_check_custom_sql.py
    :language: python
    :start-after: [START howto_operator_dq_check_custom_sql]
    :end-before: [END howto_operator_dq_check_custom_sql]

Supported checks and databases
--------------------------------

Built-in checks are plain SQL expressions executed through whichever ``common.sql``
:class:`~airflow.providers.common.sql.hooks.sql.DbApiHook` your connection resolves to.
Airflow does not validate those expressions per database dialect.

The catalog intentionally uses simple expressions that work on common relational databases
and many distributed SQL engines, but support is not guaranteed for every ``DbApiHook``.
Some hooks expose non-standard SQL layers and may reject or evaluate an expression differently.

For example, ``null_ratio`` is currently rendered as:

.. code-block:: text

    SUM(CASE WHEN {column} IS NULL THEN 1.0 ELSE 0.0 END) / COUNT(*)

This is simple and portable, but it is not guarded against an empty table or an empty
partition. A database-specific version could use a different expression, for example:

.. code-block:: text

    CASE WHEN COUNT(*) = 0 THEN NULL
         ELSE SUM(CASE WHEN {column} IS NULL THEN 1.0 ELSE 0.0 END) / COUNT(*) END

If a built-in check does not work for your database or you need different empty-table
semantics, use ``custom_sql`` and write the expression for your dialect.

Loading rules from YAML
-------------------------

Anywhere a ``RuleSet`` is accepted -- ``DQCheckOperator(ruleset=...)``,
``@task.dq_check(ruleset=...)``, :func:`~airflow.providers.common.dataquality.assets.asset_quality` -- a path
string is accepted too, and resolved via :meth:`~airflow.providers.common.dataquality.rules.RuleSet.from_file`
at Dag-parse time. This keeps rules editable by people who don't write Python.

.. exampleinclude:: /../src/airflow/providers/common/dataquality/example_dags/orders_ruleset.yaml
    :language: yaml

.. exampleinclude:: /../src/airflow/providers/common/dataquality/example_dags/example_dq_ruleset_from_yaml.py
    :language: python
    :start-after: [START howto_operator_dq_check_ruleset_from_yaml]
    :end-before: [END howto_operator_dq_check_ruleset_from_yaml]

Identity and history
----------------------

Each rule has a stable ``rule_uid``, a hash of the fields that define *what is being measured*:
``name`` (or ``previous_name``, if set), ``check``, ``column``, ``sql``, and ``condition``.
Everything else -- including ``severity``, ``partition_clause``, ``description``, and
``dimension`` -- can change between Dag runs without breaking the rule's execution history,
because it isn't part of the identity hash.

Renaming a rule outright would normally start a new history under the new name; set
``previous_name`` to the old name for one deploy to carry the old identity forward instead:

.. code-block:: python

    DQRule(
        name="order_id_is_unique",  # renamed from order_id_unique
        previous_name="order_id_unique",
        check="unique_violations",
        column="order_id",
        condition={"equal_to": 0},
    )

The derived hash isn't guaranteed unique across every possible ruleset: two rules can end up
with the same ``rule_uid`` if their identity fields happen to line up (for example, a renamed
rule's ``previous_name`` matching another rule's current ``name``, ``check``, ``column``, and
``condition``). ``RuleSet`` validates against this and raises if it happens. If you'd rather
not depend on the hash at all, set ``id`` and it's used as the ``rule_uid`` directly:

.. code-block:: python

    DQRule(
        name="order_id_is_unique",
        previous_name="order_id_unique",
        check="unique_violations",
        column="order_id",
        condition={"equal_to": 0},
        id="order_id_uniqueness",  # stable regardless of future renames or condition tweaks
    )

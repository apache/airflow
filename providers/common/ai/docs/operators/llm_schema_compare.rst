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

.. _howto/operator:llm_schema_compare:

``LLMSchemaCompareOperator``
============================

Use :class:`~airflow.providers.common.ai.operators.llm_schema_compare.LLMSchemaCompareOperator`
to compare schemas across different database systems and detect drift using LLM reasoning.

The operator introspects schemas from multiple data sources and uses an LLM to identify
mismatches that would break data loading. The LLM handles complex cross-system type
mapping that simple equality checks miss (e.g., ``varchar(255)`` vs ``string``,
``timestamp`` vs ``timestamptz``).

The result is a structured :class:`~airflow.providers.common.ai.operators.llm_schema_compare.SchemaCompareResult`
containing a list of mismatches with severity levels, descriptions, and suggested actions.

.. seealso::
    :ref:`Connection configuration <howto/connection:pydantic_ai>`

Basic Usage
-----------

Provide ``db_conn_ids`` pointing to two or more database connections and
``table_names`` to compare. The operator introspects each table via
``DbApiHook.get_table_schema()`` and sends the schemas to the LLM:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_schema_compare.py
    :language: python
    :start-after: [START howto_operator_llm_schema_compare_basic]
    :end-before: [END howto_operator_llm_schema_compare_basic]

Full Context Strategy
---------------------

Set ``context_strategy="full"`` to include primary keys, foreign keys, and indexes
in the schema context sent to the LLM.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_schema_compare.py
    :language: python
    :start-after: [START howto_operator_llm_schema_compare_full]
    :end-before: [END howto_operator_llm_schema_compare_full]

With Object Storage
-------------------

Use ``data_sources`` with
:class:`~airflow.providers.common.sql.config.DataSourceConfig` to include
object-storage sources (S3 Parquet, CSV, Iceberg, etc.) in the comparison.
These can be freely combined with ``db_conn_ids``:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_schema_compare.py
    :language: python
    :start-after: [START howto_operator_llm_schema_compare_datasource]
    :end-before: [END howto_operator_llm_schema_compare_datasource]

Customizing the System Prompt
-----------------------------

The operator ships with a ``DEFAULT_SYSTEM_PROMPT`` that teaches the LLM about
cross-system type equivalences (e.g., ``varchar`` vs ``string``, ``bigint`` vs
``int64``) and severity-level definitions (``critical``, ``warning``, ``info``).

When you pass a custom ``system_prompt``, it **replaces** the default entirely.
If you want to **keep** the built-in rules and add any specific instructions
on top, concatenate them:

.. code-block:: python

    from airflow.providers.common.ai.operators.llm_schema_compare import (
        DEFAULT_SYSTEM_PROMPT,
        LLMSchemaCompareOperator,
    )

    LLMSchemaCompareOperator(
        task_id="compare_with_custom_rules",
        prompt="Compare schemas and flag breaking changes",
        llm_conn_id="pydanticai_default",
        db_conn_ids=["postgres_source", "snowflake_target"],
        table_names=["customers"],
        system_prompt=DEFAULT_SYSTEM_PROMPT
        + ("Project-specific rules:\n" "- Snowflake VARIANT columns are compatible with PostgreSQL jsonb.\n"),
    )

TaskFlow Decorator
------------------

The ``@task.llm_schema_compare`` decorator lets you write a function that returns
the prompt. The decorator handles schema introspection, LLM comparison, and
structured output:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_schema_compare.py
    :language: python
    :start-after: [START howto_decorator_llm_schema_compare]
    :end-before: [END howto_decorator_llm_schema_compare]

Conditional ETL Based on Schema Compatibility
----------------------------------------------

The operator returns a dict with a ``compatible`` boolean. Use it with
``@task.branch`` to gate downstream ETL on schema compatibility:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_schema_compare.py
    :language: python
    :start-after: [START howto_operator_llm_schema_compare_conditional]
    :end-before: [END howto_operator_llm_schema_compare_conditional]

Structured Output
-----------------

The operator always returns a dict (serialized from
:class:`~airflow.providers.common.ai.operators.llm_schema_compare.SchemaCompareResult`)
with these fields:

- ``compatible`` (bool): ``False`` if any critical mismatches exist.
- ``mismatches`` (list): Each mismatch contains:

  - ``source`` / ``target``: The data source labels.
  - ``column``: Column where the mismatch was detected.
  - ``source_type`` / ``target_type``: The data types in each system.
  - ``severity``: ``"critical"``, ``"warning"``, or ``"info"``.
  - ``description``: Human-readable explanation.
  - ``suggested_action``: Recommended resolution.
  - ``migration_query``: Suggested migration SQL.

- ``summary`` (str): High-level summary of the comparison.

Parameters
----------

- ``prompt``: Instructions for the LLM on what to compare and flag.
- ``llm_conn_id``: Airflow connection ID for the LLM provider.
- ``model_id``: Model identifier (e.g. ``"openai:gpt-5"``). Overrides the
  connection's extra field.
- ``system_prompt``: Instructions included in the LLM system prompt. Defaults to
  ``DEFAULT_SYSTEM_PROMPT`` which contains cross-system type equivalences and
  severity definitions. Passing a value **replaces** the default — concatenate
  with ``DEFAULT_SYSTEM_PROMPT`` to extend it (see
  :ref:`Customizing the System Prompt <howto/operator:llm_schema_compare>` above).
- ``agent_params``: Additional keyword arguments passed to the pydantic-ai
  ``Agent`` constructor.
- ``db_conn_ids``: List of database connection IDs to compare. Each must resolve
  to a ``DbApiHook``.
- ``table_names``: Tables to introspect from each ``db_conn_id``.
- ``data_sources``: List of ``DataSourceConfig`` objects for object-storage or
  catalog-managed sources.
- ``context_strategy``: To fetch primary keys, foreign keys, and indexes.``full`` or ``basic``,
  strongly recommended for cross-system comparisons. default is ``full``

Logging
-------

After each LLM call, the operator logs a summary with model name, token usage,
and request count at INFO level. See :ref:`AgentOperator — Logging <howto/operator:agent>`
for details on the log format.

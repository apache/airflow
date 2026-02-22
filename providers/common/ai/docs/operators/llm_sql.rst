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

LLM Operators
=============

LLM Operators are specialized Airflow operators designed to interact with LLMs in various different ways example to generate sql queries based on provided prompts.


SQL LLM Operators
-----------------

SQL LLM Based operators leverages the capabilities ``DBApiHook`` in airflow to get the table schemas and use it to generate SQL queries. see the currently supported databases to extract schema dynamically using
existing ``DBApiHook`` or optionally provide the table schema in the input datasource config.

Current LLM Operators uses the Pydantic AI framework to connect to different LLM providers.

Supported LLM Providers `<https://ai.pydantic.dev/models/overview/>`_.

Provider Model Examples
^^^^^^^^^^^^^^^^^^^^^^^

The ``provider_model`` in the operator input follows the pattern ``provider:model_name``.

Examples:

- **Google Vertex AI**: ``google-vertex:gemini-3-pro-preview``
- **Anthropic**: ``anthropic:claude-sonnet-4-5``
- **AWS Bedrock**: ``bedrock:anthropic.claude-sonnet-4-5-20250929-v1:0``

Supported Databases
-------------------
Supported databases to extract schema dynamically using existing ``DBApiHook`` `<https://airflow.apache.org/docs/apache-airflow-providers-common-sql/stable/supported-database-types.html>`_.

Operators
---------

.. _howto/operator:LLMSQLQueryOperator:

Use the :class:`~airflow.providers.common.ai.operators.llm_sql.LLMSQLQueryOperator` to generate SQL query using LLMs based on provided prompts and database schema.

Parameters
----------

* ``datasource_configs`` (list[DataSourceConfig], required): List of datasource configurations
* ``prompts`` (list[str], required): List of prompts to generate SQL queries.
* ``provider_model`` (str, optional): LLM provider and model to use for generating SQL queries. Format should be ``provider:model`` (e.g., ``google:gemini-3-flash-preview``). If not provided, it will try to use it from the connection id.


.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_sql_query_operator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_llm_sql_query_operator]
    :end-before: [END howto_operator_llm_sql_query_operator]

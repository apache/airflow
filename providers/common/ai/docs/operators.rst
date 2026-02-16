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
These operators leverage the capabilities DBApi Hook in airflow to get the database schema and use it to generate SQL queries. see the currently supported databases to extract schema dynamically using
existing DBApi Hooks or optionally provide the database schema in the input datasource config.

Current LLM Operators uses the Pydantic AI framework to connect to different LLM providers.

Supported LLM Providers
-----------------------

- OpenAI
- Google
- Anthropic
- GitHub

* See for more configuration details ``https://ai.pydantic.dev/models/overview/``

Supported Databases
-------------------
Supported databases to extract schema dynamically using existing ``DBApiHook``.

- Postgres

Operators
---------

.. _howto/operator:LLMSQLQueryOperator:

Use the :class:`~airflow.providers.common.ai.operators.llm_sql.LLMSQLQueryOperator` to generate SQL query using LLMs based on provided prompts and database schema.

Parameters
----------

* ``datasource_configs`` (list[DataSourceConfig], required): List of datasource configurations
* ``prompts`` (list[str], required): List of prompts to generate SQL queries.
* ``provider_model`` (str, optional): LLM provider and model to use for generating SQL queries. Format should be ``provider:model`` (e.g., ``google:gemini-3-flash-preview``). If not provided, it will try to use it from the connection id.


.. exampleinclude:: /../tests/system/common/ai/example_llm_sql_query_operator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_llm_sql_query_operator]
    :end-before: [END howto_operator_llm_sql_query_operator]

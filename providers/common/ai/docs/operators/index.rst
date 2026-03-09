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

Common AI Operators
===================

Choosing the right operator
---------------------------

The common-ai provider ships four operators (and matching ``@task`` decorators). Use this table
to pick the one that fits your use case:

.. list-table::
   :header-rows: 1
   :widths: 40 30 30

   * - Need
     - Operator
     - Decorator
   * - Single prompt → text or structured output
     - :class:`~airflow.providers.common.ai.operators.llm.LLMOperator`
     - ``@task.llm``
   * - LLM picks which downstream task runs
     - :class:`~airflow.providers.common.ai.operators.llm_branch.LLMBranchOperator`
     - ``@task.llm_branch``
   * - Natural-language → SQL generation (no execution)
     - :class:`~airflow.providers.common.ai.operators.llm_sql.LLMSQLQueryOperator`
     - ``@task.llm_sql``
   * - Multi-turn reasoning with tools (DB queries, API calls, etc.)
     - :class:`~airflow.providers.common.ai.operators.agent.AgentOperator`
     - ``@task.agent``

**LLMOperator / @task.llm** — stateless, single-turn calls. Use this for classification,
summarization, extraction, or any prompt that produces one response. Supports structured output
via a ``response_format`` Pydantic model.

**AgentOperator / @task.agent** — multi-turn tool-calling loop. The model decides which tools to
invoke and when to stop. Use this when the LLM needs to take actions (query databases, call APIs,
read files) to produce its answer. You configure available tools through ``toolsets``.

AgentOperator *works* without toolsets — pydantic-ai supports tool-less agents for multi-turn
reasoning — but if you don't need tools, ``LLMOperator`` is simpler and more explicit.

Operator guides
---------------

.. toctree::
    :maxdepth: 1
    :glob:

    *

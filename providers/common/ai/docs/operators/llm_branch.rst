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

.. _howto/operator:llm_branch:

``LLMBranchOperator``
=====================

Use :class:`~airflow.providers.common.ai.operators.llm_branch.LLMBranchOperator`
for LLM-driven branching — where the LLM decides which downstream task(s) to
execute.

The operator discovers downstream tasks automatically from the DAG topology
and presents them to the LLM as a constrained enum via pydantic-ai structured
output. No text parsing or manual validation is needed.

.. seealso::
    :ref:`Connection configuration <howto/connection:pydantic_ai>`

Basic Usage
-----------

Connect the operator to downstream tasks. The LLM chooses which branch to
execute based on the prompt:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_branch.py
    :language: python
    :start-after: [START howto_operator_llm_branch_basic]
    :end-before: [END howto_operator_llm_branch_basic]

Multiple Branches
-----------------

Set ``allow_multiple_branches=True`` to let the LLM select more than one
downstream task. All selected branches run; unselected branches are skipped:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_branch.py
    :language: python
    :start-after: [START howto_operator_llm_branch_multi]
    :end-before: [END howto_operator_llm_branch_multi]

TaskFlow Decorator
------------------

The ``@task.llm_branch`` decorator wraps ``LLMBranchOperator``. The function
returns the prompt string; all other parameters are passed to the operator:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_branch.py
    :language: python
    :start-after: [START howto_decorator_llm_branch]
    :end-before: [END howto_decorator_llm_branch]

With multiple branches:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_branch.py
    :language: python
    :start-after: [START howto_decorator_llm_branch_multi]
    :end-before: [END howto_decorator_llm_branch_multi]

How It Works
------------

At execution time, the operator:

1. Reads ``self.downstream_task_ids`` from the DAG topology.
2. Creates a dynamic ``Enum`` with one member per downstream task ID.
3. Passes that enum as ``output_type`` to ``pydantic-ai``, constraining the LLM to
   valid task IDs only.
4. Converts the LLM's structured output to task ID string(s) and calls
   ``do_branch()`` to skip non-selected downstream tasks.

Parameters
----------

- ``prompt``: The prompt to send to the LLM (operator) or the return value of the
  decorated function (decorator).
- ``llm_conn_id``: Airflow connection ID for the LLM provider.
- ``model_id``: Model identifier (e.g. ``"openai:gpt-5"``). Overrides the connection's extra field.
- ``system_prompt``: System-level instructions for the agent. Supports Jinja templating.
- ``allow_multiple_branches``: When ``False`` (default) the LLM returns a single
  task ID. When ``True`` the LLM may return one or more task IDs.
- ``agent_params``: Additional keyword arguments passed to the pydantic-ai ``Agent``
  constructor (e.g. ``retries``, ``model_settings``). Supports Jinja templating.

Logging
-------

After each LLM call, the operator logs a summary with model name, token usage,
and request count at INFO level. See :ref:`AgentOperator — Logging <howto/operator:agent>`
for details on the log format.

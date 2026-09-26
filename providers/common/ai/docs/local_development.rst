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

.. _howto/local_development:

Develop and test AI tasks locally
=================================

Getting an agent's prompt and tools right takes many reruns. Do them in a
notebook, with no Airflow running, then test the finished Dag without an API key.

Iterate in a notebook
---------------------

``AgentOperator`` builds its agent from a
:class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook` and the
toolsets you pass it. Build the same agent yourself and call it directly. Hooks
read connections from ``AIRFLOW_CONN_<ID>`` environment variables, so no
scheduler, metadata database, or ``airflow db migrate`` is involved:

.. code-block:: bash

    export AIRFLOW_CONN_MY_LLM='{"conn_type": "pydanticai", "password": "sk-...", "extra": {"model": "openai:gpt-5"}}'
    export AIRFLOW_CONN_ORDERS_DB='{"conn_type": "sqlite", "host": "/tmp/orders.db"}'

.. code-block:: python

    from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
    from airflow.providers.common.ai.toolsets.sql import SQLToolset

    agent = PydanticAIHook.get_hook("my_llm").create_agent(
        instructions="You answer questions about the orders database.",
        toolsets=[SQLToolset(db_conn_id="orders_db", allowed_tables=["orders"])],
    )
    result = agent.run_sync("How many orders are there?")
    print(result.output)

Change the instructions, tools, or prompt and rerun the cell. To talk to the agent
instead, pydantic-ai's ``agent.to_cli_sync()`` opens a chat in the terminal (it
needs the ``pydantic-ai-slim[cli]`` extra).

When the answers look right, copy the arguments into the task. ``instructions``
becomes ``system_prompt``, the connection ID goes in ``llm_conn_id``, ``toolsets``
and ``output_type`` keep their names, and any other agent argument, such as
``retries``, goes in ``agent_params``.

.. code-block:: python

    # dags/orders_report.py
    from airflow.providers.common.ai.toolsets.sql import SQLToolset
    from airflow.sdk import dag, task


    @dag(schedule=None)
    def orders_report():
        @task.agent(
            llm_conn_id="my_llm",
            system_prompt="You answer questions about the orders database.",
            toolsets=[SQLToolset(db_conn_id="orders_db", allowed_tables=["orders"])],
        )
        def orders_question() -> str:
            return "How many orders are there?"

        orders_question()


    orders_report()

To keep prompt data on your machine while iterating, point ``my_llm`` at a model
you serve yourself; see :ref:`howto/self_hosted_models`.

Test an agent task without calling a model
------------------------------------------

Script the model's replies with pydantic-ai's
`FunctionModel <https://pydantic.dev/docs/ai/guides/testing/>`__ and patch
``PydanticAIHook.get_conn``, which is where ``AgentOperator`` and ``@task.agent``
get their model. ``dag.test()`` then runs the real task, including template rendering,
toolset calls against your test database and XCom, without network access:

.. code-block:: python

    # tests/test_orders_report.py
    from unittest import mock

    import pydantic_ai.models
    from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart, ToolReturnPart
    from pydantic_ai.models.function import FunctionModel

    from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook

    from orders_report import orders_report  # dags/orders_report.py from above

    # Fail the test instead of calling a real model by accident.
    pydantic_ai.models.ALLOW_MODEL_REQUESTS = False


    def scripted_model(messages, info):
        returns = [p for m in messages for p in m.parts if isinstance(p, ToolReturnPart)]
        if not returns:
            return ModelResponse(parts=[ToolCallPart("query", {"sql": "SELECT COUNT(*) AS n FROM orders"})])
        return ModelResponse(parts=[TextPart(f"Result: {returns[-1].content}")])


    def test_report_counts_orders():
        with mock.patch.object(
            PydanticAIHook, "get_conn", autospec=True, return_value=FunctionModel(scripted_model)
        ):
            dag_run = orders_report().test()

        assert dag_run.state == "success"

The first reply calls the ``query`` tool, which runs against the test database
behind ``orders_db``; the second turns the tool result into the answer. The task
still looks up ``my_llm``, so define it in the test environment too; it needs no
key:

.. code-block:: bash

    export AIRFLOW_CONN_MY_LLM='{"conn_type": "pydanticai", "extra": {"model": "openai:gpt-5"}}'

``dag.test()`` needs a metadata database, so run ``airflow db migrate`` once in the
test environment. It also looks the Dag up in your Dags folder, so import the Dag
from its file instead of defining it in the test, and put the Dags folder on the
import path (pytest's ``pythonpath`` setting).

Setting a ``pydanticai`` connection's model to ``test`` swaps in pydantic-ai's
``TestModel`` instead, with no patching. It calls *every* tool once with generated
arguments, against your real connections. That suits ``@task.llm`` and agents whose
tools accept any input, but a ``SQLToolset`` rejects the generated SQL until its
retries run out, and the task fails.

Check the answers themselves
----------------------------

The test above scripts the model's replies, so it can't tell you whether a changed
prompt still gives good answers. For that, run a set of cases with expected
outcomes against the notebook agent from the first section, using a real model and
a library such as `pydantic-evals <https://pydantic.dev/docs/ai/evals/evals/>`__,
before you ship the change.

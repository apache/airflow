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

.. _howto/why-common-ai:

Why Common AI
=============

You can call a model from Airflow without this provider: put an agent framework or a
vendor SDK in a ``@task`` and it runs. The trouble starts when the agent does real work.
On retry, every model and tool call runs again and is billed again. A spend limit resets
with each attempt. Pausing for a person means wiring up a separate approval task, and
giving the agent tools means writing them yourself. This provider handles those cases
for agents, and its retry policies bring a model's judgment to any task that fails.

For a single model call with no tools, where a retry costs little, a plain ``@task`` is
enough; see `When a plain task is enough`_.

The same job, two ways
----------------------

An agent reads a reports bucket, queries the orders table and says whether the numbers
agree. Without this provider, a Pydantic AI agent in a ``@task`` looks like this, with
the model key in the worker's environment and the tools written by hand:

.. code-block:: python

    from pydantic_ai import Agent

    from airflow.sdk import BaseHook, ObjectStoragePath, dag, task


    @dag
    def check_report_plain():
        @task(retries=2)
        def check_report() -> str:
            # The key comes from OPENAI_API_KEY in the worker's environment.
            agent = Agent(
                "openai:gpt-5",
                instructions=(
                    "You check reports against the warehouse. Read the report files, query "
                    "the orders table, and say whether the numbers agree."
                ),
            )

            @agent.tool_plain
            def query(sql: str) -> list:
                # Runs whatever SQL the model writes, on any table the connection can read.
                return BaseHook.get_connection("warehouse").get_hook().get_records(sql)

            @agent.tool_plain
            def read_report(name: str) -> str:
                reports = ObjectStoragePath("s3://acme-reports/finance/", conn_id="aws_default")
                return (reports / name).read_text()

            return agent.run_sync("Does the September report's revenue match the orders table?").output

        check_report()

This is the same job with ``AgentOperator``. Its ``system_prompt`` is passed to the agent
as Pydantic AI ``instructions``:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_why_common_ai.py
    :language: python
    :start-after: [START example_why_common_ai]
    :end-before: [END example_why_common_ai]

Both need an LLM connection or key, a ``warehouse`` connection with an ``orders`` table,
and a bucket of report files. What differs:

- **The tools.** The hand-written ``query`` runs any SQL the model writes, including
  ``DELETE``, on any table the connection can reach, and returns every row it gets.
  ``SQLToolset`` allows only the ``orders`` table, rejects statements that write, and caps
  the rows and bytes it returns. ``ObjectStorageToolset`` keeps reads under the prefix you
  gave it. Both run Airflow's secrets masker over results, which hides values Airflow knows
  are secret, such as connection passwords, not personal data in your tables.
- **The model.** The plain task names a vendor in code and reads its key from the
  environment. ``AgentOperator`` takes both from a connection, so moving to another vendor
  or adding a fallback is a connection change. See :doc:`model_providers`.
- **A retry.** The plain task starts again from the beginning and pays for every call
  again. ``durable=True`` replays the calls that finished and continues from there. Before
  Airflow 3.3 it needs ``[common.ai] durable_cache_path`` set; see :doc:`durable_execution`.
- **Spend.** In the plain task, Pydantic AI's default limit of 50 model requests applies
  to each attempt on its own, so three attempts can make up to 150. With ``usage_limits``
  on Airflow 3.3 and later, one total covers all three attempts, and the task fails once
  it passes 30 requests or 200,000 tokens. Calls replayed from the durable cache do not
  count.
- **The task log.** ``AgentOperator`` logs each tool call it makes.

:doc:`frameworks/pydantic_ai` lists what ``AgentOperator`` adds on top of an agent you
run yourself, including when you keep your own agent and use only this provider's
toolsets.

What a plain task leaves to you
-------------------------------

.. list-table::
    :header-rows: 1
    :widths: 28 40 32

    * - When this happens
      - What this provider does
      - Where it is documented
    * - The task retries after the agent has already made model and tool calls.
      - ``durable=True`` on ``AgentOperator`` replays the calls that finished and runs
        the rest live. If the prompt, model or tools changed since the failed attempt,
        the affected calls run again.
      - :doc:`durable_execution`
    * - A run must not spend past a limit, retries included.
      - ``usage_limits`` counts every attempt of the task instance together on
        Airflow 3.3 and later, and fails the task once the total passes a limit.
        Pydantic AI's own limits apply to one run at a time.
      - :ref:`Usage budget across attempts <agent-usage-budget>`
    * - A person has to approve, edit or reject a model's output before the Dag goes
        on.
      - ``require_approval=True`` on ``LLMOperator`` and the operators built on it
        pauses the task after it generates output, without holding a worker slot. On
        Airflow 3.1 and 3.2 the wait runs on the triggerer. Airflow 3.1 and later. A
        plain task can get the same step from the standard provider's
        ``HITLOperator`` placed after it.
      - :doc:`approval_gates`
    * - A person has to approve a tool call before it runs, such as a refund or a write
        to a production table.
      - A tool marked with Pydantic AI's approval API pauses ``AgentOperator`` before the
        call, without holding a worker slot. Airflow 3.3 and later. Not with
        ``durable=True`` or ``enable_hitl_review=True``; with either, a tool marked for
        approval fails the task.
      - :doc:`tool_approval`
    * - A reviewer wants to send an agent's answer back with feedback, several times.
      - ``enable_hitl_review=True`` on ``AgentOperator`` opens a review loop with a chat
        UI and a REST API. The task holds its worker slot for the whole review.
        Airflow 3.1 and later. For a single approve or reject of an agent's answer, put
        the standard provider's ``ApprovalOperator`` after the agent task instead; it
        waits without a worker slot.
      - :doc:`hitl_review`
    * - The agent needs your databases, object storage or an API you already have a
        hook for.
      - ``SQLToolset``, ``HookToolset`` and ``ObjectStorageToolset`` build tools from an
        Airflow connection, with table allow-lists, read-only SQL and size limits. Results
        pass through Airflow's secrets masker before the model sees them. They work in an
        agent you run yourself too.
      - :doc:`toolsets/index`
    * - A model vendor is down.
      - ``fallback_conn_ids`` fails over to other connections in order.
      - :doc:`provider_fallback`
    * - Thousands of prompts need an answer, and not within seconds.
      - The batch operator sends them through the OpenAI or Anthropic batch API at
        roughly half the per-token cost. A retry re-attaches to the batch already
        submitted if the prompts, model, output schema and connection are unchanged.
        With ``deferrable=True`` the wait does not hold a worker slot.
      - :doc:`operators/llm_batch`

The provider runs on Airflow 2.11 and later; a row that needs a newer Airflow says so.

Decide whether a failed task retries
------------------------------------

Retry policies work on any task, including one that never calls a model. A retry policy
decides whether a failed task should retry, and the two in this provider ask a model:

.. code-block:: python

    from airflow.providers.common.ai.policies.retry import LLMRetryPolicy
    from airflow.sdk import task


    @task(retries=5, retry_policy=LLMRetryPolicy(llm_conn_id="pydanticai_default"))
    def call_partner_api(): ...

When the task fails, the model reads the exception and decides whether to retry and how
long to wait, so it can decide to wait out a rate limit and to fail a revoked credential
at once. The exception text goes through the secrets masker before the model sees it,
and ``fallback_rules`` decide when the model cannot be reached. ``ClassifierRetryPolicy``
has a cheaper model pick from categories you define, and can hand the cases it is unsure
of to an ``LLMRetryPolicy``. Both need Airflow 3.3 or later. See :doc:`retry_policies`.

When a plain task is enough
---------------------------

- One model call with no tools, where running it again on retry costs little.
- An agent you already run in another framework, where you need none of the above. You
  can still give it this provider's toolsets; see :doc:`frameworks/index`.
- A vendor's native API this provider does not wrap, such as the OpenAI Responses API
  or a vendor's own embeddings endpoint. Use the vendor's provider; the table on the
  :doc:`landing page <index>` lists them.

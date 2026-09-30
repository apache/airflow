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

Airflow hooks as tools: ``HookToolset``
=======================================

Generic adapter that exposes selected methods of any Airflow Hook as
pydantic-ai tools via introspection. Requires an explicit ``allowed_methods``
list; there is no auto-discovery.

.. code-block:: python

    from airflow.providers.http.hooks.http import HttpHook
    from airflow.providers.common.ai.toolsets.hook import HookToolset

    http_hook = HttpHook(http_conn_id="my_api")

    toolset = HookToolset(
        http_hook,
        allowed_methods=["run"],
        tool_name_prefix="http_",
    )

For each listed method, the introspection engine:

1. Builds a JSON Schema from the method signature (``inspect.signature`` +
   ``get_type_hints``).
2. Extracts the description from the first paragraph of the docstring.
3. Enriches parameter descriptions from Sphinx ``:param:`` or Google
   ``Args:`` blocks.

.. _hook-toolset-templated-connection:

Templated connection IDs
------------------------

The hook's connection ID is a Jinja template, rendered for each task instance just
before it runs, so one toolset can reach a different system depending on the run:
``PostgresHook(postgres_conn_id="warehouse_{{ var.value.environment }}")`` switches
between staging and production, and a mapped task can give each map index its own
connection:

.. code-block:: python

    @task.agent(
        llm_conn_id="pydanticai_default",
        toolsets=[
            HookToolset(
                PostgresHook(postgres_conn_id="analytics_{{ task.op_kwargs.customer }}"),
                allowed_methods=["get_records"],
            )
        ],
    )
    def report(customer: str) -> str:
        return f"Summarize this month's orders for {customer}."


    report.expand(customer=customers())

Each task instance runs against a copy of the hook with its rendered connection
ID; the hook in the Dag file keeps the template. ``HookToolset`` reads the ID
from the attribute ``conn_name_attr`` names, or from ``conn_id`` for hooks such as
``WasbHook`` that keep it there; a hook that stores it anywhere else is not
templated. This works for hooks that read their connection when a method is
called, which is what Airflow hooks are expected to do: a hook that looks the
connection up in its constructor fails at Dag parse time, because the template
is not a connection ID yet. The same warning as for ``SQLToolset`` applies: build
the ID from values the Dag controls, not from ``params`` or ``dag_run.conf`` (see
:ref:`sql-toolset-templated-connection`).

Parameters
----------

- ``hook``: An instantiated Airflow Hook. Its connection ID is templated.
- ``allowed_methods``: Method names to expose as tools. Required. Methods
  are validated with ``hasattr`` + ``callable`` at instantiation time.
- ``tool_name_prefix``: Optional prefix prepended to each tool name
  (e.g. ``"s3_"`` produces ``"s3_list_keys"``).

When to choose it
-----------------

**Choose it when** the target already has an Airflow connection and a hook, and
what you want the agent to do is already a method on that hook. This is the
cheapest route (no new server, no new credential, no new query dialect) and
the only one that reaches any provider hook with synchronous methods without
anyone writing an adapter first. :class:`~airflow.providers.common.ai.toolsets.hook.HookToolset` is a
reflection-based adapter, so the work is choosing the method list.

**What it cannot do**

- It allow-lists method *names*, not arguments. Once ``read_key`` is exposed,
  the agent picks the key; the :ref:`defense-layer table <toolset-defense-layers>`
  states this outright. Choose methods whose worst case you accept, not methods
  you intend to constrain later.
- Its calls act as barriers. The tools are registered with ``sequential=True``
  because hook methods perform synchronous I/O, so a slow call holds up every
  other tool the model emitted in that step, not only this toolset's. This is
  not specific to ``HookToolset``; see :ref:`toolset-call-barriers`.
- It returns exactly one shape. Every result goes through ``serialize_for_llm``
  and comes back as a JSON-encoded string; there is no structured error type and
  no ``ModelRetry`` wrapper, so a hook exception fails the agent run, and the
  task with it, instead of giving the model something it can correct.
  ``SQLToolset``, by contrast, hands the database's own error back as a retry.
- Its ``call_tool`` calls the method and serializes what comes back. The code
  contains no path that awaits a coroutine result, and none that checks for one,
  so an ``async def`` hook method is not a case this adapter is written to
  handle. Treat synchronous methods as the supported set.

**A real example.** The read-only S3 pair from the ``HookToolset`` guidance in
:doc:`../agent_security`:

.. code-block:: python

    HookToolset(
        s3_hook,
        allowed_methods=["list_keys", "read_key"],
        tool_name_prefix="s3_",
    )

**Credentials and where it runs.** The hook instance is yours, so the credential
is whatever connection that hook resolves; the toolset never looks one up
itself. Calls run in the Airflow worker process.

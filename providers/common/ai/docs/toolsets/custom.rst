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

.. _howto/toolset:custom:

Writing your own toolset
========================

.. note::

    Experimental: this can change or be removed in a minor release of this provider.
    See :ref:`howto/stability`.

When an agent needs a system that no shipped toolset covers, write a toolset for it by
subclassing :class:`~airflow.providers.common.ai.utils.toolset_base.AirflowToolset`, the
class every toolset in this provider is built on. The subclass gets what the shipped
toolsets have: its results and errors pass through Airflow's secret masker even outside
``AgentOperator``, its tools work in other agent frameworks through ``airflow_tools()``,
and its blocking hook calls run off the event loop. ``AgentOperator`` masks the output of
every toolset it runs, a ``FunctionToolset`` included. If a method of an
existing hook already does what the agent needs, pass the hook to
:doc:`HookToolset <hook>` instead. If the tool needs no connection, a plain function in
pydantic-ai's ``FunctionToolset`` is less code.

This toolset looks packages up on PyPI through an HTTP connection. A project's response
from `PyPI's JSON API <https://docs.pypi.org/api/json/>`__ lists every release of the
project, so the toolset returns only the fields the model needs:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_custom_toolset.py
    :language: python
    :start-after: [START howto_toolset_custom_class]
    :end-before: [END howto_toolset_custom_class]

Pass it to an agent like any other toolset:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_custom_toolset.py
    :language: python
    :start-after: [START howto_toolset_custom_agent]
    :end-before: [END howto_toolset_custom_agent]

The example needs ``apache-airflow-providers-http``, a model connection named
``pydanticai_default``, and an HTTP connection named ``pypi`` whose host is
``https://pypi.org``, for example:

.. code-block:: bash

    export AIRFLOW_CONN_PYPI='{"conn_type": "http", "host": "https://pypi.org"}'

When the model calls ``get_release`` for version 0.10.0 of
``apache-airflow-providers-common-ai``, it receives the following, with
``requires_dist`` cut from 32 entries to three here:

.. code-block:: json

    {"version": "0.10.0", "requires_python": ">=3.10",
     "requires_dist": ["apache-airflow-providers-common-compat>=1.15.0",
                       "apache-airflow-providers-standard>=1.20.0",
                       "apache-airflow>=3.0.0"],
     "yanked": false, "uploaded": "2026-09-29T06:39:53.490917Z"}

What a subclass implements
--------------------------

``id``
    A name for the toolset. With ``durable=True``, each call is recorded under it, and a
    retry finds the calls an earlier attempt recorded by it. If the id changes between
    attempts, the retry stops matching the earlier attempt at the first call to this
    toolset, and that call and every step after it, model requests included, run again.
    Deriving it from the connection ID, as the example does, gives each connection's
    toolset its own.

``get_tools``
    The tools the model is offered, keyed by name. Each ``ToolDefinition`` carries the
    description and the JSON schema of the arguments, which are all the model knows about
    the tool. ``args_validator`` checks the model's arguments before ``execute_tool``
    runs, and arguments that fail it go back to the model to correct. The example's
    ``pattern`` constraints keep a name or version from changing the URL the hook calls.
    A pydantic ``TypeAdapter`` over a ``TypedDict`` gives the schema, and a pydantic-core
    ``SchemaValidator`` built from its ``core_schema`` is the validator.
    Import ``TypedDict`` from ``typing_extensions``. On Python before 3.12, building a
    ``TypeAdapter`` over a ``typing.TypedDict`` raises ``PydanticUserError: Please use
    `typing_extensions.TypedDict` instead of `typing.TypedDict` on Python < 3.12.``, and
    the Dag file fails to import.

``execute_tool``
    Runs one call. ``name`` says which tool, and ``tool_args`` holds the arguments
    ``args_validator`` returned. Return the result as the tool produced it: the base
    class's ``call_tool`` masks it on the way out. Implement ``execute_tool`` rather than
    overriding ``call_tool``, since an override skips that masking and ``AgentOperator``
    then wraps the toolset in a masking layer of its own. With ``durable=True`` a result
    has to be JSON-serializable, or the task fails with ``DurableJournalError`` and is
    not retried.

Two class attributes are optional:

``agent_template_fields``
    The attributes ``AgentOperator`` renders with Jinja for each task instance, so
    ``PyPIToolset(http_conn_id="pypi_{{ var.value.environment }}")`` picks a connection
    per environment. Rendering sets the attributes on a copy of the toolset and does not
    run ``__init__`` again, so derive nothing from them in ``__init__``: build hooks
    inside ``execute_tool``, as the example does. A hook built in ``__init__`` keeps the
    connection ID as written in the Dag file. Only ``AgentOperator`` and ``@task.agent``
    render these attributes.

``replayable``
    ``True`` by default: with ``durable=True``, a retry reuses the result an earlier
    attempt recorded instead of calling the system again. Set it to ``False`` when every
    attempt must make the call, as :doc:`ManagedAgentToolset <managed_agent>` does
    because a managed agent acts on systems Airflow cannot observe.

Errors the model sees and errors that fail the task
---------------------------------------------------

What ``execute_tool`` raises decides who deals with it:

- ``ModelRetry(message)`` sends the message to the model, which can call the tool
  again. Each one counts against the tool's ``max_retries``, which in the example is
  ``ctx.max_retries``, the agent's ``retries``. Once they are used up, the run fails with
  ``UnexpectedModelBehavior``, whose message starts ``Tool 'get_package' exceeded max
  retries count of 1`` under the default ``retries``. The
  example raises ``ModelRetry`` for a package that does not exist, and the model reads:

  .. code-block:: text

      PyPI has no 'apache-airflow-providers-commn-ai'. Check the name and version.

      Fix the errors and try again.

- ``ToolFailed(message)``, from ``pydantic_ai.exceptions``, tells the model the call
  failed for good, without counting against ``max_retries``. Use it when calling again
  with other arguments would not help, such as a record the connection is not allowed
  to read.
- Any other exception fails the agent run, so the task fails and its Airflow
  ``retries`` apply. The full exception, with its cause, is logged to the task log.
  What leaves the toolset usually keeps its type, but its message is masked and its
  cause is dropped.

Blocking calls
--------------

``execute_tool`` runs on the agent's event loop, and most hook methods block. Call them
through ``self.run_blocking(fn, *args, **kwargs)``, which runs the call in a worker
thread. Calls made through it run one at a time across the whole process, because
agent frameworks run tool calls concurrently and hooks are not thread-safe in general.
One slow call therefore holds up the blocking calls of every other toolset in the same
worker process.

Using the toolset in another agent framework
--------------------------------------------

The ``AirflowTools`` adapters for Strands, ADK, the Anthropic tool runner and the OpenAI
Agents SDK take the toolset as they take a shipped one, for example
``AirflowTools(PyPIToolset())`` (see :doc:`../frameworks/index`). They read its tools
through ``airflow_tools()``, which returns them as framework-neutral
:class:`~airflow.providers.common.ai.tools.AirflowTool` objects. Outside a pydantic-ai
run there is no real ``RunContext``: ``ctx.deps`` is ``None`` and ``ctx.model`` is a
stand-in, and only ``ctx.retry`` and ``ctx.max_retries`` are kept per tool, with
``max_retries`` set to ``1``. A toolset meant for other frameworks should read nothing
else from ``ctx``.

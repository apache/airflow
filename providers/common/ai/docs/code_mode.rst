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

.. _code-mode:

Code mode
=========

Set ``code_mode=True`` to collapse the agent's tools into a single ``run_code``
tool powered by the `Monty <https://github.com/pydantic/monty>`__ sandbox (via
pydantic-ai-harness). Instead of one model round-trip per tool call, the model
writes a single Python snippet that calls the tools as functions -- with loops,
conditionals, and ``asyncio.gather`` -- in one turn. For multi-tool workflows
this cuts round-trips and token use.

The generated code runs in Monty's deny-by-default sandbox: it cannot read the
filesystem, the network, or environment variables. It can only call the tools
you registered. Code mode therefore does not widen what the agent can reach --
the tools it calls still run in the worker -- it only changes how the model
invokes them. See :doc:`agent_security` for the tool
boundary.

When to use it
--------------

Code mode pays off for **orchestration-heavy, computation-light** workflows:
calling several tools, looping over their results, filtering, and combining them.
Collapsing many sequential tool calls into one turn is where the round-trip and
token savings come from -- the example below answers a per-customer question in a
single ``run_code`` block instead of one model round-trip per customer.

It is **not a general-purpose code runtime**. The generated code is only the glue
between tool calls; every real capability must come from a tool. Monty runs a
subset of Python and **cannot import third-party libraries** (pandas, numpy,
requests, boto3, ...) and has no filesystem or network access. If a task needs to
crunch data inline with a library, you have two options, both better than code
mode:

- **Push the work into a tool.** Do the aggregation in SQL (``SQLToolset``), or
  expose a hook method that returns the processed result (``HookToolset``). The
  tool runs in the full worker environment with all its dependencies, and code
  mode just orchestrates it.
- **Give the agent a real environment**, with
  :class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset`. It hands
  the model a shell and a filesystem in a disposable sandbox off the worker, so
  third-party packages, a real interpreter and installed binaries are all
  available. It costs a sandbox per run and a second or so to provision, against
  well under a millisecond for Monty, so reach for it when inline library code is
  genuinely required rather than by default. See :doc:`sandbox/index` for the
  backends and their limitations.

The two are not exclusive: ``code_mode=True`` and a ``SandboxToolset`` can be
enabled together, and the file tools fold into ``run_code`` while ``run_command``
stays a tool of its own.

Requires the ``code-mode`` extra::

    pip install "apache-airflow-providers-common-ai[code-mode]"

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent.py
    :language: python
    :start-after: [START howto_operator_agent_code_mode]
    :end-before: [END howto_operator_agent_code_mode]

Unlike passing a capability through ``agent_params`` (see
:ref:`capabilities-passthrough`), ``code_mode`` is a plain boolean and is
serialization-safe: the ``CodeMode`` capability is built at execution time, not
stored on the serialized operator.

.. note::

    Monty is pre-1.0. The ``code-mode`` extra is opt-in so its dependency churn
    never affects the base provider install.

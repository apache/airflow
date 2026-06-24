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

Run a command in an ephemeral sandbox
=====================================

:class:`~airflow.providers.sandbox.operators.sandbox.SandboxOperator` runs a
single command inside an ephemeral cloud sandbox (``local`` / Daytona / E2B /
Modal / islo) and returns its stdout. Unlike the
:doc:`SandboxExecutor <../sandbox-executor>`, it needs no special executor — it
runs from inside a normal task, so you can adopt sandboxes one task at a time.

The backend is selected with the ``provider`` argument (or the ``[sandbox]
provider`` config). Credentials passed via ``env`` are injected into the sandbox
only — never onto the worker — which is the recommended way to run untrusted or
LLM-generated code.

Using the operator
------------------

.. exampleinclude:: /../tests/system/sandbox/example_sandbox.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sandbox]
    :end-before: [END howto_operator_sandbox]

TaskFlow form
-------------

``@task.sandbox`` mirrors ``@task.bash``: the decorated callable returns the
command string to run inside the sandbox.

.. code-block:: python

    from airflow.decorators import task


    @task.sandbox(provider="daytona", env={"ANTHROPIC_API_KEY": "{{ var.value.anthropic_api_key }}"})
    def run_agent() -> str:
        return "python /opt/agent.py"

Reference
---------

* Operator: :class:`~airflow.providers.sandbox.operators.sandbox.SandboxOperator`
* Backends: ``local``, ``daytona``, ``e2b``, ``modal``, ``islo`` (or a custom
  ``module:Class`` implementing
  :class:`~airflow.providers.sandbox.backends.base.SandboxProvider`)

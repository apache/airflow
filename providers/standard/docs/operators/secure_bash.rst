.. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-8.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

.. _howto/operator:SecureBashOperator:

SecureBashOperator
==================

The ``SecureBashOperator`` is a drop-in replacement for the standard :class:`~airflow.providers.standard.operators.bash.BashOperator` that protects against **command injection**.

It uses a technique called **Template Lifting** (the shell equivalent of SQL parameterized queries) to structurally separate untrusted data from shell code.

When to use SecureBashOperator
------------------------------

You should use ``SecureBashOperator`` whenever you are passing untrusted user input directly into your inline Bash commands. This includes inputs from:

- DAG Run configurations (``dag_run.conf``)
- DAG Params (``params``)
- Airflow Variables (``var.value`` / ``var.json``)
- Connections (``conn``)

How it works
------------

If a user triggers a DAG with a malicious payload: ``{"user_input": "; rm -rf / #"}``

**Vulnerable BashOperator:**
The template is rendered *before* being passed to the shell. The shell sees and executes the injection.

.. code-block:: python

    BashOperator(
        task_id="unsafe",
        bash_command='echo "Hello {{ dag_run.conf["user_input"] }}"',
    )
    # Renders to: echo "Hello "; rm -rf / #"
    # Result: The shell deletes your filesystem.

**SecureBashOperator:**
Untrusted variables are **lifted** out of the bash command and placed into environment variables. The bash command is rewritten to reference the environment variable instead.

.. code-block:: python

    SecureBashOperator(
        task_id="safe",
        bash_command='echo "Hello {{ dag_run.conf["user_input"] }}"',
    )
    # Under the hood, this becomes:
    # command: echo "Hello ${_AIRFLOW_LIFTED_safe_0}"
    # env:     {'_AIRFLOW_LIFTED_safe_0': '; rm -rf / #'}

Because of how the POSIX execution model works, the shell will treat the environment variable strictly as **literal data**, completely neutralizing the command injection attempt.

Limitations
-----------

- **Variable Aliasing:** If you deliberately alias an untrusted variable in Jinja using ``{% set x = params.foo %}``, the lifter cannot track it. You must pass untrusted variables directly.
- **Eval/Source:** If your command uses ``eval`` or ``source``, it will explicitly re-evaluate the data as code. The lifter cannot protect against this. A warning will be logged.
- **Single Quotes:** Bash does not expand variables inside single quotes. If your template has ``'{{ params.x }}'``, the lifter will output ``'${VAR}'``, which bash will print literally. Use double quotes or leave the variable unquoted.

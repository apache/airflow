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

.. _howto/modal-usage:

Using Modal from Airflow
========================

This provider ships a hook, not operators. A task gets a
:class:`~airflow.providers.modal.hooks.modal.ModalHook`, asks it for a handle to a deployed
function, class or sandbox, and then uses the Modal SDK on that handle exactly as it would
outside Airflow. What the hook adds is that every handle carries the connection's credentials
and environment.

Invoking a deployed function
----------------------------

Deploy the function once, outside Airflow, with the Modal CLI:

.. code-block:: python

    # training.py
    import modal

    app = modal.App("training")


    @app.function(gpu="A10G", timeout=3600)
    def train(dataset: str, epochs: int) -> dict:
        ...
        return {"loss": 0.12, "checkpoint": "s3://.../ckpt-42"}

.. code-block:: bash

    modal deploy training.py

Then call it from a task. ``.remote()`` blocks until the function returns and hands back its
return value, which the task returns to XCom:

.. code-block:: python

    from airflow.providers.modal.hooks.modal import ModalHook
    from airflow.sdk import dag, task


    @dag(schedule=None)
    def train_model():
        @task
        def train(dataset: str) -> dict:
            hook = ModalHook(modal_conn_id="modal_default")
            train_fn = hook.get_function("training", "train")
            return train_fn.remote(dataset, epochs=3)

        train("s3://bucket/dataset.parquet")


    train_model()

``get_cls`` works the same way for ``@app.cls`` classes: ``hook.get_cls("training", "Trainer")()``
returns an instance whose methods have ``.remote()``.

Running a command in a sandbox
------------------------------

A sandbox is a fresh container that runs one command and exits. The hook resolves the app the
sandbox belongs to (creating it on first use if you ask), and forwards every other keyword to
``modal.Sandbox.create``.

``Sandbox.wait()`` only raises when the sandbox is terminated from outside; an ordinary nonzero
exit returns normally. Check ``returncode`` yourself, or the task succeeds while the command
failed. Terminate in ``finally`` so a task killed mid-run does not leave a sandbox billing.

.. code-block:: python

    import modal

    from airflow.providers.modal.hooks.modal import ModalHook
    from airflow.sdk import dag, task


    @dag(schedule=None)
    def sandbox_example():
        @task
        def run_script() -> str:
            hook = ModalHook()
            sandbox = hook.create_sandbox(
                "python",
                "-c",
                "import sys; print('hello from Modal'); sys.exit(3)",
                app_name="airflow-sandboxes",
                create_app_if_missing=True,
                image=modal.Image.debian_slim(python_version="3.12"),
                timeout=600,
            )
            try:
                sandbox.wait()
                stdout = sandbox.stdout.read()
                stderr = sandbox.stderr.read()
                if sandbox.returncode != 0:
                    raise RuntimeError(f"sandbox exited with {sandbox.returncode}: {stderr}")
                return stdout
            finally:
                sandbox.terminate()

        run_script()


    sandbox_example()

Environments
------------

Modal `environments <https://modal.com/docs/guide/environments>`__ separate deployments with
the same name, for example a ``training`` app in ``main`` and another in ``dev``. The
connection's ``environment`` extra picks one, and every hook method applies it:
``lookup_app``, ``get_function``, ``get_cls``, ``get_secret``, ``get_volume``,
``create_sandbox`` (through the app it resolves). When the extra is empty, the Modal SDK uses
``MODAL_ENVIRONMENT`` or the active profile's environment, and otherwise the workspace default.

Raw SDK calls made with ``hook.client_kwargs`` carry the credentials only. Pass
``environment_name=hook.environment_name`` yourself on any SDK call that accepts it, or the
lookup lands in the SDK's default environment under the connection's credentials.

Execution semantics
-------------------

* ``.remote()`` and ``Sandbox.wait()`` block the Airflow worker slot for as long as the remote
  work runs. Set the task's ``execution_timeout`` at or above the Modal function's ``timeout``
  so the two do not disagree about who gives up first.
* ``.spawn()`` returns a ``FunctionCall`` immediately. A task that spawns and returns has not
  waited for success; store ``call.object_id`` and poll ``modal.FunctionCall.from_id(...)``
  in a later task (pass ``**hook.client_kwargs``) if you need the result.
* An Airflow retry runs the task function again, which submits new remote work. Make the remote
  side idempotent, or key it on ``run_id`` / ``try_number`` from the task context.

What this provider does not do yet
----------------------------------

* No operators, sensors or deferrable triggers; blocking calls in a ``@task`` are the pattern.
* No executor. Tasks run on Airflow workers and call out to Modal; they do not run inside Modal.
* Token authentication only. Modal's OAuth client credentials are not modeled on the
  connection.
* *Test connection* on the connection form checks that the token authenticates against the
  Modal API. It does not check that the ``environment`` exists or that any app is deployed.

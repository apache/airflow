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

.. _howto/decorator:modal:

Running a Python function on Modal
==================================

``@task.modal`` runs the body of a Python function in a Modal
`sandbox <https://modal.com/docs/guide/sandboxes>`__, in an image you define, while the task
itself stays on an Airflow worker. Use it for code whose dependencies cannot be installed next
to Airflow, such as ``pydantic<2`` or a specific vLLM build, or that needs a GPU the workers do
not have. The image does not need Airflow or the Modal client, only ``python``.

If the code is already deployed as a Modal function, call it from a plain ``@task`` with
``ModalHook.get_function`` instead; see :ref:`howto/modal-usage`.

Running a function
------------------

Decorate the function and give it an image. Everything the function imports is installed in
the image, not on the worker:

.. exampleinclude:: /../tests/system/modal/example_modal_task.py
    :language: python
    :dedent: 4
    :start-after: [START howto_decorator_modal]
    :end-before: [END howto_decorator_modal]

The task starts a sandbox under the Modal app ``airflow`` (``app_name`` changes it; the app is
created on first use), sends it the function's source and arguments, and logs the function's
output as it runs:

.. code-block:: text

    Started Modal sandbox sb-01M6MBWQQ3DSHZMVF1W677BDAT in app 'airflow'.
    Validated 2 records with pydantic 1.10.27

The return value goes to XCom like any task's, here with the scores coerced to floats by
pydantic 1:

.. code-block:: python

    {
        "pydantic_version": "1.10.27",
        "records": [
            {"id": 1, "email": "ada@example.com", "score": 0.91},
            {"id": 2, "email": "grace@example.com", "score": 0.47},
        ],
    }

Each sandbox carries tags with the task instance's Dag id, task id, run id, map index and try
number, so you can find it in the Modal dashboard from the task, and the other way round.

Writing the function
--------------------

The function's source is sent to the sandbox on its own, as with ``@task.docker`` and
``@task.kubernetes``:

* Import what the function needs inside its body. Module-level imports, globals and helper
  functions in the Dag file do not exist in the sandbox.
* Take every input as an argument. There is no task context, connection, variable or hook
  inside the function.
* Arguments and the return value are pickled. They must load on the other side, so pass
  plain Python values, not objects from packages that only the worker or only the image has.
  The image needs Python 3.8 or later, and its minor version does not have to match the
  worker's. Dates that come from another task's XCom arrive as ``pendulum`` objects, which
  need ``pendulum`` in the image; pass ``.isoformat()`` strings instead to avoid that.
* Keep the return value small. It comes back through the sandbox's output, and Modal keeps a
  limited amount of that; the task fails with a clear error when the value was cut short.
  Write large results to a volume and return their path.
* Pass credentials as Modal secrets, not as arguments: arguments are rendered in the Airflow UI.

Choosing resources
------------------

``image``, ``gpu``, ``cpu``, ``memory``, ``timeout``, ``secrets`` and ``volumes`` go to
`modal.Sandbox.create <https://modal.com/docs/reference/modal.Sandbox>`__, and anything left
unset gets Modal's default; :func:`~airflow.providers.modal.decorators.modal.modal_task`
lists every argument. Before a long or memory-heavy run:

* Set ``timeout`` for anything that runs longer than five minutes. Modal stops a sandbox after
  300 seconds by default. When only the task's ``execution_timeout`` is set, the sandbox gets
  that instead; when both are set, the shorter wins. Either way a sandbox stops no later than
  ``execution_timeout`` after it starts.
* A single ``cpu`` or ``memory`` number is a reservation, which the sandbox can exceed. To cap
  it, pass a ``(reservation, limit)`` pair; a function that goes over a memory limit is
  stopped.
* ``secrets`` and ``volumes`` take names as well as Modal objects. Names are looked up when the
  task runs, through the connection, so they resolve in its ``environment`` and parsing the
  Dag file makes no Modal call. Each secret's keys become environment variables in the
  sandbox.

The default image is ``modal.Image.debian_slim()`` built for the worker's Python version.

A GPU job that reads a Hugging Face token from a secret, named ``huggingface`` and holding
``HF_TOKEN``, and caches the weights on a volume. It needs an H100, so it is a sketch rather
than an example the docs build runs:

.. code-block:: python

    @task.modal(
        image=modal.Image.debian_slim().pip_install("vllm"),
        gpu="H100",
        timeout=3600,
        secrets=["huggingface"],
        volumes={"/weights": "model-weights"},
    )
    def batch_infer(prompts: list[str]) -> list[str]:
        from vllm import LLM

        llm = LLM("meta-llama/Llama-3.1-8B-Instruct", download_dir="/weights")
        return [output.outputs[0].text for output in llm.generate(prompts)]

Not holding a worker slot
-------------------------

With ``deferrable=True`` the task starts the sandbox and then defers, so the worker slot is
free while the function runs. The triggerer follows the sandbox's output into the task log,
and when the sandbox exits the task resumes on a worker to read the return value:

.. exampleinclude:: /../tests/system/modal/example_modal_task.py
    :language: python
    :dedent: 4
    :start-after: [START howto_decorator_modal_deferrable]
    :end-before: [END howto_decorator_modal_deferrable]

On a T4 the task pushes ``"Tesla T4, 15360 MiB"`` to XCom.

The triggerer talks to Modal through the same ``modal_conn_id``. If you rely on ambient
credentials (``MODAL_TOKEN_ID`` and ``MODAL_TOKEN_SECRET``, or ``~/.modal.toml``) rather than
a connection, the triggerer needs them as well as the workers.

If the triggerer restarts while it waits, the trigger that takes over reads the output again
from the oldest part Modal still holds, so the task log can repeat lines logged before the
restart. ``[operators] default_deferrable`` sets the default for ``deferrable``.

When the function fails
-----------------------

The task fails with ``ModalSandboxError``, whose ``returncode`` and ``sandbox_id`` say what
happened and where:

* The function raised. Its traceback is in the task log, and the error reads
  ``The function failed in Modal sandbox sb-... with exit code 1. Its traceback is in the log
  above.``
* The sandbox ran past ``timeout`` (exit code 124). Modal interrupts the function, so the task
  log shows a ``KeyboardInterrupt`` traceback from wherever it was, followed by ``Modal sandbox
  sb-... reached its timeout (10s) before the function returned. Raise the decorator's
  timeout, or the task's execution_timeout if that is shorter.``
* The sandbox was stopped from outside (exit code 137), for example by going over a ``memory``
  limit: ``Modal sandbox sb-... was terminated before the function returned, for example
  because it ran out of memory under a memory limit, or from the Modal dashboard.``

If the task loses the function's output stream, it logs a warning and keeps waiting for the
sandbox to exit. If it loses track of the sandbox itself, it tries to terminate the sandbox
before failing, so a retry does not run beside the earlier attempt.

A retry starts a new sandbox and runs the function from the beginning. Marking a running task
failed or success, or clearing it, terminates its sandbox. A worker that dies without running
the task's ``on_kill`` leaves the sandbox running until it exits or hits its ``timeout``.

A deferred task's sandbox is terminated on those actions from Airflow 3.3, which added
``on_kill`` for triggers. On earlier versions it runs until it exits or hits its ``timeout``.

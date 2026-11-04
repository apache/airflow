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
output as it runs. The return value goes to XCom like any task's:

.. code-block:: text

    TASK_LOG_PLACEHOLDER

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
* Arguments and the return value are pickled. They must unpickle on the other side, so pass
  plain Python values, not objects from packages that only the worker or only the image has.
  The image's Python version does not have to match the worker's.
* Pass credentials as Modal secrets, not as arguments: arguments are rendered in the Airflow UI.

Choosing resources
------------------

The decorator passes these arguments to ``modal.Sandbox.create``. Anything left unset gets
Modal's default:

``image``
    A ``modal.Image``. Defaults to ``modal.Image.debian_slim()``, which matches the worker's
    Python version.

``gpu``
    For example ``"H100"`` or ``"A10G:2"``.

``cpu`` and ``memory``
    Cores and MiB. A single number is a reservation the sandbox can exceed when the host has
    room; a ``(reservation, limit)`` pair also caps it.

``timeout``
    Seconds before Modal stops the sandbox. Modal's default is 300, so set it for anything
    longer than five minutes.

``secrets`` and ``volumes``
    Secret names, or a list of ``modal.Secret`` objects, and a mapping from mount path to
    volume name or ``modal.Volume``. Names are looked up when the task runs, through the
    connection, so they resolve in its ``environment`` and parsing the Dag file makes no Modal
    call. Each secret's keys become environment variables in the sandbox.

``modal_conn_id``
    The :ref:`Modal connection <howto/connection:modal>`; ``modal_default`` when unset.

A GPU job that reads a Hugging Face token from a secret and caches weights on a volume:

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

If the triggerer restarts while it waits, the triggerer that takes over reads the output again
from the start, so the task log repeats the lines logged before the restart.
``[operators] default_deferrable`` sets the default for every task.

When the function fails
-----------------------

The task fails with ``ModalSandboxError``, whose ``returncode`` and ``sandbox_id`` say what
happened and where:

* The function raised. Its traceback is in the task log, and the error reads
  ``The function failed in Modal sandbox sb-... with exit code 1. Its traceback is in the log
  above.``
* The sandbox ran past ``timeout`` (exit code 124): ``Modal sandbox sb-... reached its timeout
  before the function returned. Raise the decorator's timeout (Modal's default is 300
  seconds).``
* Something else stopped the sandbox (exit code 137), such as terminating it from the Modal
  dashboard.

If the task fails for any other reason after starting the sandbox, such as losing the
connection to Modal while following its output, it terminates the sandbox before failing, so
a retry does not run beside the earlier attempt.

A retry starts a new sandbox and runs the function from the beginning. Marking a running task
failed or success terminates its sandbox. For a deferred task this needs Airflow 3.3 or later;
on older versions the sandbox runs until it exits or hits its ``timeout``. A worker that dies
without running the task's ``on_kill`` also leaves the sandbox running until then.

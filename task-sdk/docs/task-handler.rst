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


Task handlers
=============

A task may be declared in one place and have its body implemented in another.
The *stub task* interface is implemented by the Python (``@task.stub``) and YAML
(``run:``) Dag formats. A stub has its body supplied by a *task handler*.
Handlers can be written in any language with a Task SDK that implements the
task handler interface.

Writing a handler
-----------------

Decorate a plain function with the ``(dag_id, task_id)`` it implements:

.. code-block:: python

    from airflow.sdk import task_handler


    @task_handler(dag_id="events_pipeline", task_id="transform")
    def transform(region: str, events: list[dict]) -> dict:
        return {"region": region, "count": len(events)}

* The function's parameters are the task's arguments. They are bound from the
  stub's captured arguments by name, and may accept either a literal value, or
  a reference to a return value XCom.
* The return value becomes the task's return value XCom, so downstream tasks
  consume it like any other task's output.
* ``task_id`` defaults to the function name, so the above could be written
  equivalently as simply ``@task_handler(dag_id="events_pipeline")``.

Deploying handlers
------------------

A handler must be importable in two places: the Dag processor (to validate a
stub's arguments against the handler's signature at parse time) and the worker
(to run it). The handler modules should be made available by placing them in a
:doc:`Dag bundle <apache-airflow:administration-and-deployment/dag-bundles>` and
pointing a :class:`~airflow.sdk.coordinators.python.PythonCoordinator` at it.
The coordinator should be configured in ``[sdk] coordinators`` with an
appropriate ``task_handler_bundle_name``, and have the task's queue routed to it
with ``[sdk] queue_to_coordinator``:

.. code-block:: ini

    [dag_processor]
    # A Dag bundle holding the @task_handler modules, registered like any other bundle.
    dag_bundle_config_list = [
        {"name": "python-handlers", "classpath": "...", "kwargs": {...}},
    ]

    [sdk]
    coordinators = {
        "python": {
            "classpath": "airflow.sdk.coordinators.python.PythonCoordinator",
            "kwargs": {"task_handler_bundle_name": "python-handlers"}
        }
    }
    queue_to_coordinator = {"python-workers": "python"}

With this, a stub task whose ``queue`` is ``python-workers`` is routed to the
``PythonCoordinator``, which imports ``python-handlers`` and runs the registered
handler. ``task_handler_bundle_name`` is validated against
``dag_bundle_config_list`` when configuration loads, so a typo fails fast.

It is considered best practice to give handler tasks dedicated queues for
management purposes. However, you can have a queue that's shared by regular
tasks and stub task handlers, and it would work just fine as long as the
coordinator routed to the queue is capable of executing them.

Notes
-----

* A handler bundle is imported once per worker process.
* A stub's arguments must satisfy cross-language requirements. An argument must
  be either JSON-compatible, or reference an upstream task's return value XCom.
* When a handler cannot be resolved at parse time (no bundle configured yet),
  argument validation is deferred; a genuinely missing handler is reported when
  the task runs.

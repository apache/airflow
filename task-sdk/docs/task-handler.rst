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
* Arguments are bound to parameters by name, so ``*args``, ``**kwargs``, and
  positional-only parameters are not allowed. No parameter may be named after a
  task-context key (``ti``, ``ds``, etc.).

Deploying handlers
------------------

A stub task's handler must be available in two places: the Dag processor, which
validates the stub's arguments against the handler's signature when the Dag is
parsed, and the worker that runs the task.

In both cases, the handler is loaded from a Dag bundle, so the most important
thing to decide is which bundle should hold it.

In the stub's own bundle
~~~~~~~~~~~~~~~~~~~~~~~~

A handler is discovered automatically in the stub task's own
:doc:`Dag bundle <apache-airflow:administration-and-deployment/dag-bundles>`.
If you place the handler in the same bundle as the Dag that uses it, it is
imported with no configuration at all. This is the recommended layout.

Keep the bundle lean. Everything in a handler's bundle is loaded on every parse
and every task run to discover its handlers. Airflow cannot tell which
files are handlers, so it loads them all. A bundle stuffed with unrelated
definitions (Python Dag files, for example) can therefore be slow to load. The
leaner the bundle, the faster it loads; prefer a focused bundle of stub Dags
together with the handlers that implement them.

In a separate bundle
~~~~~~~~~~~~~~~~~~~~

Alternatively, the stub's coordinator can be pointed at the handler's bundle
with the ``task_handler_bundle_name`` keyword argument. Register the handler
bundle in ``[dag_processor] dag_bundle_config_list`` like any other bundle, set
``task_handler_bundle_name`` on the coordinator in ``[sdk] coordinators``, and
route the stub's ``queue`` to that coordinator in ``[sdk] queue_to_coordinator``
like this:

.. code-block:: ini

    [dag_processor]
    # The bundle holding the handlers, registered like any other bundle.
    dag_bundle_config_list = [
        {"name": "shared-handlers", "classpath": "...", "kwargs": {...}},
    ]

    [sdk]
    coordinators = {
        "handlers": {
            "classpath": "airflow.sdk.coordinators.python.PythonCoordinator",
            "kwargs": {"task_handler_bundle_name": "shared-handlers"}
        }
    }
    queue_to_coordinator = {"workers": "handlers"}

A stub whose ``queue`` is ``workers`` is then routed to that coordinator, which
loads ``shared-handlers`` instead of the stub's own bundle.
``task_handler_bundle_name`` is validated against ``dag_bundle_config_list``
when configuration loads, so a typo fails fast.

It is considered best practice to give handler tasks dedicated queues for
management purposes. However, you can have a queue that's shared by regular
tasks and stub task handlers, and it would work just fine as long as the
coordinator routed to the queue is capable of executing them.

Notes
-----

* For the Python SDK, a handler bundle is imported fresh in each Dag parse and
  task run; every non-ignored ``.py`` file in it is imported to discover
  handlers (``.airflowignore`` is honored).
* A stub's arguments must satisfy cross-language requirements. An argument must
  be either JSON-compatible, or reference an upstream task's return value XCom.
* A stub's arguments are validated against the handler's signature when the Dag
  is parsed. If a handler cannot be resolved at parse time, argument validation
  is deferred; a genuinely missing handler is reported when the task runs.

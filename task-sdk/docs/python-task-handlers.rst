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

Python task handlers
====================

The :func:`airflow.sdk.task_handler` decorator registers a Python function as
the implementation of a YAML Dag task's ``run:`` body. The handler is selected
by its Dag ID and task ID:

.. code-block:: python

    from airflow.sdk import task_handler


    @task_handler(dag_id="my_yaml_dag", task_id="transform")
    def transform(rows: list[dict], factor: int) -> int:
        return len(rows) * factor

The handler's parameters must match the names declared under ``run:`` in the
YAML Dag. Its return value follows normal Task SDK task semantics and is
available to downstream tasks as an XCom value.

Place the module containing the handler in the configured Python handler Dag
bundle. Route the YAML task's ``queue`` to a
``airflow.sdk.coordinators.python.PythonCoordinator`` in ``[sdk]``. Set the
coordinator's ``task_handler_bundle_name`` when handlers live in a different
bundle from the YAML Dag; otherwise, the task's own bundle is searched.

Handler lookup during Dag parsing is best effort. Missing handlers or argument
signature mismatches are reported when they can be determined; execution still
fails clearly if the matching handler cannot be resolved on the worker.

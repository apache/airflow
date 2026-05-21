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

Task State
==========

.. versionadded:: 3.2

Traditionally, Apache Airflow has always treated tasks as stateless, idempotent units of work.
However, a growing class of workloads requires that a Task be able to persist a certain amount of state information and retrieve it in a subsequent execution of that same Task.
This could be for a subsequent execution of the same task i.e. a retry of the same DAG run, or a future run of the same DAG.

"Task state" supports these operations. Task state provide a mechanism for some information to be persisted by a task and used later.
Below is an example of how task state can be used to persist and retrieve information.

.. code-block:: python
    from airflow.sdk import task
    import random

    # ... define a DAG

    @task
    def get_random_number(**context):
        # Retrieve the task_state
        task_state = context["task_state"]

        # Retrieve, generate, and set a new number
        my_last_number = task_state.get("my_number")
        my_new_number = random.randint(0, 10)
        task_state.set("my_number", my_new_number)


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

Cluster Policy
==============

In case you want to apply cluster-wide mutations to the Airflow tasks,
you can either mutate the task right after the DAG is loaded or
mutate the task instance before task execution.

Mutate tasks after DAG loaded
-----------------------------

To mutate the task right after the DAG is parsed, you can define
a ``policy`` function in ``airflow_local_settings.py`` that mutates the
task based on other task or DAG attributes (through ``task.dag``).
It receives a single argument as a reference to the task object and you can alter
its attributes.

For example, this function could apply a specific queue property when
using a specific operator, or enforce a task timeout policy, making sure
that no tasks run for more than 48 hours. Here's an example of what this
may look like inside your ``airflow_local_settings.py``:


.. code-block:: python

    def policy(task):
        if task.__class__.__name__ == 'HivePartitionSensor':
            task.queue = "sensor_queue"
        if task.timeout > timedelta(hours=48):
            task.timeout = timedelta(hours=48)


Please note, cluster policy will have precedence over task
attributes defined in DAG meaning if ``task.sla`` is defined
in dag and also mutated via cluster policy then later will have precedence.


Mutate task instances before task execution
-------------------------------------------

To mutate the task instance before the task execution, you can define a
``task_instance_mutation_hook`` function in ``airflow_local_settings.py``
that mutates the task instance.

For example, this function re-routes the task to execute in a different
queue during retries:

.. code-block:: python

    def task_instance_mutation_hook(ti):
        if ti.try_number >= 1:
            ti.queue = 'retry_queue'


Where to put ``airflow_local_settings.py``?
-------------------------------------------

Add a ``airflow_local_settings.py`` file to your ``$PYTHONPATH``
or to ``$AIRFLOW_HOME/config`` folder.

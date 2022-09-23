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

Testing DAGs with dag.test()
=============================

To debug DAGs in an IDE, you can set up the ``dag.test`` command in your dag file and run through your DAG in a single
serialized python process.

This approach can be used with any supported database (including a local SQLite database) and will
*fail fast* as all tasks run in a single process.

To set up ``dag.test``, add these two lines to the bottom of your dag file:

.. code-block:: python

  if __name__ == "__main__":
      dag.test()

and that's it! You can add argument such as ``execution_date`` if you want to test argument-specific dagruns, but otherwise
you can run or debug DAGs as needed.

Comparison with DebugExecutor
*****************************

The ``dag.test`` command has the following benefits over the :class:`~airflow.executors.debug_executor.DebugExecutor`
class, which is now deprecated:

1. It does not require running an executor at all. Tasks are run one at a time with no executor or scheduler logs.
2. It is significantly faster than running code with a DebugExecutor as it does not need to go through a scheduler loop.
3. It does not perform a backfill.


Debugging Airflow DAGs on the command line
==========================================

With the same two line addition as mentioned in the above section, you can now easily debug a DAG using pdb as well.
Run ``python -m pdb <path to dag file>.py`` for an interactive debugging experience on the command line.

.. code-block:: bash

  root@ef2c84ad4856:/opt/airflow# python -m pdb airflow/example_dags/example_bash_operator.py
  > /opt/airflow/airflow/example_dags/example_bash_operator.py(18)<module>()
  -> """Example DAG demonstrating the usage of the BashOperator."""
  (Pdb) b 45
  Breakpoint 1 at /opt/airflow/airflow/example_dags/example_bash_operator.py:45
  (Pdb) c
  > /opt/airflow/airflow/example_dags/example_bash_operator.py(45)<module>()
  -> bash_command='echo 1',
  (Pdb) run_this_last
  <Task(EmptyOperator): run_this_last>

.. _executor:DebugExecutor:

Debug Executor (deprecated)
===========================

The :class:`~airflow.executors.debug_executor.DebugExecutor` is meant as
a debug tool and can be used from IDE. It is a single process executor that
queues :class:`~airflow.models.taskinstance.TaskInstance` and executes them by running
``_run_raw_task`` method.

Due to its nature the executor can be used with SQLite database. When used
with sensors the executor will change sensor mode to ``reschedule`` to avoid
blocking the execution of DAG.

Additionally ``DebugExecutor`` can be used in a fail-fast mode that will make
all other running or scheduled tasks fail immediately. To enable this option set
``AIRFLOW__DEBUG__FAIL_FAST=True`` or adjust ``fail_fast`` option in your ``airflow.cfg``.
For more information on setting the configuration, see :doc:`../howto/set-config`.

**IDE setup steps:**

1. Add ``main`` block at the end of your DAG file to make it runnable.

It will run a backfill job:

.. code-block:: python

  if __name__ == "__main__":
      from airflow.utils.state import State

      dag.clear()
      dag.run()


2. Setup ``AIRFLOW__CORE__EXECUTOR=DebugExecutor`` in run configuration of your IDE. In
   this step you should also setup all environment variables required by your DAG.

3. Run / debug the DAG file.

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


.. _executor:DebugExecutor:

Debug Executor
==================

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

  if __name__ == '__main__':
    from airflow.utils.state import State
    dag.clear(dag_run_state=State.NONE)
    dag.run()


2. Setup ``AIRFLOW__CORE__EXECUTOR=DebugExecutor`` in run configuration of your IDE. In
   this step you should also setup all environment variables required by your DAG.

3. Run / debug the DAG file.

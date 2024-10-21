
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

DAG Testing
===========

To ease and speed up the process of developing DAGs, you can use
py:meth:`~airflow.models.dag.DAG.test`, which will run a dag in a single process.

To set up the IDE:

1. Add ``main`` block at the end of your DAG file to make it runnable.

.. code-block:: python

  if __name__ == "__main__":
      dag.test()


3. Run and debug the DAG file.


You can also run the dag in the same manner with the Airflow CLI command ``airflow dags test``:

.. code-block:: bash

    # airflow dags test [dag_id] [execution_date]
    airflow dags test example_branch_operator 2018-01-01

By default ``/files/dags`` folder is mounted from your local ``<AIRFLOW_SOURCES>/files/dags`` and this is
the directory used by airflow scheduler and webserver to scan dags for. You can place your dags there
to test them.

The DAGs can be run in the main version of Airflow but they also work
with older versions.

-----

For other kinds of tests look at `Testing document <../09_testing.rst>`__

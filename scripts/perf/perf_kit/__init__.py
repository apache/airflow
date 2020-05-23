# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
""""

Perf-kit
========

Useful decorators and context managers used when testing the performance of various Airflow components.

To use this package, you must add the parent directory to the :envvar:`PYTHONPATH` environment variable.
If the repository root directory is ``/opt/airflow/``, then you need to run the following command:

.. code-block:: bash

    export PYTHONPATH=/opt/airflow/scripts/perf

Content
=======
The following decorators and context managers are included.

.. autofunction:: perf_kit.memory.trace_memory

.. autofunction:: perf_kit.python.pyspy

.. autofunction:: perf_kit.python.profiled

.. autofunction:: perf_kit.repeat_and_time.timing

.. autofunction:: perf_kit.repeat_and_time.repeat

.. autofunction:: perf_kit.repeat_and_time.timeout

.. autofunction:: perf_kit.sqlalchemy.trace_queries

.. autofunction:: perf_kit.sqlalchemy.count_queries

Documentation for each function is provided in the function docstrings. Each module also has an example in
the main section of the module.

Examples
========

If you want to run an all example for ``perf_kit.sqlalchemy``, you can run the following command.

.. code-block:: bash

    python -m perf_kit.sqlalchemy

If you want to know how to use these functions, it is worth to familiarize yourself with these examples.

Use in tests
============

If you need it, you can easily test only one test using context manager.

Suppose we have the following fragment of the file with tests.

.. code-block:: python

        prev = dag.previous_schedule(_next)
        prev_local = local_tz.convert(prev)

        self.assertEqual(prev_local.isoformat(), "2018-03-24T03:00:00+01:00")
        self.assertEqual(prev.isoformat(), "2018-03-24T02:00:00+00:00")

    def test_bulk_sync_to_db(self):
        clear_db_dags()
        dags = [
            DAG(f'dag-bulk-sync-{i}', start_date=DEFAULT_DATE, tags=["test-dag"]) for i in range(0, 4)
        ]

        with assert_queries_count(3):
            DAG.bulk_sync_to_db(dags)

You can add a code snippet before the method definition, and then perform only one test and count the
queries in it.

.. code-block:: python
   :emphasize-lines: 6-8

        prev = dag.previous_schedule(_next)
        prev_local = local_tz.convert(prev)

        self.assertEqual(prev_local.isoformat(), "2018-03-24T03:00:00+01:00")
        self.assertEqual(prev.isoformat(), "2018-03-24T02:00:00+00:00")

    from perf_kit.sqlalchemy import trace_queries

    @trace_queries
    def test_bulk_sync_to_db(self):
        clear_db_dags()
        dags = [
            DAG(f'dag-bulk-sync-{i}', start_date=DEFAULT_DATE, tags=["test-dag"]) for i in range(0, 4)
        ]

        with assert_queries_count(3):
            DAG.bulk_sync_to_db(dags)

To run the test, execute the command

.. code-block:: bash

    pytest tests.models.dag -k test_bulk_sync_to_db -s

This is not a beautiful solution, but it allows you to easily check a random piece of code.

Having a separate file to save various tests cases can be helpful.
"""

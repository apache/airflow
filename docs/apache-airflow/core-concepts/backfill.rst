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

Backfill
========

Backfill is when you create runs for past dates of a dag.  Airflow provides a mechanism
to do this through the CLI and REST API.  You provide a dag, a start date, and an end date,
and airflow will create runs in the range according to the dag's schedule.

Backfill does not make sense for dags that don't have a time-based schedule.

Control over data reprocessing
------------------------------

There are three options for reprocessing behavior:

* **none** - if there's already a run for this logical date, do not create another, no matter the state
* **failed** - if a run exists, if the state is failed, create a new run for this date
* **completed** - if a run exists, if the state is completed or failed, create a new run for this date

If the latest run is still running or is queued, we do not create another run, no matter the chosen reprocessing behavior.

Concurrency control
-------------------

You can set ``max_active_runs`` on a backfill and it will control how many dag runs in
the backfill can run concurrently. Backfill ``max_active_runs`` is applied independently
the DAG ``max_active_runs`` setting.

Run ordering
------------

You can run your backfill in reverse, i.e. latest runs first.  The CLI option is ``--run-backwards``.

Dry run
-------

Backfill dry run is a CLI option that will print out the dates that the
backfill will consider creating runs for.  Whether or not they will be created
depends on your chosen reprocessing behavior and the states of any existing
runs in the range at the time you actually run the backfill.

Example:
--------

.. code-block:: bash

    airflow backfill create --dag tutorial \
        --start-date 2015-06-01 \
        --end-date 2015-06-07 \
        --reprocessing-behavior failed \
        --max-active-runs 3 \
        --run-backwards \
        --dag-run-conf '{"my": "param"}'

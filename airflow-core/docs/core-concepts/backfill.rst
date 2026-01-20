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

Backfill is when you create runs for past dates of a Dag.  Airflow provides a mechanism
to do this through the CLI and REST API.  You provide a Dag, a start date, and an end date,
and Airflow will create runs in the range according to the Dag's schedule.

Backfill does not make sense for Dags that don't have a time-based schedule.

Control over data reprocessing
------------------------------

There are three options for reprocessing behavior:

* **none** - if there's already a run for this logical date, do not create another, no matter the state
* **failed** - if a run exists, if the state is failed, create a new run for this date
* **completed** - if a run exists, if the state is completed or failed, create a new run for this date

If the latest run is still running or is queued, we do not create another run, no matter the chosen reprocessing behavior.

Concurrency control
-------------------

You can set ``max_active_runs`` on a backfill and it will control how many Dag runs in
the backfill can run concurrently. Backfill ``max_active_runs`` is applied independently
the Dag ``max_active_runs`` setting.

Run ordering
------------

You can run your backfill in reverse, i.e. latest runs first.  The CLI option is ``--run-backwards``.

Running backfills on paused DAGs
---------------------------------

By default, backfills require the DAG to be unpaused before they can execute. However, you may want to run backfills on historical data without activating the regular DAG schedule. This is common when:

* You need to reprocess old data but don't want daily scheduled runs
* The data source is not available for regular runs
* You want to avoid unnecessary computation and logs from scheduled triggers

Airflow allows you to run backfills while keeping the DAG paused. When enabled, only the backfill runs will execute—the DAG remains paused and will not be triggered by its regular schedule.

**Via UI**: Select the **"Run backfill while keeping DAG paused"** checkbox when creating a backfill for a paused DAG.

**Via API**: Set the ``keep_dag_paused`` parameter to ``true`` when creating a backfill.

This option is only available when the DAG is paused and is mutually exclusive with unpausing the DAG.

Dry run
-------

Backfill dry run is a CLI option that will print out the dates that the
backfill will consider creating runs for.  Whether or not they will be created
depends on your chosen reprocessing behavior and the states of any existing
runs in the range at the time you actually run the backfill.

Example
-------

Backfill can be created from either the CLI or the UI.

For CLI, below is an example command:

.. code-block:: bash

    airflow backfill create --dag-id tutorial \
        --start-date 2015-06-01 \
        --end-date 2015-06-07 \
        --reprocessing-behavior failed \
        --max-active-runs 3 \
        --run-backwards \
        --dag-run-conf '{"my": "param"}'

For UI, follow the following steps:

1. Navigate to a Dag's Details page and click **Trigger**.
2. In the pop-up window, select **Backfill**.
3. Fill in the form:

   - **Date range**: set "From" and "To" logical datetimes for the backfill window.
   - **Reprocess behavior**: choose one of ``Missing Runs``, ``Missing and Errored Runs``, or ``All Runs``.
   - **Max active runs**: limit concurrent backfill runs for this backfill.
   - **Run backwards**: execute most recent intervals first.
   - **Advanced Config**: optionally provide JSON ``dag_run.conf``.
   - If the Dag is paused, you have two options:
     
     - **Unpause on trigger**: Unpause the DAG so both backfill runs and regular scheduled runs will execute.
     - **Run backfill while keeping DAG paused**: Run only the backfill runs without unpausing the DAG. This is useful when you want to reprocess historical data without triggering regular scheduled runs (e.g., when daily data is not available).

.. image:: ../img/ui-light/backfill.png
   :alt: Backfill pop-up window (Light Mode)

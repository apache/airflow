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

Error Tracking
===============

Airflow can be set up to send errors to `Sentry <https://docs.sentry.io/>`__.

Setup
------

First you must install sentry requirement:

.. code-block:: bash

   pip install 'apache-airflow[sentry]'

Add your ``SENTRY_DSN`` to your configuration file e.g. ``airflow.cfg`` under ``[sentry]``. Its template resembles the following: ``'{PROTOCOL}://{PUBLIC_KEY}@{HOST}/{PROJECT_ID}'``

.. code-block:: ini

    [sentry]
    sentry_dsn = http://foo@sentry.io/123

.. note::
    If this value is not provided, the SDK will try to read it from the ``SENTRY_DSN`` environment variable.

Tags
-----

=================================== ==================================================
Name                                Description
=================================== ==================================================
dag_id                              Dag name of the dag that failed
task_id                             Task name of the task that failed
execution_date                      Execution date when the task failed
operator                            Operator name of the task that failed
=================================== ==================================================

Breadcrumbs
------------


When a task fails with an error `breadcrumbs <https://docs.sentry.io/enriching-error-data/breadcrumbs/?platform=python>`__ will be added for the other tasks in the current dag run.

=================================== ==============================================================
Name                                Description
=================================== ==============================================================
completed_tasks[task_id]             Task ID of task that executed before failed task
completed_tasks[state]               Final state of task that executed before failed task (only Success and Failed states are captured)
completed_tasks[operator]            Task operator of task that executed before failed task
completed_tasks[duration]            Duration in seconds of task that executed before failed task
=================================== ==============================================================

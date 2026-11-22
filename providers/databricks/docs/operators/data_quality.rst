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

.. _howto/operator:DatabricksDataQualityRefreshOperator:

Refresh data profiling metrics
==============================

Use :class:`~airflow.providers.databricks.operators.data_quality.DatabricksDataQualityRefreshOperator`
to refresh an existing data profiling monitor on a Unity Catalog table. This uses the
`Data Quality Monitoring refresh API <https://docs.databricks.com/api/data-quality/v1/create-refresh>`_,
which is in Public Preview and currently supports table monitors.

Pass the three-level table name as ``table_name``. The operator resolves its Unity Catalog ID
using the :ref:`Databricks connection <howto/connection:databricks>`. Both ``table_name`` and
``databricks_conn_id`` support templating. Use a connection to the workspace where the monitor
was created, with permission to read the table and refresh its monitor.

.. exampleinclude:: /../../databricks/tests/system/databricks/example_databricks_data_quality.py
    :language: python
    :start-after: [START howto_operator_databricks_data_quality_refresh]
    :end-before: [END howto_operator_databricks_data_quality_refresh]

By default, the task waits in the worker for the submitted refresh to succeed. Failed, canceled,
and unrecognized states fail the task. ``polling_period_seconds`` defaults to 30 seconds.
``timeout`` defaults to 3600 seconds and bounds the wait after submission, including status requests,
retries and sleeps. A timeout does not cancel the remote refresh.

Set ``wait_for_completion=False`` to return after submission. Both modes return the submitted
``refresh_id`` through XCom. The refresh processes data already available in the table; it does
not force pending inference logs to arrive.

HTTP requests are attempted once by default. Increasing ``databricks_retry_limit``, configuring
``databricks_retry_args``, retrying a task, or clearing it can submit another refresh after an
ambiguous submission failure. The example sets ``retries=0`` to override Dag-level retry defaults.

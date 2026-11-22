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

Data Quality Monitoring system test
===================================

The example refreshes an existing data profiling monitor. It does not create or delete the monitor,
its source table, metric tables or dashboard.

Before running it:

* Create a Unity Catalog Delta table with test data and a data profiling monitor in the target workspace.
  See `Create a data profile using the API <https://docs.databricks.com/aws/en/data-quality-monitoring/data-profiling/create-monitor-api>`_.
* Wait for the monitor's initial refresh to complete. Use an isolated monitor without a refresh schedule.
* Set ``DATA_QUALITY_TABLE_NAME`` to its three-level table name.
* Configure ``databricks_default`` for that workspace. The principal needs ``USE_CATALOG``,
  ``USE_SCHEMA``, ``SELECT`` and ``MANAGE`` on the table, or the equivalent catalog/schema privileges
  described in the refresh API.
* Ensure serverless data profiling is available in the workspace. Refreshes use billable serverless compute.

Run the example through ``breeze testing system-tests``, forwarding the connection and table-name
configuration. Verify the returned refresh ID independently with the get-refresh API and check that
the metric tables were updated.

After testing, delete any monitor created for the test and its generated metric tables and dashboard,
then delete the test source table and schema. Deleting a monitor does not delete its metric tables
or dashboard. Keep pre-existing resources and configuration intact.

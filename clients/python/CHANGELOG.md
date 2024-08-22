<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# v2.10.0

## Major changes:

   - Add dag_stats rest api endpoint ([#41017](https://github.com/apache/airflow/pull/41017))
   - AIP-64: Add task instance history list endpoint ([#40988](https://github.com/apache/airflow/pull/40988))
   - Change DAG Audit log tab to Event Log ([#40967](https://github.com/apache/airflow/pull/40967))
   - AIP-64: Add REST API endpoints for TI try level details ([#40441](https://github.com/apache/airflow/pull/40441))
   - Make XCom display as react json ([#40640](https://github.com/apache/airflow/pull/40640))
   - Replace usages of task context logger with the log table ([#40867](https://github.com/apache/airflow/pull/40867))
   - Fix tasks API endpoint when DAG doesn't have `start_date` ([#40878](https://github.com/apache/airflow/pull/40878))
   - Add try_number to log table ([#40739](https://github.com/apache/airflow/pull/40739))
   - Add executor field to the task instance API ([#40034](https://github.com/apache/airflow/pull/40034))
   - Add task documentation to details tab in grid view. ([#39899](https://github.com/apache/airflow/pull/39899))
   - Add max_consecutive_failed_dag_runs in API spec ([#39830](https://github.com/apache/airflow/pull/39830))
   - Add task failed dependencies to details page. ([#38449](https://github.com/apache/airflow/pull/38449))
   - Add dag re-parsing request endpoint ([#39138](https://github.com/apache/airflow/pull/39138))
   - Reorder OpenAPI Spec tags alphabetically ([#38717](https://github.com/apache/airflow/pull/38717))


# v2.9.1

## Major changes:

   - Add max_consecutive_failed_dag_runs in API spec ([#39830](https://github.com/apache/airflow/pull/39830))


# v2.9.0

## Major changes:

   - Allow users to write dag_id and task_id in their national characters, added display name for dag / task (v2) ([#38446](https://github.com/apache/airflow/pull/38446))
   - Add dataset_expression to grid dag details ([#38121](https://github.com/apache/airflow/pull/38121))
   - Adding run_id column to log table ([#37731](https://github.com/apache/airflow/pull/37731))
   - Show custom instance names for a mapped task in UI ([#36797](https://github.com/apache/airflow/pull/36797))
   - Add excluded/included events to get_event_logs api ([#37641](https://github.com/apache/airflow/pull/37641))
   - Filter Datasets by associated dag_ids (GET /datasets) ([#37512](https://github.com/apache/airflow/pull/37512))
   - Add data_interval_start and data_interval_end in dagrun create API endpoint ([#36630](https://github.com/apache/airflow/pull/36630))
   - Return the specified field when get dag/dagRun ([#36641](https://github.com/apache/airflow/pull/36641))

## NEW API supported

   - Add post endpoint for dataset events ([#37570](https://github.com/apache/airflow/pull/37570))
   - Add "queuedEvent" endpoint to get/delete DatasetDagRunQueue ([#37176](https://github.com/apache/airflow/pull/37176))


# v2.8.0

## Major changes:

  - Allow filtering event logs by attributes ([#34417](https://github.com/apache/airflow/pull/34417))
  - Add extra fields to plugins endpoint ([#34913](https://github.com/apache/airflow/pull/34913))
  - Let auth managers provide their own API endpoints ([#34349](https://github.com/apache/airflow/pull/34349))
  - Enable pools to consider deferred tasks ([#32709](https://github.com/apache/airflow/pull/32709))
  - Add dag_run_ids and task_ids filter for the batch task instance API endpoint ([#32705](https://github.com/apache/airflow/pull/32705))

## Major Fixes

  - Add DagModel attributes before dumping DagDetailSchema for get_dag_details API endpoint ([#34947](https://github.com/apache/airflow/pull/34947))
  - Add TriggerRule missing value in rest API ([#35194](https://github.com/apache/airflow/pull/35194))
  - Fix wrong plugin schema ([#34858](https://github.com/apache/airflow/pull/34858))
  - Make dry run optional for patch task instance ([#34568](https://github.com/apache/airflow/pull/34568))
  - OpenAPI Spec fix nullable alongside $ref ([#32887](https://github.com/apache/airflow/pull/32887))
  - Clarify new_state in OpenAPI spec ([#34056](https://github.com/apache/airflow/pull/34056))

## NEW API supported

  - NA

# v2.7.3

## Major changes:

 - NA

## Major Fixes

 - Add TriggerRule missing value in rest API ([#35194](https://github.com/apache/airflow/pull/35194))
 - Fix wrong plugin schema ([#34858](https://github.com/apache/airflow/pull/34858))

# v2.7.2

 Apache Airflow API version: 2.7.2

## Major changes:

 - NA

## Major Fixes

- Fix: make dry run optional for patch task instance  ([#34568](https://github.com/apache/airflow/pull/34568))

## NEW API supported

 - NA

# v2.7.0

Apache Airflow API version: 2.7.0

## Major changes:

 - Enable pools to consider deferred tasks ([#32709](https://github.com/apache/airflow/pull/32709))
 - add dag_run_ids and task_ids filter for the batch task instance API endpoint ([#32705](https://github.com/apache/airflow/pull/32705))
 - Add xcom map_index as a filter to xcom endpoint ([#32453](https://github.com/apache/airflow/pull/32453))
 - Updates health check endpoint to include dag_processor status. ([#32382](https://github.com/apache/airflow/pull/32382))
 - Add TriggererStatus to OpenAPI spec ([#31579](https://github.com/apache/airflow/pull/31579))

## Major Fixes

 - OpenAPI Spec fix nullable alongside $ref ([#32887](https://github.com/apache/airflow/pull/32887))
 - Fix incorrect default on readonly property in our API ([#32510](https://github.com/apache/airflow/pull/32510))
 - Fix broken links in openapi/v1.yaml ([#31619](https://github.com/apache/airflow/pull/31619))
 - Update Dag trigger API and command docs ([#32696](https://github.com/apache/airflow/pull/32696))

## NEW API supported

 - NA

# v2.6.2

Apache Airflow API version: 2.6.2

## Major changes:

 - Add TriggererStatus to OpenAPI spec ([#31579](https://github.com/apache/airflow/pull/31579))

## Major Fixes

 - Fixing broken links in openapi/v1.yaml ([#31619](https://github.com/apache/airflow/pull/31619))

## NEW API supported

 - NA

# v2.6.1

Apache Airflow API version: 2.6.1

## Major changes:

 - NA

## Major Fixes

 - Fix Pool schema OpenAPI spec ([#30973](https://github.com/apache/airflow/pull/30973))

## NEW API supported

 - NA

# v2.6.0

Apache Airflow API version: 2.6.0

## Major changes:

 - Minimum Python version is 3.7
 - DAGRun dag_id parameter is properly validated as read-only and setting it might result in an error:
   "`dag_id` is a read-only attribute" This might break some workflows that used examples from the documentation.

## Major Fixes

 - Move read only property in order to fix Dagrun API docs ([#30149](https://github.com/apache/airflow/pull/30149))
 - Fix clear dag run openapi spec responses by adding additional return type ([#29600](https://github.com/apache/airflow/pull/29600))
 - Fix Rest API update user output ([#29409](https://github.com/apache/airflow/pull/29409))
 - Add a param for get_dags endpoint to list only unpaused dags
([#28713](https://github.com/apache/airflow/pull/28713))
 - Expose updated_at filter for dag run and task instance endpoints ([#28636](https://github.com/apache/airflow/pull/28636))

## NEW API supported

 - NA

# v2.5.1

Apache Airflow API version: 2.5.1

## Major changes:

- NA

## Major fixes:

- Fix authentication issues by regenerating the client with proper security schemas

## New API supported:

- NA

# v2.5.0

Apache Airflow API version: 2.5.x

## Major changes:

- NA

## Major fixes:

- NA

## New API supported:

- GET /datasets/events | Get dataset events
- GET /datasets | Get datasets
- GET /datasets/{id} | Get a dataset
- POST /dags/{dag_id}/dagRuns/{dag_run_id}/clear | Clear a dagrun endpoint
- GET /dags/{dag_id}/dagRuns/{dag_run_id}/upstreamDatasetEvents | Get dataset events for a DAG run

# v2.3.0

Apache Airflow API version: 2.3.x

## Major changes:

- NA

## Major fixes:

- NA

## New API supported:

- PATCH /dags | Update DAGs
- GET /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index} | Get a mapped task instance
- GET /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/listMapped | List mapped task instances

# v2.2.0

Apache Airflow API version: 2.2.x

## Major changes:

- Client code is generated using OpenApi's 5.3.0 generator CLI

## Major fixes:

- NA

## New API supported:

- POST /connections/test | Test a connection
- DELETE /dags/{dag_id} | Delete a DAG
- PATCH /dags/{dag_id}/dagRuns/{dag_run_id} | Modify a DAG run
- DELETE /users/{username} | Delete a user
- PATCH /users/{username} | Update a user
- POST /users | Create a user

# v2.1.0

Apache Airflow API version: 2.1.x

## Major changes:

 - Client code is generated using OpenApi's 5.1.1 generator CLI

## Major fixes:

 - Fixed the iteration issue on array items caused by unsupported class 'object'.
   Issue [#15](https://github.com/apache/airflow-client-python/issues/15)

## New API supported:

 - Permissions
 - Plugins
 - Providers
 - Roles
 - Users

# v2.0.0

Apache Airflow API version: 2.0.x

Initial version of the Python client.

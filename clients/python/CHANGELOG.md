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

# v3.1.0

## New Features:

- Add `map_index` filter to TaskInstance API queries ([#55614](https://github.com/apache/airflow/pull/55614))
- Add `has_import_errors` filter to Core API GET /dags endpoint ([#54563](https://github.com/apache/airflow/pull/54563))
- Add `dag_version` filter to get_dag_runs endpoint ([#54882](https://github.com/apache/airflow/pull/54882))
- Implement pattern search for event log endpoint ([#55114](https://github.com/apache/airflow/pull/55114))
- Add asset-based filtering support to DAG API endpoint ([#54263](https://github.com/apache/airflow/pull/54263))
- Add Greater Than and Less Than range filters to DagRuns and Task Instance list ([#54302](https://github.com/apache/airflow/pull/54302))
- Add `try_number` as filter to task instances ([#54695](https://github.com/apache/airflow/pull/54695))
- Add filters to Browse XComs endpoint ([#54049](https://github.com/apache/airflow/pull/54049))
- Add Filtering by DAG Bundle Name and Version to API routes ([#54004](https://github.com/apache/airflow/pull/54004))
- Add search filter for DAG runs by triggering user name ([#53652](https://github.com/apache/airflow/pull/53652))
- Enable multi sorting (AIP-84) ([#53408](https://github.com/apache/airflow/pull/53408))
- Add `run_on_latest_version` support for backfill and clear operations ([#52177](https://github.com/apache/airflow/pull/52177))
- Add `run_id_pattern` search for Dag Run API ([#52437](https://github.com/apache/airflow/pull/52437))
- Add tracking of triggering user to Dag runs ([#51738](https://github.com/apache/airflow/pull/51738))
- Expose DAG parsing duration in the API ([#54752](https://github.com/apache/airflow/pull/54752))

## New API Endpoints:

- Add Human-in-the-Loop (HITL) endpoints for approval workflows ([#52868](https://github.com/apache/airflow/pull/52868), [#53373](https://github.com/apache/airflow/pull/53373), [#53376](https://github.com/apache/airflow/pull/53376), [#53885](https://github.com/apache/airflow/pull/53885), [#53923](https://github.com/apache/airflow/pull/53923), [#54308](https://github.com/apache/airflow/pull/54308), [#54310](https://github.com/apache/airflow/pull/54310), [#54723](https://github.com/apache/airflow/pull/54723), [#54773](https://github.com/apache/airflow/pull/54773), [#55019](https://github.com/apache/airflow/pull/55019), [#55463](https://github.com/apache/airflow/pull/55463), [#55525](https://github.com/apache/airflow/pull/55525), [#55535](https://github.com/apache/airflow/pull/55535), [#55603](https://github.com/apache/airflow/pull/55603), [#55776](https://github.com/apache/airflow/pull/55776))
- Add endpoint to watch dag run until finish ([#51920](https://github.com/apache/airflow/pull/51920))
- Add TI bulk actions endpoint ([#50443](https://github.com/apache/airflow/pull/50443))
- Add Keycloak Refresh Token Endpoint ([#51657](https://github.com/apache/airflow/pull/51657))

## Deprecations:

- Mark `DagDetailsResponse.concurrency` as deprecated ([#55150](https://github.com/apache/airflow/pull/55150))

## Bug Fixes:

- Fix dag import error modal pagination ([#55719](https://github.com/apache/airflow/pull/55719))


# v3.0.2

## Major changes:

- Add `owner_links` field to DAGDetailsResponse ([#50557](https://github.com/apache/airflow/pull/50557))
- Allow non-string valid JSON values in Variable import ([#49844](https://github.com/apache/airflow/pull/49844))
- Add `bundle_version` to DagRun response ([#49726](https://github.com/apache/airflow/pull/49726))
- Use `NonNegativeInt` for `backfill_id` ([#49691](https://github.com/apache/airflow/pull/49691))
- Rename operation IDs for task instance endpoints to include map indexes ([#49608](https://github.com/apache/airflow/pull/49608))
- Remove filtering by last dag run state in patch dags endpoint ([#51176](https://github.com/apache/airflow/pull/51176))
- Make `dag_run` nullable in Details page ([#50719](https://github.com/apache/airflow/pull/50719))

## Bug Fixes

- Fix OpenAPI schema for `get_log` API ([#50547](https://github.com/apache/airflow/pull/50547))
- Fix bulk action annotation ([#50852](https://github.com/apache/airflow/pull/50852))
- Fix `patch_task_instance` endpoint ([#50550](https://github.com/apache/airflow/pull/50550))

# v3.0.0

This is the first release of the **Airflow 3.0.0** Python client. It introduces compatibility with the new [Airflow 3.0 REST API](https://airflow.apache.org/docs/apache-airflow/3.0.0/stable-rest-api-ref.html), and includes several **breaking changes** and behavior updates.

Below is a list of important changes. Refer to individual endpoint documentation for full details.

- API v1 (`/api/v1`) has been dropped and replaced with API v2(`/api/v2`).

- **422 Validation Errors (instead of 400)**

  The API now returns `422 Unprocessable Entity` for validation errors (e.g. bad payload, path params, or query params), instead of `400 Bad Request`.

- **Partial response support removed (`fields` parameter)**

  Endpoints like `GET /dags` no longer support the `fields` query param for partial responses. Full objects are returned by default. This feature may return in a future 3.x release.

- Passing list in query parameters switched from ``form, non exploded`` to ``form, exploded`` i.e before ``?my_list=item1,item2`` now ``?my_list=item1&my_list=item2``

- **`execution_date` has been removed**

  The previously deprecated `execution_date` parameter and fields are now fully removed. Use `logical_date` instead.

- **Datetime format updated to RFC3339-compliant**

  Datetimes returned are now in [RFC3339](https://datatracker.ietf.org/doc/html/rfc3339) format (e.g. `2024-10-01T13:00:00Z`). Both `Z` and `+00:00` forms are accepted in inputs.
  → This change comes from FastAPI & Pydantic v2 behavior.
  [More info](https://github.com/fastapi/fastapi/discussions/7693#discussioncomment-5143311)

- PATCH on ``DagRun`` and ``TaskInstance`` are more generic and allow in addition to update the resource state and the note content.

  Therefore, the two legacy dedicated endpoints to update a ``DagRun`` note and ``TaskInstance`` note have been removed.

  Same for the set task instance state, it is now handled by the broader PATCH on task instances.

- ``assets/queuedEvent`` endpoints have moved to ``assets/queuedEvents`` for consistency.

- **`dag_parsing` returns 409 for duplicates**

  If a `DagPriorityParsingRequest` already exists, `POST /dag_parsing` now returns `409 Conflict` instead of `201 Created`.

- **Default value change in `clearTaskInstances`**

  The `reset_dag_runs` field now defaults to `true` instead of `false`.

- **Pool name is no longer editable**

  `PATCH /pools/{pool_name}` can no longer be used to rename a pool. Pool names are immutable via the API.

- **`logical_date` is now a required nullable field**

  When triggering a DAG run (`POST /dags/{dag_id}/dagRuns`), `logical_date` is now required but can explicitly be set to `null`.

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

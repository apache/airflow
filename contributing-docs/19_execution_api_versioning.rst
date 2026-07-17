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

Execution API Versioning
========================

Airflow's Task Execution API uses `Cadwyn <https://github.com/zmievsa/cadwyn>`_ for API versioning with CalVer.
This allows us to maintain backward compatibility while evolving the API over time.

Why?
----

Airflow components (e.g., workers, API servers) could be deployed independently. This can lead
to version mismatches—for example, a worker using Task SDK 1.0.0 (requiring Airflow >=3.0.0) while the
API server has been upgraded to 3.0.1. Without versioning, such mismatches can cause runtime failures or subtle bugs.
Enforcing a clear versioning contract ensures backward compatibility and allows for safe, incremental upgrades
across different components.

Note: We're prioritizing backward compatibility, older clients should continue to work seamlessly with newer servers.
Since the client (Task SDK) and server (API) are still coupled, we can look at forward compatibility once we have a
clear versioning strategy between Task SDK and Airflow Core.

Versioning Principles
---------------------

1. Any change to the API schema or behavior that affects existing clients requires a new version.
2. All versions are maintained in the codebase
3. Migrations encapsulate all schema changes.
4. Core code logic remains version-agnostic.

Version Changes
---------------

When making changes to the Execution API, you must:

1. Add a new migration module (e.g., ``v2025_04_28.py``). Pick a likely date for when the Airflow version
   will be released.

   - Use the ``vYYYY_MM_DD`` format.
   - New migrations can be added to an existing unreleased version; not every migration needs a new version.
   - For unreleased versions, pick a likely date for when it will be released.
   - The version number should be unique and not conflict with any existing versions.
2. Document the changes in the migration.
3. Update tests to cover both old and new versions.
4. Ensure backward compatibility wherever possible.

Example Version Migration
-------------------------

Here's an example of how to add a new version migration that adds a new field ``consumed_asset_events`` to the
``DagRun`` model.

.. code-block:: python

    from cadwyn import ResponseInfo, VersionChange, convert_response_to_previous_version_for, schema

    from airflow.api_fastapi.execution_api.datamodels.taskinstance import DagRun, TIRunContext


    class AddConsumedAssetEventsField(VersionChange):
        """Add the `consumed_asset_events` to DagRun model."""

        description = __doc__

        instructions_to_migrate_to_previous_version = (schema(DagRun).field("consumed_asset_events").didnt_exist,)

        @convert_response_to_previous_version_for(TIRunContext)  # type: ignore
        def remove_consumed_asset_events(response: ResponseInfo):  # type: ignore
            response.body["dag_run"].pop("consumed_asset_events")

Directory Structure
-------------------

The Execution API versioning code is organized as follows:

- ``airflow-core/src/airflow/api_fastapi/execution_api/versions/`` - Contains version migrations
- ``airflow-core/src/airflow/api_fastapi/execution_api/versions/head/`` - Contains the latest version models
- ``airflow-core/src/airflow/api_fastapi/execution_api/versions/v2025_04_28/`` - Contains version-specific models and migrations
- ``airflow-core/tests/unit/api_fastapi/execution_api/versions/head`` - Contains tests for the latest version
- ``airflow-core/tests/unit/api_fastapi/execution_api/versions/v2025_04_28`` - Contains tests for the version-specific models

Examples
--------

For an example of how to implement version changes, see:
- `PR #50528 <https://github.com/apache/airflow/pull/50528>`_
- `PR #48125 <https://github.com/apache/airflow/pull/48125>`_

Resources
---------

- `Cadwyn Documentation <https://docs.cadwyn.dev/>`_
- `Cadwyn GitHub Repository <https://github.com/zmievsa/cadwyn>`_
- `Cadwyn Version Changes Guide <https://docs.cadwyn.dev/concepts/version_changes/>`_

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

Adding a New API Endpoint in Apache Airflow
===========================================

This documentation outlines the steps required to add a new API endpoint in Apache Airflow. It includes implementing the logic, running pre-commit checks, and documenting the changes.

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**


Introduction
------------

The source code for the RestAPI is located under ``api_fastapi``. Endpoints are located under ``api_fastapi/views`` and contains different types of endpoints. The main two are ``public`` and ``ui``.
Public endpoints are part of the public API, standardized, well documented and most importantly backward compatible. UI endpoints are custom endpoints made for the frontend that do not respect backward compatibility i.e they can be adapted at any time for UI needs.
When adding an endpoint you should try as much as possible to make it reusable by the community, have a stable design in mind, standardized and therefore part of the public API. If this is not possible because the data types are too specific or subject to frequent change
then adding it to the UI endpoints is more suitable.


Step 1: Implement the Endpoint Logic
------------------------------------
1. Considering the details above decide whether your endpoints should be part of the ``public`` or ``ui`` interface.
1. Navigate to the appropriate views directory in ``api_fastapi/views``.
2. Register a new route for your endpoint with the appropriate HTTP method, query params, permissions, body type, etc.
3. Specify the appropriate Pydantic type in the return type annotation.

Example:

.. code-block:: python

  @dags_router.get("/dags")  # permissions go in the dependencies parameter here
  async def get_dags(
      *,
      limit: int = 100,
      offset: int = 0,
      tags: Annotated[list[str] | None, Query()] = None,
      dag_id_pattern: str | None = None,
      only_active: bool = True,
      paused: bool | None = None,
      order_by: str = "dag_id",
      session: Annotated[Session, Depends(get_session)],
  ) -> DAGCollectionResponse:
      pass


Step 2: Add tests for your Endpoints
------------------------------------
1. Verify manually with a local API that the endpoint behaves as expected.
2. Go to the test folder and initialize new tests.
3. Implements extensives tests to validate query param, permissions, error handling etc.


Step 3: Documentation
---------------------
Documentation is built automatically by FastAPI and our pre-commit hooks. Verify by going to ``/docs`` that the documentation is clear and appears as expected (body and return types, query params, validation)


Step 4: Run Pre-commit Hooks
-----------------------------
1. Ensure all code meets the project's quality standards by running pre-commit hooks.
2. Pre-commit hooks include static code checks, formatting, and other validations.
3. Persisted openapi spec is located in ``v1-generated.yaml`` and a hook will take care of updating it based on your new endpoint, you just need to add and commit the change.
4. Run the following command to execute all pre-commit hooks:

.. code-block:: bash

   pre-commit run --all-files


Optional: Adding Pydantic Model
-------------------------------
In some cases, you may need to define additional models for new data structures. For example, if you are adding an endpoint that involves new data objects or collections, you may define a model in a Python file. The model will be used to validate and serialize/deserialize objects. Here's an example:

.. code-block:: python

    class DAGModelResponse(BaseModel):
        """DAG serializer for responses."""

        dag_id: str
        dag_display_name: str
        is_paused: bool
        is_active: bool
        last_parsed_time: datetime | None

These models are defined to structure and validate the data handled by the API. Once defined, these models will automatically be added to the OpenAPI spec file as long as they are actually used by one endpoint.

After adding or modifying Pydantic models, make sure to run the pre-commit hooks again to update any generated files.

Situational: Legacy Endpoint Migration
--------------------------------------
When migrating legacy endpoint to the new FastAPI API:

1. Implement a feature complete endpoint in comparison to the legacy one or explain why this is not possible in your context.
2. Make sure to have a good test coverage by copying over the legacy test cases to the new endpoint. This will guarantee an isofunctional new endpoint.
3. Mark the legacy endpoint with the ``@mark_fastapi_migration_done`` decorator. This will help maintainers keep track of the endpoints remaining for the migration and those already migrated.

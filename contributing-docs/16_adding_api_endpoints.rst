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

This documentation outlines the steps required to add a new API endpoint in Apache Airflow. It includes implementing the logic, running prek hooks, and documenting the changes.

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**


Introduction
------------

The source code for the RestAPI is located under ``api_fastapi/core_api``. Endpoints are located under ``api_fastapi/core_api/routes`` and contains different types of endpoints. The main two are ``public`` and ``ui``.
Public endpoints are part of the public API, standardized, well documented and most importantly backward compatible. UI endpoints are custom endpoints made for the frontend that do not respect backward compatibility i.e they can be adapted at any time for UI needs.
When adding an endpoint you should try as much as possible to make it reusable by the community, have a stable design in mind, standardized and therefore part of the public API. If this is not possible because the data types are too specific or subject to frequent change
then adding it to the UI endpoints is more suitable.


Step 1: Implement the Endpoint Logic
------------------------------------
1. Considering the details above decide whether your endpoints should be part of the ``public`` or ``ui`` interface.
2. Navigate to the appropriate routes directory in ``api_fastapi/core_api/routes``.
3. Register a new route for your endpoint with the appropriate HTTP method, query params, permissions, body type, etc.
4. Specify the appropriate Pydantic type in the return type annotation.

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
      session: SessionDep,
  ) -> DagCollectionResponse:
      pass


Step 2: Add tests for your Endpoints
------------------------------------
1. Verify manually with a local API that the endpoint behaves as expected.
2. Go to the test folder and initialize new tests.
3. Implements extensive tests to validate query param, permissions, error handling etc.


Step 3: Documentation
---------------------
Documentation is built automatically by FastAPI and our prek hooks. Verify by going to ``/docs`` that the documentation is clear and appears as expected (body and return types, query params, validation)


Step 4: Run prek Hooks
----------------------
1. Ensure all code meets the project's quality standards by running prek hooks.
2. Prek hooks include static code checks, formatting, and other validations.
3. Persisted openapi spec is located in ``v2-rest-api-generated.yaml` and a hook will take care of updating it based on your new endpoint, you just need to add and commit the change.
4. Run the following command to execute all prek hooks:

.. code-block:: bash

   prek --all-files


Optional: Adding Pydantic Model
-------------------------------
In some cases, you may need to define additional models for new data structures. For example, if you are adding an endpoint that involves new data objects or collections, you may define a model in a Python file. The model will be used to validate and serialize/deserialize objects. Here's an example:

.. code-block:: python

    class DagModelResponse(BaseModel):
        """Dag serializer for responses."""

        dag_id: str
        dag_display_name: str
        is_paused: bool
        is_active: bool
        last_parsed_time: datetime | None

These models are defined to structure and validate the data handled by the API. Once defined, these models will automatically be added to the OpenAPI spec file as long as they are actually used by one endpoint.

After adding or modifying Pydantic models, make sure to run the prek hooks again to update any generated files.

------

If you happen to change architecture of Airflow, you can learn how we create our `Architecture diagrams <17_architecture_diagrams.rst>`__.

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

This documentation outlines the steps required to add a new API endpoint in Apache Airflow. It includes defining the endpoint in the OpenAPI specification, implementing the logic, running pre-commit checks, and documenting the changes.

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**

Step 1: Define the Endpoint in ``v1.yaml``
----------------------------------------
1. Navigate to the ``v1.yaml`` file, which contains the OpenAPI specifications.
2. Add a new path for your endpoint, specifying the URL path, HTTP method (GET, POST, etc.), and a brief summary.
3. Define the parameters required for the endpoint, including types, required/optional status, and default values.
4. Describe the responses, including status codes, content types, and schema details.

Example:

.. code-block:: yaml

   paths:
     /example/endpoint:
       get:
         summary: Example API endpoint
         description: This endpoint provides an example response.
         parameters:
           - name: example_param
             in: query
             required: true
             schema:
               type: string
         responses:
           "200":
             description: Successful response
             content:
               application/json:
                 schema:
                   type: object
                   properties:
                     message:
                       type: string
           "400":
             $ref: "#/components/responses/BadRequest"
           "404":
             $ref: "#/components/responses/NotFound"


Step 2: Implement the Endpoint Logic
------------------------------------
1. In the appropriate Python file, implement the endpoint's logic.
2. Ensure proper parameter handling and validation.
3. Implement the core logic, such as data retrieval or processing.
4. Add error handling for potential issues like missing parameters or invalid data.

Example:

.. code-block:: python

   @security.requires_access_dag("GET", DagAccessEntity.TASK_LOGS)
   @provide_session
   @unify_bucket_name_and_key
   @provide_bucket_name
   def get_example(
       *,
       example_param: str,
       session: Session = NEW_SESSION,
   ) -> APIResponse:
       # Implementation details here
       pass


Step 3: Run Pre-commit Hooks
-----------------------------
1. Ensure all code meets the project's quality standards by running pre-commit hooks.
2. Pre-commit hooks include static code checks, formatting, and other validations.
3. One specific pre-commit hook to note is the ``update-common-sql-api-stubs`` hook. This hook automatically updates the common SQL API stubs whenever it recognizes changes in the API. This ensures that any modifications to the API are accurately reflected in the stubs, maintaining consistency between the implementation and documentation.
4. Run the following command to execute all pre-commit hooks:

.. code-block:: bash

   pre-commit run --all-files


Optional: Adding Schemas
-----------------------------
In some cases, you may need to define additional schemas for new data structures. For example, if you are adding an endpoint that involves new data objects or collections, you may define a schema in a Python file. Here's an example:

.. code-block:: python

   class TaskLogPageSchema(Schema):
       """Schema for task log pagination details."""

       total_pages = fields.Int(description="Total number of pages for task logs.")
       current_page = fields.Int(description="Current page number.")
       page_size = fields.Int(description="Number of logs per page.")

These schemas are defined to structure and validate the data handled by the API. Once defined, you can include these schemas in the OpenAPI specification file (e.g., v1.yaml) and reference them in the API endpoint definitions.

For example, in v1.yaml, you might add:

.. code-block:: yaml

   components:
     schemas:
       TaskLogPage:
         type: object
         properties:
           total_pages:
             type: integer
             description: Total number of pages for task logs.
           current_page:
             type: integer
             description: Current page number.
           page_size:
             type: integer
             description: Number of logs per page.

Including schemas helps in automatically generating API documentation and ensures consistent data structures across the API.

After adding or modifying schemas, make sure to run the pre-commit hooks again to update any generated files.

Note on the New FastAPI API
---------------------------
As part of the AIP-84 you may be adding new endpoints to the FastAPI API by migrating some legacy endpoints. In case you are doing so don't forget to mark the legacy endpoint (Flask one) with the ``@mark_fastapi_migration_done`` decorator. This will help maintainers keep track of the endpoints remaining for the migration and those already migrated.

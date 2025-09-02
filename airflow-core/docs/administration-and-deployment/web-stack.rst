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



Web Stack
=========


Configuration
-------------

Sometimes you want to deploy the backend and frontend behind a
variable url path prefix. To do so, you can configure the url :ref:`config:api__base_url`
for instance, set it to ``http://localhost:28080/d12345``. All the APIs routes will
now be available through that additional ``d12345`` prefix. Without rebuilding
the frontend, XHR requests and static file queries should be directed to the prefixed url
and served successfully.

You will also need to update the execution API server url
:ref:`config:core__execution_api_server_url` for tasks to be able to reach the API
with the new prefix.

Separating API Servers
-----------------------

By default, both the Core API Server and the Execution API Server are served together:

.. code-block:: bash

   airflow api-server
   # same as
   airflow api-server --apps all
   # or
   airflow api-server --apps core,execution

If you want to separate the Core API Server and the Execution API Server, you can run them
separately. This might be useful for scaling them independently or for deploying them on different machines.

.. code-block:: bash

   # serve only the Core API Server
   airflow api-server --apps core
   # serve only the Execution API Server
   airflow api-server --apps execution

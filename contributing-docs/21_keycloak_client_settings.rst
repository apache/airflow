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

Setting up Keycloak Client for Breeze
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
To fully integrate Keycloak into local Airflow development with Breeze, you need to set up a Keycloak client. During client creation, the relevant section is called ``Login Settings``. After the client is created, this section is renamed to ``Access Settings``.

.. list-table::
   :header-rows: 1
   :widths: 25 75 75

   * - Field
     - Local (Breeze) Value
     - Local (Breeze) Templated Value
   * - Root URL
     - http://localhost:28080
     - ${authBaseUrl}
   * - Home URL
     - http://localhost:28080
     - <Not Usable>
   * - Valid Redirect URIs
     - http://localhost:28080/*
     - <Not Usable>
   * - Valid Post Logout Redirect URIs
     - http://localhost:28080/*
     - <Not Usable>
   * - Web Origins
     - http://localhost:28080
     - ${authBaseUrl}

After configuring the client, your settings will remain intact as long as you do not remove the Docker volumes.
To acquire client credentials, you need to check ``Credentials`` tab of the created client.

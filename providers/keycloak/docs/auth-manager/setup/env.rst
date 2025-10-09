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

============================
Setup a Keycloak environment
============================

To use the Keycloak auth manager, you must have a running Keycloak environment.
This is where you define users, groups, roles, and permissions.
Airflow delegates all authentication and authorization operations to Keycloak, querying it for every access decision.

If you don't have a Keycloak environment set up, please refer to the `official Keycloak documentation <https://www.keycloak.org/guides>`__ for installation and configuration guidance.

Setup and run Keycloak with Breeze (local development)
------------------------------------------------------

Setting up and running a Keycloak environment with Breeze for local development is straightforward.
Simply run ``breeze start-airflow --integration keycloak``. Breeze will handle starting Keycloak in a separate container.
The Keycloak admin console will be available at http://localhost:48080/.

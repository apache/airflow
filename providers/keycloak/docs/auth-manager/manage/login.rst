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

================================================
Manage login settings with Keycloak auth manager
================================================
This topic is related to the Keycloak ``Login Settings``.
This is shown when you are creating the client in Keycloak.
It also shown after you created the client. It is then under ``Access Settings`` tab.

Keycloak Client Configuration Guide
===================================
This document explains how to correctly configure a Keycloak Client using your production HTTP(s) URL
(``<https://yourcompany.airflow.com>``) as well as a local development example (Airflow Breeze example
app running on ``http://localhost``).

Overview
--------

To allow your application to authenticate users through Keycloak, you must correctly configure the following
fields in your Keycloak Client:

* Root URL
* Home URL
* Valid Redirect URIs
* Valid Post Logout Redirect URIs
* Web Origins
* Admin URL (optional)

This guide shows:

* **Production values**: using your company deployment ``<https://yourcompany.airflow.com>``
* **Local development values**: using Airflow Breeze ``http://localhost:28080``

Notes on Keycloak Template Variables
------------------------------------

``${authBaseUrl}``
This expands to **Keycloak's own base URL**, not your application URL. You should not use it as a Root URL for
your Airflow application.


Sample Settings Table
---------------------
.. list-table::
   :header-rows: 1
   :widths: 30 40 30

   * - Field
     - Production Value
     - Local (Breeze) Example
   * - Root URL
     - https://yourcompany.airflow.com
     - http://localhost:28080
   * - Home URL
     - https://yourcompany.airflow.com
     - http://localhost:28080
   * - Valid Redirect URIs
     - https://yourcompany.airflow.com/*
     - http://localhost:28080/*
   * - Valid Post Logout Redirect URIs
     - https://yourcompany.airflow.com/*
     - http://localhost:28080/*
   * - Web Origins
     - https://yourcompany.airflow.com
     - http://localhost:28080

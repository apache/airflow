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
These settings appear when you create a client in Keycloak, and remain accessible afterward under the ``Access Settings`` tab.
They play an important role in limiting the client's scope and reducing its potential impact area.

Keycloak Client Configuration Guide
===================================
This document explains how to properly configure a Keycloak client using your production HTTP(S) URL
(``<https://yourcompany.airflow.com>``).

Overview
--------
Client configuration is a crucial part of enabling Keycloak authentication for your application.
You must ensure that Client Authentication, Authorization, and the Authentication Flow are correctly configured.

Set ``Client Authentication`` to ``ON``.
Set ``Authorization`` to ``ON``.
For ``Authentication Flow`` values, refer to the table below.

Login settings (After Client is Created)
----------------------------------------
.. list-table::
    :header-rows: 1
    :widths: 30 70

    * - Field
      - Value
    * - Standard Flow
      - ON
    * - Direct Access Grants
      - ON
    * - Implicit Flow
      - OFF
    * - Service accounts roles
      - ON (by default if configuration overridden from Keycloak)
    * - OAuth 2.0 Device Authorization Grant
      - OFF
    * - OIDC CIBA Grant
      - OFF

To enable your application to authenticate users through Keycloak, you must configure the following fields in your Keycloak client:

* Root URL
* Home URL
* Valid Redirect URIs
* Valid Post Logout Redirect URIs
* Web Origins
* Admin URL (optional)

Login Settings (While Creating Client)/Access Settings (After Client is Created)
--------------------------------------------------------------------------------
.. list-table::
   :header-rows: 1
   :widths: 30 40

   * - Field
     - Production Value
   * - Root URL
     - https://yourcompany.airflow.com
   * - Home URL
     - https://yourcompany.airflow.com
   * - Valid Redirect URIs
     - https://yourcompany.airflow.com/*
   * - Valid Post Logout Redirect URIs
     - https://yourcompany.airflow.com/*
   * - Web Origins
     - https://yourcompany.airflow.com

Logout settings (After Client is Created)
-----------------------------------------
.. list-table::
    :header-rows: 1
    :widths: 30 70

    * - Field
      - Value
    * - Front channel logout
      - ON

Notes on Keycloak Template Variables
------------------------------------

``${authBaseUrl}``
This resolves to **Keycloak's own base URL**, not your application's URL. You should not use it as the Root URL for your Airflow application.

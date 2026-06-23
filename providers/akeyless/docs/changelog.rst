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

Changelog
=========

0.2.0
-----

.. note::
    The Akeyless connection field ``jwt`` has been renamed to ``jwt_token`` so the credential
    is correctly masked in logs. Update existing Akeyless connections to use the new field
    name. As this provider is still pre-1.0, the breaking change ships in a minor release.

Breaking changes
~~~~~~~~~~~~~~~~~~

* ``Fix Akeyless JWT connection credential is not redacted (#67443)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare providers release 2026-05-05 (#66424)``

0.1.0
-----

Initial release.

Features
~~~~~~~~

* ``AkeylessHook``: Hook for interacting with Akeyless Vault Platform (static,
  dynamic, and rotated secrets; item CRUD; multiple auth methods).
* ``AkeylessBackend``: Secrets backend for sourcing Airflow Connections,
  Variables, and Configuration from Akeyless.
* Custom ``akeyless`` connection type with dedicated UI fields for all
  authentication methods.

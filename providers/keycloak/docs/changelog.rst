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

.. NOTE TO CONTRIBUTORS:
    Please, only add notes to the Changelog just below the "Changelog" header when there are some breaking changes
    and you want to add an explanation to the users on how they are supposed to deal with them.
    The changelog is updated and maintained semi-automatically by release manager.

``apache-airflow-providers-keycloak``

Changelog
---------

0.1.0
.....


Release Date: ``|PypiReleaseDate|``

Features
~~~~~~~~

* ``Add 'LIST' scope in Keycloak auth manager (#54998)``

Doc-only
~~~~~~~~

* ``docs(keycloak): Update documentation for Keycloak auth manager CLI usage and permission management (#54928)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Switch pre-commit to prek (#54258)``

.. Review and move the new changes to one of the sections above:
   * ``Fix Airflow 2 reference in README/index of providers (#55240)``

0.0.1
.....

.. note::
    Provider is still WIP. It can be used with production but we may introduce breaking changes without following semver until version 1.0.0

* ``Initial version of the provider (#46694)``

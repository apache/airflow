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

0.3.0
.....

Features
~~~~~~~~

* ``Add 'LIST' permission to admin role in Keycloak auth manager (#57978)``

Bug Fixes
~~~~~~~~~

* ``Fix logout in Fab and Keycloak auth managers (#57992)``

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``

Doc-only
~~~~~~~~

* ``[Doc] Fixing some typos and spelling errors (#57225)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Enable PT006 rule to keycloak Provider test (#57923)``
   * ``Synchronize default versions in all split .pre-commit-config.yaml (#57851)``
   * ``Extract prek hooks for Keycloak provider (#57182)``

0.2.0
.....

Features
~~~~~~~~

* ``Integrate KeycloakAuthManager with airflowctl (#55969)``

Bug Fixes
~~~~~~~~~

* ``Update refresh token flow (#55506)``
* ``Update authentication to handle JWT token in backend (#56633)``

Doc-only
~~~~~~~~

* ``Remove placeholder Release Date in changelog and index files (#56056)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable PT011 rule to prvoider tests (#56021)``

0.1.0
.....


Features
~~~~~~~~

* ``Add 'LIST' scope in Keycloak auth manager (#54998)``

Doc-only
~~~~~~~~

* ``docs(keycloak): Update documentation for Keycloak auth manager CLI usage and permission management (#54928)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Switch pre-commit to prek (#54258)``
   * ``Fix Airflow 2 reference in README/index of providers (#55240)``

0.0.1
.....

.. note::
    Provider is still WIP. It can be used with production but we may introduce breaking changes without following semver until version 1.0.0

* ``Initial version of the provider (#46694)``

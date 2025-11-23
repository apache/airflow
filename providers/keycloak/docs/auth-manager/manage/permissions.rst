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

==================================================
Manage user permissions with Keycloak auth manager
==================================================

You can set-up and manage user permissions with Keycloak auth manager in different ways:

1. Using Airflow CLI
2. Using Keycloak console

With Airflow CLI
----------------
Setting up the permissions can be done using CLI commands.
They can create the permissions and needed resources easily.

There are two options to create the permissions:

* Create all permissions (Scopes, Resources, Permissions) in one go using one CLI command
* Create all permissions (Scopes, Resources, Permissions) step-by-step using the CLI commands

CLI commands take the following parameters:

* ``--username``: Keycloak admin username
* ``--password``: Keycloak admin password
* ``--user-realm``: Keycloak user realm
* ``--client-id``: Keycloak client id (default: admin-cli)

Please check the `Keycloak auth manager CLI </cli-refs.html>`_ documentation for more information about accepted parameters.

One-go creation of permissions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There is a single command do all the magic for you.

This command will create scopes, resources and permissions in one-go.

.. code-block:: bash

  airflow keycloak-auth-manager create-all

Step-by-step creation of permissions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

First we need to create scopes for the permissions. These scopes later will be used in Keycloak authorization [1].

This command will create scopes for certain types of permissions.

.. code-block:: bash

  airflow keycloak-auth-manager create-scopes

This command will create resources for certain types of permissions.

.. code-block:: bash

  airflow keycloak-auth-manager create-resources

Finally, with the command below, we create the permissions using the previously created scopes and resources.

.. code-block:: bash

  airflow keycloak-auth-manager create-permissions

This will create

* read-only permissions
* admin permissions
* user permissions
* operations permissions

More resources about permissions can be found in the official documentation of Keycloak:

1- `Keycloak Authorization Process <https://www.keycloak.org/docs/latest/authorization_services/index.html#the-authorization-process>`_

2- `Keycloak Permission Overview <https://www.keycloak.org/docs/latest/authorization_services/index.html#_permission_overview>`_

3- `Keycloak Creating scope-based Permissions <https://www.keycloak.org/docs/latest/authorization_services/index.html#_policy_overview>`_

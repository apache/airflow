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
* ``--password``: Keycloak admin password. Specifying the parameter without a value will prompt for the password securely.
* ``--user-realm``: Keycloak user realm
* ``--client-id``: Keycloak client id (default: admin-cli)

They also take the following optional parameters:

* ``--dry-run``: If set, the command will check the connection to Keycloak and print the actions that would be performed, without actually executing them.
* ``--teams``: Comma-separated list of team names to create team-scoped resources and permissions.

Please check the `Keycloak auth manager CLI </cli-ref.html>`_ documentation for more information about accepted parameters.

.. warning::

    ``create-all``, ``create-permissions``, and ``create-team`` configure the
    Keycloak client's resource-server decision strategy as ``AFFIRMATIVE``,
    in both non-team and multi-team mode. This allows an applicable permission
    that permits access to authorize a request even when another applicable
    permission denies it.

    This setting applies to the entire client, including existing custom
    permissions. Authorization rules that rely on the ``UNANIMOUS`` strategy
    to combine a permit with a restrictive denial can therefore behave
    differently. Review existing custom permissions before running these
    commands against an existing client. Use ``--dry-run`` to preview the
    strategy change without applying it.

One-go creation of permissions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There is a single command do all the magic for you.

This command will create scopes, resources and permissions in one-go.

.. code-block:: bash

  airflow keycloak-auth-manager create-all

To create team-scoped resources and permissions (so Keycloak can enforce per-team access) pass ``--teams``:

.. code-block:: bash

  airflow keycloak-auth-manager create-all --teams team-a,team-b

Step-by-step creation of permissions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

First we need to create scopes for the permissions. These scopes later will be used in Keycloak authorization [1].

This command will create scopes for certain types of permissions.

.. code-block:: bash

  airflow keycloak-auth-manager create-scopes

This command will create resources for certain types of permissions.

.. code-block:: bash

  airflow keycloak-auth-manager create-resources --teams team-a,team-b

Finally, with the command below, we create the permissions using the previously created scopes and resources.

.. code-block:: bash

  airflow keycloak-auth-manager create-permissions --teams team-a,team-b

Without ``--teams``, the CLI creates resource-specific permissions for non-team installations.
It generates a ``<Resource>Read`` permission with ``GET`` and ``LIST`` scopes and a
``<Resource>Write`` permission with ``POST``, ``PUT``, and ``DELETE`` scopes for each resource
except the generic ``Menu`` resource. The current model has 24 read/write permissions for
12 resources, plus ``DefaultMenuAccess`` for individual menu resources with the ``MENU`` scope.

The generated non-team grants are:

.. list-table:: Non-team role grants
    :header-rows: 1
    :widths: 40 30 30

    * - Resources
      - Read roles
      - Write roles
    * - ``Dag``, ``Asset``
      - Viewer, User, Op, Admin, SuperAdmin
      - User, Op, Admin, SuperAdmin
    * - ``Backfill``, ``Connection``, ``Pool``, ``Variable``
      - Viewer, User, Op, Admin, SuperAdmin
      - Op, Admin, SuperAdmin
    * - ``AdminView``, ``AssetAlias``, ``Configuration``, ``Custom``, ``Team``, ``View``
      - Viewer, User, Op, Admin, SuperAdmin
      - Admin, SuperAdmin

Higher roles are explicitly attached to the appropriate permissions; composite roles are not
required. ``DefaultMenuAccess`` grants all individual menu resources to all five default roles.
These are Keycloak scope grants. Airflow endpoint checks and configuration exposure settings
can impose additional restrictions, and not every resource has an endpoint for every scope.

With ``--teams``, the CLI instead creates team-specific ``ReadOnly-<team>``, ``User-<team>``,
``Op-<team>``, and ``Admin-<team>`` permissions, together with global list, view, and menu
permissions. Use ``create-team`` as described below to configure each team's group and policy
attachments.

.. note::

    Provisioning preserves legacy permissions and extra policy attachments in non-team mode.
    Re-running the commands creates the new resource-specific permissions but does not remove
    the old ``ReadOnly``, ``User``, ``Op``, or ``Admin`` permissions. Existing legacy or custom
    grants can therefore affect effective access; upgrading the provider alone does not migrate
    the client's authorization configuration.

Managing teams with Keycloak
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When using team-scoped resources, create Keycloak groups that represent teams and attach them to the
team-specific permissions. The CLI provides helpers for this flow:

.. code-block:: bash

  airflow keycloak-auth-manager create-team team-a
  airflow keycloak-auth-manager add-user-to-team user-a team-a

These commands create a Keycloak group named ``team-a``, set up team-scoped resources and permissions,
and attach team-specific policies to the permissions for that team.
When using team-scoped permissions, the model is:

* Keycloak group represents the team (``<name>``)
* Keycloak roles (Admin/Op/User/Viewer) represent the role within that team
* Team policies require both group membership and role membership
* A separate ``SuperAdmin`` role can be used for global admin access across all teams

Note: the CLI creates groups, resources, permissions, and policies, but **does not assign roles to users**.
You must assign the appropriate Keycloak roles (Admin/Op/User/Viewer or SuperAdmin) to each user separately.
In multi-team mode, the ``Admin`` role is **team-scoped** (group + role). Only ``SuperAdmin`` grants global
admin access across all teams.
Views over records that are not tied to a Dag or a team -- audit log entries not tied to a Dag,
import errors for files with no registered Dag and reparsing such files -- are checked against the
``AdminView`` resource, which the CLI grants to ``SuperAdmin`` only. The other views are checked against
the ``View`` resource, which every team role can read.

.. note::

    These commands do not migrate an existing non-team Keycloak client to multi-team mode.
    Provision a separate client for multi-team configuration. Reusing a non-team client can
    leave its existing grants active alongside the team-specific permissions.

More resources about permissions can be found in the official documentation of Keycloak:

1- `Keycloak Authorization Process <https://www.keycloak.org/docs/latest/authorization_services/index.html#the-authorization-process>`_

2- `Keycloak Permission Overview <https://www.keycloak.org/docs/latest/authorization_services/index.html#_permission_overview>`_

3- `Keycloak Creating scope-based Permissions <https://www.keycloak.org/docs/latest/authorization_services/index.html#_policy_overview>`_

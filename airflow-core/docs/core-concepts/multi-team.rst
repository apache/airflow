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


Multi-Team
==========

.. warning::
  Multi-Team is an :ref:`experimental <experimental>`/incomplete feature currently in preview. The feature will not be
  fully complete until Airflow 3.3 and may be subject to changes without warning based on user feedback.
  See the :ref:`Work in Progress <multi-team-work-in-progress>` section below for details.

Multi-Team Airflow is a feature that enables organizations to run multiple teams within a single Airflow deployment while providing resource isolation and team-based access controls. This feature is designed for medium to large organizations that need to share Airflow infrastructure across multiple teams while maintaining logical separation of resources.

.. note::

    Multi-Team Airflow is different from multi-tenancy. It provides isolation within a single deployment but is not designed for complete tenant separation. All teams share the same Airflow infrastructure, scheduler, and metadata database.

When to Use Multi-Team Mode
---------------------------

Multi-Team mode is designed for medium to large organizations that typically have many discrete teams who need access to an Airflow environment. Often there is a dedicated platform or DevOps team to manage the shared Airflow infrastructure, but this is not required.

**Use Multi-Team mode when:**

- You have many teams that need to share Airflow infrastructure
- You need resource isolation (Variables, Connections, Secrets, etc) between teams
- You want separate execution environments per team
- You want separate views per team in the Airflow UI
- You want to minimize operational overhead or cost by sharing a single Airflow deployment

Core Concepts
-------------

Teams
^^^^^

A **Team** is a logical grouping that represents a group of users within your organization. Teams are in part stored in the Airflow metadata database and serve as the basis for resource isolation.

Teams within the Airflow database have a very simple structure, only containing one field:

- **name**: A unique identifier for the team (3-50 characters, alphanumeric with hyphens and underscores)

Teams are associated with Dag bundles through a separate association table, which links team names to Dag bundle names.

Dag Bundles and Team Ownership
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Teams are associated with Dags through **Dag Bundles**. A Dag bundle can be owned by at most one team. When a Dag bundle is assigned to a team:

- All Dags within that bundle belong to that team
- Tasks in those Dags inherit the team association
- All Callbacks associated with those Dags also inherit the team association
- The scheduler uses this relationship to determine which executor to use

.. note::

    The relationship chain is: **Task/Callback → Dag → Dag Bundle → Team**

Resource Isolation
^^^^^^^^^^^^^^^^^^

When Multi-Team mode is enabled, the following resources can be scoped to specific teams:

- **Variables**: Team members can only access variables owned by their team or global variables
- **Connections**: Team members can only access connections owned by their team or global connections
- **Pools**: Pools can be assigned to teams

Resources without a team assignment are considered **global** and accessible to all teams.

Secrets Backends
""""""""""""""""

Airflow's secrets backends, including: environment variables, metastore and local filesystem are team-aware. Custom
Secrets Backends are supported on a case by case basis.

When a task requests a Variable or Connection, the secrets backend will return a team-specific value, if any. The
backend will automatically resolve the correct value based on the requesting task's team.

Enabling Multi-Team Mode
------------------------

To enable Multi-Team mode, set the following configuration in your ``airflow.cfg``:

.. code-block:: ini

    [core]
    multi_team = True

Or via environment variable:

.. code-block:: bash

    export AIRFLOW__CORE__MULTI_TEAM=True

.. warning::

    Changing this setting on an existing deployment requires careful planning.

Creating and Managing Teams
---------------------------

Teams are managed using the Airflow CLI. The following commands are available:

Creating a Team
^^^^^^^^^^^^^^^

.. code-block:: bash

    airflow teams create <team_name>

Team names must be 3-50 characters long and contain only alphanumeric characters, hyphens, and underscores.

Listing Teams
^^^^^^^^^^^^^

.. code-block:: bash

    airflow teams list

This displays all teams in the deployment with their names.

Deleting a Team
^^^^^^^^^^^^^^^

.. code-block:: bash

    airflow teams delete <team_name>

Or to skip the confirmation prompt:

.. code-block:: bash

    airflow teams delete <team_name> --yes

.. warning::

    A team cannot be deleted if it has associated resources (Dag bundles, Variables, Connections, or Pools). You must remove these associations first.

Configuring Team Resources
--------------------------

Team-scoped Variables
^^^^^^^^^^^^^^^^^^^^^

Variables can be associated with teams when created. Tasks belonging to a team can access:

1. Variables owned by their team
2. Global variables (no team association)

When a task requests a variable, the system checks for a team-specific variable first.

Team-scoped variables can be created and managed through the Airflow UI or via environment variables.

**Via environment variables**, you can set team-scoped variables using the format:

.. code-block:: bash

    # Global variable
    export AIRFLOW_VAR_MY_VARIABLE="global_value"

    # Team-scoped variable for "team_a"
    export AIRFLOW_VAR__TEAM_A___MY_VARIABLE="team_a_value"

The format is: ``AIRFLOW_VAR__{TEAM}___{KEY}`` (note: double underscore before team, triple underscore between team and key)

Team-scoped Connections
^^^^^^^^^^^^^^^^^^^^^^^

Connections follow the same pattern as variables. Tasks can access connections owned by their team or global connections.

Team-scoped connections can be created and managed through the Airflow UI or via environment variables.

**Via environment variables**:

.. code-block:: bash

    # Global connection
    export AIRFLOW_CONN_MY_DATABASE="postgresql://..."

    # Team-scoped connection for "team_a"
    export AIRFLOW_CONN__TEAM_A___MY_DATABASE="postgresql://..."

The format is: ``AIRFLOW_CONN__{TEAM}___{CONN_ID}`` (note: double underscore before team, triple underscore between team and connection ID)

Team-scoped Pools
^^^^^^^^^^^^^^^^^

Pools can be assigned to teams, providing resource isolation for task execution slots. When a pool is assigned to a team:

- Only tasks from that team can use the pool
- The scheduler validates pool access when scheduling tasks
- Tasks attempting to use a pool from another team will fail with an error

Pools without a team assignment remain globally accessible to all teams.

Team-based Executor Configuration
---------------------------------

One of the most powerful features of Multi-Team mode is the ability to configure different executors per team or the
same executor but configured differently. This allows teams to have dedicated compute resources or execution flows per
team.

Similarly to global executors, team-scoped executor configurations also support multiple executors (for example, both
``LocalExecutor`` and ``KubernetesExecutor``), allowing tasks within that team to specify which executor to use. For
details on configuring multiple executors, see :ref:`Using Multiple Executors Concurrently <using-multiple-executors-concurrently>`.

Configuration Format
^^^^^^^^^^^^^^^^^^^^

The executor configuration supports team-based assignments using the following format:

.. code-block:: ini

    [core]
    executor = GlobalExecutor;team1=Team1Executor;team2=Team2Executor

For example:

.. code-block:: ini

    [core]
    executor = LocalExecutor;team_a=CeleryExecutor;team_b=KubernetesExecutor

In this configuration:

- ``LocalExecutor`` is the global default executor
- Tasks from ``team_a`` use ``CeleryExecutor`` or the ``LocalExecutor``
- Tasks from ``team_b`` use ``KubernetesExecutor`` or the ``LocalExecutor``
- Tasks from the global scope use the ``LocalExecutor``

Important Rules
^^^^^^^^^^^^^^^

1. **Global executor must come first**: At least one global executor (without a team prefix) must be configured and must appear before any team-specific executors.

2. **Teams must exist**: All team names in the executor configuration must exist in the database before Airflow starts.

3. **Executor must support multi-team**: Not all executors support multi-team mode. The executor class must have ``supports_multi_team = True``.

4. **No duplicate teams**: Each team may only appear once in the executor configuration.

5. **No duplicate executors within a team**: A team cannot have the same executor configured multiple times.

Example configurations:

.. code-block:: ini

    # Valid: Global executor with team-specific executors
    executor = LocalExecutor;team_a=CeleryExecutor

    # Valid: Multiple team-specific executors
    executor = LocalExecutor;team_a=CeleryExecutor;team_b=KubernetesExecutor

    # Valid: Multiple executors globally and per team
    executor = LocalExecutor,KubernetesExecutor;team_a=CeleryExecutor,KubernetesExecutor;team_b=LocalExecutor

    # Invalid: No global executor
    executor = team_a=CeleryExecutor;team_b=LocalExecutor

    # Invalid: Global executor after team executor
    executor = team_a=CeleryExecutor;LocalExecutor

    # Invalid: Duplicate Team
    executor = LocalExecutor;team_a=CeleryExecutor;team_b=LocalExecutor;team_a=KubernetesExecutor

    # Invalid: Duplicate Executor within a Team
    executor = LocalExecutor;team_a=CeleryExecutor,CeleryExecutor;team_b=LocalExecutor

Dag Bundle to Team Association
------------------------------

Dag bundles are associated with teams through the Dag bundle configuration. When configuring your Dag bundles, you specify a ``team_name`` for each bundle:

.. code-block:: ini

    [dag_processor]
    dag_bundle_config_list = [
        {
            "name": "team_a_dags",
            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
            "kwargs": {"path": "/opt/airflow/dags/team_a"},
            "team_name": "team_a"
        },
        {
            "name": "team_b_dags",
            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
            "kwargs": {"path": "/opt/airflow/dags/team_b"},
            "team_name": "team_b"
        },
        {
            "name": "shared_dags",
            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
            "kwargs": {"path": "/opt/airflow/dags/shared"}
        }
    ]

In this example:

- Dags in ``/opt/airflow/dags/team_a`` belong to ``team_a``
- Dags in ``/opt/airflow/dags/team_b`` belong to ``team_b``
- Dags in ``/opt/airflow/dags/shared`` have no team (global)

.. note::

    The team specified in ``team_name`` must exist in the database before syncing the Dag bundles. Create teams first using ``airflow teams create``.

How Scheduling Works
--------------------

When Multi-Team mode is enabled, the scheduler performs additional logic to determine the correct executor for each task:

.. TODO: Diagram showing the scheduler team resolution flow would be helpful here

1. **Task to Team Resolution**: The scheduler resolves the team for each task by following the relationship chain:

   - Task → Dag (via ``dag_id``)
   - Dag → Dag Bundle (via ``bundle_name``)
   - Dag Bundle → Team (via ``dag_bundle_team`` association table)

2. **Executor Selection**: Once the team is determined:

   - If a team-specific executor is configured, use that executor
   - Otherwise, fall back to the global default executor

.. warning::

    Multi-Team Airflow provides **logical isolation** for a secure perimeter around teams, not complete isolation. All
    teams share the same metadata database and common Airflow infrastructure. For absolutely strict security
    requirements, consider separate Airflow deployments.

Architecture
------------

The following diagram shows a Multi-Team Airflow deployment with resource isolation between teams.

The components in blue are the shared components and those in green are the team components (note the green shadow box
indicating more than one team is present in the architecture).

.. image:: /img/multi_team_arch_diagram.png
   :alt: Multi-Team Airflow Architecture showing resource isolation between teams

Important Considerations
------------------------

.. _multi-team-work-in-progress:

Work in Progress
^^^^^^^^^^^^^^^^

Multi-Team mode is currently an experimental feature in preview. It is not yet fully complete and may be subject to changes without warning based on user feedback. Some missing functionality includes:

- Dimensional metrics by team
- Async support (Triggers, Event Driven Scheduling, async Callbacks, etc)
- Some UI elements may not be fully team-aware
- Full provider support for executors and secrets backends
- Plugins

Global Uniqueness of Identifiers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Dag IDs, Variable keys, and Connection IDs must be unique across the entire Airflow deployment**, regardless of which team owns them. This is similar to how S3 bucket names are globally unique across all AWS accounts. You should establish naming conventions within your organization to avoid naming conflicts (e.g. prefix identifiers with the team name)

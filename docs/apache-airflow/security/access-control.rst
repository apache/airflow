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

Access Control
==============

Access Control of Airflow Webserver UI is handled by Flask AppBuilder (FAB).
Please read its related `security document <http://flask-appbuilder.readthedocs.io/en/latest/security.html>`_
regarding its security model.

.. spelling:word-list::
    clearTaskInstances
    dagRuns
    dagSources
    eventLogs
    importErrors
    taskInstances
    xcomEntries

Default Roles
'''''''''''''
Airflow ships with a set of roles by default: Admin, User, Op, Viewer, and Public.
By default, only ``Admin`` users can configure/alter permissions for roles. However,
it is recommended that these default roles remain unaltered, and instead ``Admin`` users
create new roles with the desired permissions if changes are necessary.

Admin
^^^^^
``Admin`` users have all possible permissions, including granting or revoking permissions from
other users.

Public
^^^^^^
``Public`` users (anonymous) don't have any permissions.

Viewer
^^^^^^
``Viewer`` users have limited read permissions:

.. exampleinclude:: /../../airflow/auth/managers/fab/security_manager/override.py
    :language: python
    :start-after: [START security_viewer_perms]
    :end-before: [END security_viewer_perms]

User
^^^^
``User`` users have ``Viewer`` permissions plus additional permissions:

.. exampleinclude:: /../../airflow/auth/managers/fab/security_manager/override.py
    :language: python
    :start-after: [START security_user_perms]
    :end-before: [END security_user_perms]

Op
^^
``Op`` users have ``User`` permissions plus additional permissions:

.. exampleinclude:: /../../airflow/auth/managers/fab/security_manager/override.py
    :language: python
    :start-after: [START security_op_perms]
    :end-before: [END security_op_perms]

Custom Roles
'''''''''''''

DAG Level Role
^^^^^^^^^^^^^^
``Admin`` can create a set of roles which are only allowed to view a certain set of DAGs. This is called DAG level access. Each DAG defined in the DAG model table
is treated as a ``View`` which has two permissions associated with it (``can_read`` and ``can_edit``. ``can_dag_read`` and ``can_dag_edit`` are deprecated since 2.0.0).
There is a special view called ``DAGs`` (it was called ``all_dags`` in versions 1.10.*) which
allows the role to access all the DAGs. The default ``Admin``, ``Viewer``, ``User``, ``Op`` roles can all access ``DAGs`` view.

.. image:: /img/add-role.png
.. image:: /img/new-role.png

The image shows the creation of a role which can only write to
``example_python_operator``. You can also create roles via the CLI
using the ``airflow roles create`` command, e.g.:

.. code-block:: bash

  airflow roles create Role1 Role2

And we could assign the given role to a new user using the ``airflow
users add-role`` CLI command.


Permissions
'''''''''''

Resource-Based permissions
^^^^^^^^^^^^^^^^^^^^^^^^^^

Starting with version 2.0, permissions are based on individual resources and a small subset of actions on those
resources. Resources match standard Airflow concepts, such as ``Dag``, ``DagRun``, ``Task``, and
``Connection``. Actions include ``can_create``, ``can_read``, ``can_edit``, and ``can_delete``.

Permissions (each consistent of a resource + action pair) are then added to roles.

**To access an endpoint, the user needs all permissions assigned to that endpoint**

There are five default roles: Public, Viewer, User, Op, and Admin. Each one has the permissions of the preceding role, as well as additional permissions.

DAG-level permissions
^^^^^^^^^^^^^^^^^^^^^

For DAG-level permissions exclusively, access can be controlled at the level of all DAGs or individual DAG objects. This includes ``DAGs.can_read``, ``DAGs.can_edit``, and ``DAGs.can_delete``. When these permissions are listed, access is granted to users who either have the listed permission or the same permission for the specific DAG being acted upon. For individual DAGs, the resource name is ``DAG:`` + the DAG ID.

For example, if a user is trying to view DAG information for the ``example_dag_id``, and the endpoint requires ``DAGs.can_read`` access, access will be granted if the user has either ``DAGs.can_read`` or ``DAG:example_dag_id.can_read`` access.

================================================================================== ====== ================================================================= ============
Stable API Permissions
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Endpoint                                                                           Method Permissions                                                       Minimum Role
================================================================================== ====== ================================================================= ============
/config                                                                            GET    Configurations.can_read                                           Op
/connections                                                                       GET    Connections.can_read                                              Op
/connections                                                                       POST   Connections.can_create                                            Op
/connections/{connection_id}                                                       DELETE Connections.can_delete                                            Op
/connections/{connection_id}                                                       PATCH  Connections.can_edit                                              Op
/connections/{connection_id}                                                       GET    Connections.can_read                                              Op
/dagSources/{file_token}                                                           GET    DAG Code.can_read                                                 Viewer
/dags                                                                              GET    DAGs.can_read                                                     Viewer
/dags/{dag_id}                                                                     GET    DAGs.can_read                                                     Viewer
/dags/{dag_id}                                                                     PATCH  DAGs.can_edit                                                     User
/dags/{dag_id}/clearTaskInstances                                                  POST   DAGs.can_edit, DAG Runs.can_read, Task Instances.can_edit         User
/dags/{dag_id}/details                                                             GET    DAGs.can_read                                                     Viewer
/dags/{dag_id}/tasks                                                               GET    DAGs.can_read, Task Instances.can_read                            Viewer
/dags/{dag_id}/tasks/{task_id}                                                     GET    DAGs.can_read, Task Instances.can_read                            Viewer
/dags/{dag_id}/dagRuns                                                             GET    DAGs.can_read, DAG Runs.can_read                                  Viewer
/dags/{dag_id}/dagRuns                                                             POST   DAGs.can_edit, DAG Runs.can_create                                User
/dags/{dag_id}/dagRuns/{dag_run_id}                                                DELETE DAGs.can_edit, DAG Runs.can_delete                                User
/dags/{dag_id}/dagRuns/{dag_run_id}                                                GET    DAGs.can_read, DAG Runs.can_read                                  Viewer
/dags/~/dagRuns/list                                                               POST   DAGs.can_edit, DAG Runs.can_read                                  User
/eventLogs                                                                         GET    Audit Logs.can_read                                               Viewer
/eventLogs/{event_log_id}                                                          GET    Audit Logs.can_read                                               Viewer
/importErrors                                                                      GET    ImportError.can_read                                              Viewer
/importErrors/{import_error_id}                                                    GET    ImportError.can_read                                              Viewer
/health                                                                            GET    None                                                              Public
/version                                                                           GET    None                                                              Public
/pools                                                                             GET    Pool.can_read                                                     Op
/pools                                                                             POST   Pool.can_create                                                   Op
/pools/{pool_name}                                                                 DELETE Pool.can_delete                                                   Op
/pools/{pool_name}                                                                 GET    Pool.can_read                                                     Op
/pools/{pool_name}                                                                 PATCH  Pool.can_edit                                                     Op
/providers                                                                         GET    Provider.can_read                                                 Op
/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances                                  GET    DAGs.can_read, DAG Runs.can_read, Task Instances.can_read         Viewer
/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}                        GET    DAGs.can_read, DAG Runs.can_read, Task Instances.can_read         Viewer
/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/links                  GET    DAGs.can_read, DAG Runs.can_read, Task Instances.can_read         Viewer
/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{task_try_number} GET    DAGs.can_read, DAG Runs.can_read, Task Instances.can_read         Viewer
/dags/~/dagRuns/~/taskInstances/list                                               POST   DAGs.can_edit, DAG Runs.can_read, Task Instances.can_read         User
/variables                                                                         GET    Variables.can_read                                                Op
/variables                                                                         POST   Variables.can_create                                              Op
/variables/{variable_key}                                                          DELETE Variables.can_delete                                              Op
/variables/{variable_key}                                                          GET    Variables.can_read                                                Op
/variables/{variable_key}                                                          PATCH  Variables.can_edit                                                Op
/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries            GET    DAGs.can_read, DAG Runs.can_read,                                 Viewer
                                                                                          Task Instances.can_read, XComs.can_read
/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key} GET    DAGs.can_read, DAG Runs.can_read,                                 Viewer
                                                                                          Task Instances.can_read, XComs.can_read
/users                                                                             GET    Users.can_read                                                    Admin
/users/{username}                                                                  GET    Users.can_read                                                    Admin
================================================================================== ====== ================================================================= ============


====================================== ======================================================================= ============
Website Permissions
-------------------------------------- ------------------------------------------------------------------------------------
Action                                 Permissions                                                             Minimum Role
====================================== ======================================================================= ============
Access homepage                        Website.can_read                                                        Viewer
Get DAG stats                          DAGs.can_read, DAG Runs.can_read                                        Viewer
Get Task stats                         DAGs.can_read, DAG Runs.can_read, Task Instances.can_read               Viewer
Get last DAG runs                      DAGs.can_read, DAG Runs.can_read                                        Viewer
Get DAG code                           DAGs.can_read, DAG Code.can_read                                        Viewer
Get DAG details                        DAGs.can_read, DAG Runs.can_read                                        Viewer
Get rendered DAG                       DAGs.can_read, Task Instances.can_read                                  Viewer
Get Logs with metadata                 DAGs.can_read, Task Instances.can_read, Task Logs.can_read              Viewer
Get Log                                DAGs.can_read, Task Instances.can_read, Task Logs.can_read              Viewer
Redirect to external Log               DAGs.can_read, Task Instances.can_read, Task Logs.can_read              Viewer
Get Task                               DAGs.can_read, Task Instances.can_read                                  Viewer
Get XCom                               DAGs.can_read, Task Instances.can_read, XComs.can_read                  Viewer
Triggers Task Instance                 DAGs.can_edit, Task Instances.can_create                                User
Delete DAG                             DAGs.can_delete                                                         User
Trigger DAG run                        DAGs.can_edit, DAG Runs.can_create                                      User
Clear DAG                              DAGs.can_edit, Task Instances.can_delete                                User
Clear DAG Run                          DAGs.can_edit, Task Instances.can_delete                                User
Mark DAG as blocked                    DAGS.can_edit, DAG Runs.can_read                                        User
Mark DAG Run as failed                 DAGS.can_edit, DAG Runs.can_edit                                        User
Mark DAG Run as success                DAGS.can_edit, DAG Runs.can_edit                                        User
Mark Task as failed                    DAGs.can_edit, Task Instances.can_edit                                  User
Mark Task as success                   DAGs.can_edit, Task Instances.can_edit                                  User
Get DAG as tree                        DAGs.can_read, Task Instances.can_read,                                 Viewer
                                       Task Logs.can_read
Get DAG as graph                       DAGs.can_read, Task Instances.can_read,                                 Viewer
                                       Task Logs.can_read
Get DAG as duration graph              DAGs.can_read, Task Instances.can_read                                  Viewer
Show all tries                         DAGs.can_read, Task Instances.can_read                                  Viewer
Show landing times                     DAGs.can_read, Task Instances.can_read                                  Viewer
Toggle DAG paused status               DAGs.can_edit                                                           User
Show Gantt Chart                       DAGs.can_read, Task Instances.can_read                                  Viewer
Get external links                     DAGs.can_read, Task Instances.can_read                                  Viewer
Show Task Instances                    DAGs.can_read, Task Instances.can_read                                  Viewer
Show Configs                           Configurations.can_read                                                 Viewer
Delete multiple records                DAGs.can_edit                                                           User
Set Task Instance as running           DAGs.can_edit                                                           User
Set Task Instance as failed            DAGs.can_edit                                                           User
Set Task Instance as success           DAGs.can_edit                                                           User
Set Task Instance as up_for_retry      DAGs.can_edit                                                           User
Autocomplete                           DAGs.can_read                                                           Viewer
List Logs                              Audit Logs.can_read                                                     Viewer
List Jobs                              Jobs.can_read                                                           Viewer
List SLA Misses                        SLA Misses.can_read                                                     Viewer
List Plugins                           Plugins.can_read                                                        Viewer
List Task Reschedules                  Task Reschedules.can_read                                               Admin
List Triggers                          Triggers.can_read                                                       Admin
====================================== ======================================================================= ============

These DAG-level controls can be set directly through the UI / CLI, or encoded in the dags themselves through the access_control arg.

Order of precedence for DAG-level permissions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Since DAG-level access control can be configured in multiple places, conflicts are inevitable and a clear resolution strategy is required. As a result,
Airflow considers the ``access_control`` argument supplied on a DAG itself to be completely authoritative if present, which has a few effects:

Setting ``access_control`` on a DAG will overwrite any previously existing DAG-level permissions if it is any value other than ``None``:

.. code-block:: python

    DAG(
        dag_id="example_fine_grained_access",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        access_control={
            "Viewer": {"can_edit", "can_create", "can_delete"},
        },
    )

This also means that setting ``access_control={}`` will wipe any existing DAG-level permissions for a given DAG from the DB:

.. code-block:: python

    DAG(
        dag_id="example_no_fine_grained_access",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        access_control={},
    )

Conversely, removing the access_control block from a DAG altogether (or setting it to ``None``) won't make any changes and can leave dangling permissions.

.. code-block:: python

    DAG(
        dag_id="example_indifferent_to_fine_grained_access",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    )

In the case that there is no ``access_control`` defined on the DAG itself, Airflow will defer to existing permissions defined in the DB, which
may have been set through the UI, CLI or by previous access_control args on the DAG in question.

In all cases, system-wide roles such as ``Can edit on DAG`` take precedence over dag-level access controls, such that they can be considered ``Can edit on DAG: *``

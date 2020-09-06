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

.. contents::
  :depth: 1
  :local:

Default Roles
'''''''''''''
Airflow ships with a set of roles by default: Admin, User, Op, Viewer, and Public.
Only ``Admin`` users could configure/alter the permissions for other roles. But it is not recommended
that ``Admin`` users alter these default roles in any way by removing
or adding permissions to these roles.

Admin
^^^^^
``Admin`` users have all possible permissions, including granting or revoking permissions from
other users.

Public
^^^^^^
``Public`` users (anonymous) don't have any permissions.

Viewer
^^^^^^
``Viewer`` users have limited viewer permissions

.. exampleinclude:: /../airflow/www/security.py
    :language: python
    :start-after: [START security_viewer_perms]
    :end-before: [END security_viewer_perms]

on limited web views

.. exampleinclude:: /../airflow/www/security.py
    :language: python
    :start-after: [START security_viewer_vms]
    :end-before: [END security_viewer_vms]


User
^^^^
``User`` users have ``Viewer`` permissions plus additional user permissions

.. exampleinclude:: /../airflow/www/security.py
    :language: python
    :start-after: [START security_user_perms]
    :end-before: [END security_user_perms]

on User web views which is the same as Viewer web views.

Op
^^
``Op`` users have ``User`` permissions plus additional op permissions

.. exampleinclude:: /../airflow/www/security.py
    :language: python
    :start-after: [START security_op_perms]
    :end-before: [END security_op_perms]

on ``User`` web views plus these additional op web views

.. exampleinclude:: /../airflow/www/security.py
    :language: python
    :start-after: [START security_op_vms]
    :end-before: [END security_op_vms]


Custom Roles
'''''''''''''

DAG Level Role
^^^^^^^^^^^^^^
``Admin`` can create a set of roles which are only allowed to view a certain set of dags. This is called DAG level access. Each dag defined in the dag model table
is treated as a ``View`` which has two permissions associated with it (``can_dag_read`` and ``can_dag_edit``). There is a special view called ``all_dags`` which
allows the role to access all the dags. The default ``Admin``, ``Viewer``, ``User``, ``Op`` roles can all access ``all_dags`` view.

Add a new role
''''''''''''''

To configure a new role, go to **Security** tab and click **List Roles** in the new UI.

.. image:: /img/add-role.png
.. image:: /img/new-role.png

The image shows the creation of a role which can only write to
``example_python_operator``. You can also create roles via the CLI
using the ``airflow roles create`` command, e.g.:

.. code-block:: bash

  airflow roles create Role1 Role2

And we could assign the given role to a new user using the ``airflow
users add-role`` CLI command.

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

Simple auth manager
===================

.. note::
    Before reading this, you should be familiar with the concept of auth manager.
    See :doc:`/core-concepts/auth-manager/index`.

.. warning::
  The simple auth manager is intended to be used for development and testing purposes. It should not be used in a production environment.

The simple auth manager is the auth manager that comes by default in Airflow 3. As its name suggests,
the logic and implementation of the simple auth manager is **simple**.

Manage users
------------

Users are managed through the Airflow configuration. Example:

.. code-block:: ini

  [core]
  simple_auth_manager_users = "bob:admin,peter:viewer"

The list of users are separated with a comma and each user is a couple username/role separated by a colon.
Each user needs two pieces of information:

* **username**. The user's username
* **role**. The role associated to the user. For more information about these roles, :ref:`see next section <roles-permissions>`.

In the example above, two users are defined:

* **bob** whose role is **admin**
* **peter** whose role is **viewer**

The password is auto-generated for each user and printed out in the webserver logs.
When generated, these passwords are also saved in your environment, therefore they will not change if you stop or restart your environment.

The passwords are saved in the file ``generated/simple_auth_manager_passwords.json.generated``, you can read and update them directly in the file as well if desired.

.. _roles-permissions:

Manage roles and permissions
----------------------------

There is no option to manage roles and permissions in simple auth manager. They are defined as part of the simple auth manager implementation and cannot be modified.
Here is the list of roles defined in simple auth manager. These roles can be associated to users.

* **viewer**. Read-only permissions on DAGs, assets and pools
* **user**. **viewer** permissions plus all permissions (edit, create, delete) on DAGs
* **op**. **user** permissions plus all permissions on pools, assets, config, connections and variables
* **admin**. All permissions

Optional features
-----------------

Disable authentication and allow everyone as admin
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This option allow you to disable authentication and allow everyone as admin.
As a consequence, whoever access the Airflow UI is automatically logged in as an admin with all permissions.

you can enable this feature through the config. Example:

.. code-block:: ini

  [core]
  simple_auth_manager_all_admins = "True"

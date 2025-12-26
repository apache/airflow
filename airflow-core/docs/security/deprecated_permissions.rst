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

Deprecation Notice for airflow.security.permissions
===================================================

Since the release of Airflow 3, the Flask AppBuilder (AKA "FAB") provider is
`no longer a core Airflow dependency <https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-79%3A+Remove+Flask+AppBuilder+as+Core+dependency>`_.
However, some modules specifically designed for :doc:`apache-airflow-providers-fab:auth-manager/index` remain in the core Airflow distribution as a
backwards-compatible convenience for Airflow users. One such module which remains in the core distribution for backwards compatibility purposes is ``airflow.security.permissions``

If your deployment depends on ``airflow.security.permissions`` for any custom role definitions, or for any custom Auth Manager logic --
regardless of whether you use the FAB Auth Manager or some other Auth Manager -- you should transition
to the new authorization standard definitions for resources and actions.
The deprecated ``airflow.security.permissions`` will be removed in Airflow 4.

Does this Deprecation Affect My Airflow System?
-----------------------------------------------

Generally speaking, this deprecation warning applies to any Airflow system in which **either** of the following is true:

* The Airflow deployment relies on ``airflow.security.permissions`` to define custom RBAC roles.
* The Airflow deployment has other custom logic which relies on ``airflow.security.permissions``, including any custom :doc:`/core-concepts/auth-manager/index` dependencies.

However, if you rely on the **unmodified** :doc:`apache-airflow-providers-fab:auth-manager/index` and you **do not** use any custom role definitions, then the rest of this doc does not apply to you.
Similarly, if you rely on the :doc:`/core-concepts/auth-manager/simple/index` or any of the other provider Auth Managers, and have no custom code using ``airflow.security.permissions``, then the rest of this doc does not apply to you.

.. note::
    Each customized Airflow RBAC setup differs on a case-by-case basis. As such, this doc can only provide general
    guidance for the transition to the new Airflow authorization standards, without being overly prescriptive.

How to Migrate to the New Authorization Standard Definitions
------------------------------------------------------------

Refer to the list below for the deprecated permissions module components, and the corresponding supported
replacement available from Airflow core:

* ``airflow.security.permissions.ACTION_*`` --> ``airflow.api_fastapi.auth.managers.base_auth_manager.ResourceMethod``
* ``airflow.security.permissions.RESOURCE_*`` --> ``airflow.api_fastapi.auth.managers.models.resource_details``
* ``DAG.access_control`` --> Dag-level permissions should be handled by the chosen Auth Manager's ``filter_authorized_dag_ids`` method.

If you maintain a custom :doc:`/core-concepts/auth-manager/index` which relies on the deprecated module, it is
recommended you refer to the ``SimpleAuthManager``'s `source code <https://github.com/apache/airflow/blob/main/airflow-core/src/airflow/api_fastapi/auth/managers/simple/simple_auth_manager.py>`_
as an example for how you might use the ``ResourceMethod`` and ``resource_details`` components.

If you rely on custom role definitions based off the deprecated module, you should refer to the documentation of the auth manager your system uses.

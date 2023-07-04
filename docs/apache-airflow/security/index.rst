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

Security
========

.. toctree::
    :maxdepth: 1
    :glob:

    *
    secrets/index


This document describes Airflow's security model from the perspective of
the Airflow user. It is intended to help users understand the security
model and make informed decisions about how to deploy and manage Airflow.
If you would like to know how to report security vulnerabilities and how
security reports are handled by the security team of Airflow, head to
`Airflow's Security Policy <https://github.com/apache/airflow/security/policy>`_.

Airflow security model - user types
-----------------------------------

The Airflow security model involves different types of users with
varying access and capabilities:

1. **Deployment Managers**: They have the highest level of access and
   control. They install and configure Airflow, make decisions about
   technologies and permissions. They can potentially delete the entire
   installation and have access to all credentials. Deployment Managers
   can also decide to keep audits, backups and copies of information
   outside of Airflow, which are not covered by Airflow's security
   model.

2. **DAG Authors**: They can upload, modify, and delete DAG files. The
   code in DAG files is executed on workers. Therefore, DAG authors can create
   and change code executed on workers and potentially access the credentials
   that DAG code uses to access external systems. DAG Authors have full access
   to the metadata database and internal audit logs.

3. **Authenticated UI users**: They have access to the UI and API. Admin
   users can manage permissions and execute code on workers. Connection
   configuration users can configure connections and execute code on
   workers. Operations users have access to DAG execution status. Trust
   is crucial to prevent abuse and Denial of Service attacks.

4. **Non-authenticated UI users**: Airflow doesn't support
   unauthenticated users by default. If allowed, vulnerabilities must be
   addressed by the Deployment Manager.

Capabilities of authenticated UI users
--------------------------------------

The capabilities of **Authenticated UI users** can vary depending on
what roles have been configured by the Deployment Manager or Admin users as well as what permissions those roles have. Permissions on roles can be scoped as tightly as a single DAG, for example, or as broad as Admin. Below are three general categories to help conceptualize some of the capabilities authenticated users may have:

1. **Admin users**: They manage and grant permissions to other users,
   with full access to all UI capabilities. They can potentially execute
   code on workers by configuring connections and need to be trusted not
   to abuse these privileges. They have access to sensitive credentials
   and can modify them. By default, they don't have access to
   system-level configuration. They should be trusted not to misuse
   sensitive information accessible through connection configuration.
   They also have the ability to create a Webserver Denial of Service
   situation and should be trusted not to misuse this capability.

2. **Connection configuration users**: They configure connections and
   potentially execute code on workers during DAG execution. Trust is
   required to prevent misuse of these privileges. They have full access
   to sensitive credentials stored in connections and can modify them.
   Access to sensitive information through connection configuration
   should be trusted not to be abused. They also have the ability to
   create a Webserver Denial of Service situation and should be trusted
   not to misuse this capability.

3. **Operations users**: They have access to DAG execution status via
   the UI. Currently, Airflow lacks full protection for accessing groups
   of DAGs' history and execution. They can perform actions such as
   clearing, re-running, triggering DAGs, and changing parameters.
   Depending on access restrictions, they may also have access to
   editing variables and viewing Airflow configuration. They should not
   have access to sensitive system-level information or connections, and
   they should not be able to access sensitive task information unless
   deliberately exposed in logs by DAG authors. They should be trusted
   not to abuse their privileges, as they can potentially overload the
   server and cause Denial of Service situations.

Responsibilities of Deployment Managers
---------------------------------------

Deployment Managers determine access levels and understand the potential
damage users can cause. Some Deployment Managers may further limit
access through fine-grained privileges for the **Authenticated UI
users**. However, these limitations are outside the basic Airflow's
security model and are at the discretion of Deployment Managers.
Examples of fine-grained access control include (but are not limited
to):

-  Limiting login permissions: Restricting the accounts that users can
   log in with, allowing only specific accounts or roles belonging to
   access the Airflow system.

-  Access restrictions to views or DAGs: Controlling user access to
   certain views or specific DAGs, ensuring that users can only view or
   interact with authorized components.

-  Implementing static code analysis and code review: Introducing
   processes such as static code analysis and code review as part of the
   DAG submission pipeline. This helps enforce code quality standards,
   security checks, and adherence to best practices before DAGs are
   deployed.

These examples showcase ways in which administrators can refine and
limit user privileges within Airflow, providing tighter control and
ensuring that users have access only to the necessary components and
functionalities based on their roles and responsibilities, however the
fine-grained access control does not provide full isolation and
separation of access allowing isolation of different user groups in a
multi-tenant fashion yet. In the future versions of Airflow some of the
fine-grained access features might become part of the Airflow security
model. The Airflow community is working on a multi-tenant model that might
address some of the fine-grained access components.


Releasing Airflow with security patches
---------------------------------------

Apache Airflow uses a strict [SemVer](https://semver.org) versioning policy, which means that we strive for
any release of a given ``MAJOR`` Version (version "2" currently) to be backwards compatible. When we
release a ``MINOR`` version, the development continues in the ``main`` branch where we prepare the next
``MINOR`` version, but we release ``PATCHLEVEL`` releases with selected bugfixes (including security
bugfixes) cherry-picked to the latest released ``MINOR`` line of Apache Airflow. At the moment, when we
release a new ``MINOR`` version, we stop releasing ``PATCHLEVEL`` releases for the previous ``MINOR`` version.

For example, once we released ``2.6.0`` version on April 30, 2023 all the security patches will be cherry-picked and released in ``2.6.*`` versions until we release ``2.7.0`` version. There will be no
``2.5.*`` versions  released after ``2.6.0`` has been released.

This means that in order to apply security fixes with Apache Airflow software released by us, you
MUST upgrade to the latest ``MINOR`` version of Airflow.

Releasing Airflow providers with security patches
-------------------------------------------------

Similarly to Airflow, providers use strict `SemVer <https://semver.org>`_ versioning policy, and the same
policies apply for providers as for Airflow itself. This means that you need to upgrade to the latest
``MINOR`` version of the provider to get the latest security fixes.
Airflow providers are released independently from Airflow itself and the information about vulnerabilities
is published separately. You can upgrade providers independently from Airflow itself, following the
instructions found in :ref:`installing-from-pypi-managing-providers-separately-from-airflow-core`.

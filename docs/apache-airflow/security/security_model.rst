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

Airflow Security Model
======================

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
   control. They install and configure Airflow, and make decisions about
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
   unauthenticated users by default. If allowed, potential vulnerabilities
   must be assessed and addressed by the Deployment Manager.

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

Deployment Managers determine access levels and must understand the potential
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

These examples showcase ways in which Deployment Managers can refine and
limit user privileges within Airflow, providing tighter control and
ensuring that users have access only to the necessary components and
functionalities based on their roles and responsibilities. However,
fine-grained access control does not provide full isolation and
separation of access to allow isolation of different user groups in a
multi-tenant fashion yet. In future versions of Airflow, some
fine-grained access control features could become part of the Airflow security
model, as the Airflow community is working on a multi-tenant model currently.

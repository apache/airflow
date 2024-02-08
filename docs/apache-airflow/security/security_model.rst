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

The Airflow security model involves different types of users with varying access and capabilities:

While - in smaller installations - all the actions related to Airflow can be performed by a single user,
in larger installations it is apparent that there different responsibilities, roles and
capabilities that need to be separated.

This is why Airflow has the following user types:

* Deployment Managers - overall responsible for the Airflow installation, security and configuration
* Authenticated UI users - users that can access Airflow UI and API and interact with it
* DAG Authors - responsible for creating DAGs and submitting them to Airflow

You can see more on how the user types influence Airflow's architecture in :doc:`/core-concepts/overview`,
including, seeing the diagrams of less and more complex deployments.




Deployment Managers
...................

They have the highest level of access and
control. They install and configure Airflow, and make decisions about
technologies and permissions. They can potentially delete the entire
installation and have access to all credentials. Deployment Managers
can also decide to keep audits, backups and copies of information
outside of Airflow, which are not covered by Airflow's security
model.

DAG Authors
...........

They can upload, modify, and delete DAG files. The
code in DAG files is executed on workers and in the DAG File Processor. Note
that in the simple deployment configuration, parsing DAGs is executed as
a subprocess of the Scheduler process, but with Standalone DAG File Processor
deployment managers might separate parsing DAGs from the Scheduler process.
Therefore, DAG authors can create and change code executed on workers
and the DAG File Processor and potentially access the credentials that the DAG
code uses to access external systems. DAG Authors have full access
to the metadata database and internal audit logs.

Authenticated UI users
.......................

They have access to the UI and API. See below for more details on the capabilities
authenticated UI users may have.

Non-authenticated UI users
..........................

Airflow doesn't support unauthenticated users by default. If allowed, potential vulnerabilities
must be assessed and addressed by the Deployment Manager.

Capabilities of authenticated UI users
--------------------------------------

The capabilities of **Authenticated UI users** can vary depending on
what roles have been configured by the Deployment Manager or Admin users
as well as what permissions those roles have. Permissions on roles can be
scoped as tightly as a single DAG, for example, or as broad as Admin.
Below are four general categories to help conceptualize some of the
capabilities authenticated users may have:

Admin users
...........

They manage and grant permissions to other users,
with full access to all UI capabilities. They can potentially execute
code on workers by configuring connections and need to be trusted not
to abuse these privileges. They have access to sensitive credentials
and can modify them. By default, they don't have access to
system-level configuration. They should be trusted not to misuse
sensitive information accessible through connection configuration.
They also have the ability to create a Webserver Denial of Service
situation and should be trusted not to misuse this capability.

Operations users
................

The primary difference between an operator and admin is the ability to manage and grant permissions
to other users - only admins are able to do this. Otherwise assume they have the same access as an admin.

Connection configuration users
..............................

They configure connections and potentially execute code on workers during DAG execution. Trust is
required to prevent misuse of these privileges. They have full access
to sensitive credentials stored in connections and can modify them.
Access to sensitive information through connection configuration
should be trusted not to be abused. They also have the ability to
create a Webserver Denial of Service situation and should be trusted
not to misuse this capability.

Audit log users
...............

They can view audit events for the whole Airflow installation.

Regular users
.............

They can view and interact with the UI and API. They are able to view and edit DAGs,
task instances, and DAG runs, and view task logs.

For more information on the capabilities of authenticated UI users, see :doc:`apache-airflow-providers-fab:auth-manager/access-control`.

Capabilities of DAG Authors
---------------------------

DAG authors are able to submit code - via Python files placed in the DAGS_FOLDER - that will be executed
in a number of circumstances. The code to execute is neither verified, checked nor sand-boxed by Airflow
(that would be very difficult if not impossible to do), so effectively DAG authors can execute arbitrary
code on the workers (part of Celery Workers for Celery Executor, local processes run by scheduler in case
of Local Executor, Task Kubernetes POD in case of Kubernetes Executor), in the DAG File Processor
(which can be either executed as standalone process or can be part of the Scheduler) and in the Triggerer.

There are several consequences of this model chosen by Airflow, that deployment managers need to be aware of:

Local executor and built-in DAG File Processor
..............................................

In case of Local Executor and DAG File Processor running as part of the Scheduler, DAG authors can execute
arbitrary code on the machine where scheduler is running. This means that they can affect the scheduler
process itself, and potentially affect the whole Airflow installation - including modifying cluster-wide
policies and changing Airflow configuration. If you are running Airflow with one of those settings,
the Deployment Manager must trust the DAG authors not to abuse this capability.

Celery Executor
...............

In case of Celery Executor, DAG authors can execute arbitrary code on the Celery Workers. This means that
they can potentially influence all the tasks executed on the same worker. If you are running Airflow with
Celery Executor, the Deployment Manager must trust the DAG authors not to abuse this capability and unless
Deployment Manager separates task execution by queues by Cluster Policies, they should assume, there is no
isolation between tasks.

Kubernetes Executor
...................

In case of Kubernetes Executor, DAG authors can execute arbitrary code on the Kubernetes POD they run. Each
task is executed in a separate POD, so there is already isolation between tasks as generally speaking
Kubernetes provides isolation between PODs.

Triggerer
.........

In case of Triggerer, DAG authors can execute arbitrary code in Triggerer. Currently there are no
enforcement mechanisms that would allow to isolate tasks that are using deferrable functionality from
each other and arbitrary code from various tasks can be executed in the same process/machine. Deployment
Manager must trust that DAG authors will not abuse this capability.

DAG files not needed for Scheduler and Webserver
................................................

The Deployment Manager might isolate the code execution provided by DAG authors - particularly in
Scheduler and Webserver by making sure that the Scheduler and Webserver don't even
have access to the DAG Files (that requires standalone DAG File Processor to be deployed). Generally
speaking - no DAG author provided code should ever be executed in the Scheduler or Webserver process.

Allowing DAG authors to execute selected code in Scheduler and Webserver
........................................................................

There are a number of functionalities that allow the DAG author to use pre-registered custom code to be
executed in scheduler or webserver process - for example they can choose custom Timetables, UI plugins,
Connection UI Fields, Operator extra links, macros, listeners - all of those functionalities allow the
DAG author to choose the code that will be executed in the scheduler or webserver process. However this
should not be arbitrary code that DAG author can add in DAG folder. All those functionalities are
only available via ``plugins`` and ``providers`` mechanisms where the code that is executed can only be
provided by installed packages (or in case of plugins it can also be added to PLUGINS folder where DAG
authors should not have write access to). PLUGINS FOLDER is a legacy mechanism coming from Airflow 1.10
- but we recommend using entrypoint mechanism that allows the Deployment Manager to - effectively -
choose and register the code that will be executed in those contexts. DAG Author has no access to
install or modify packages installed in Webserver and Scheduler, and this is the way to prevent
the DAG Author to execute arbitrary code in those processes.

The Deployment Manager might decide to introduce additional control mechanisms to prevent DAG authors from
executing arbitrary code. This is all fully in hands of the Deployment Manager and it is discussed in the
following chapter.

Access to All DAGs
........................................................................

All DAG authors have access to all DAGs in the airflow deployment. This means that they can view, modify,
and update any DAG without restrictions at any time.

Responsibilities of Deployment Managers
---------------------------------------

As a Deployment Manager, you should be aware of the capabilities of DAG authors and make sure that
you trust them not to abuse the capabilities they have. You should also make sure that you have
properly configured the Airflow installation to prevent DAG authors from executing arbitrary code
in the Scheduler and Webserver processes.

Deploying and protecting Airflow installation
.............................................

Deployment Managers are also responsible for deploying airflow and make it accessible to the users
in the way that follows best practices of secure deployment applicable to the organization where
Airflow is deployed. This includes but is not limited to:

* protecting communication using TLS/VPC and whatever network security is required by the organization
  that is deploying Airflow
* applying rate-limiting and other forms of protections that is usually applied to web applications
* applying authentication and authorization to the web application so that only known and authorized
  users can have access to Airflow
* any kind of detection of unusual activity and protection against it
* choosing the right session backend and configuring it properly including timeouts for the session

Limiting DAG Author capabilities
.................................

The Deployment Manager might also use additional mechanisms to prevent DAG authors from executing
arbitrary code - for example they might introduce tooling around DAG submission that would allow
to review the code before it is deployed, statically-check it and add other ways to prevent malicious
code to be submitted. The way how submitting code to DAG folder is done and protected is completely
up to the Deployment Manager - Airflow does not provide any tooling or mechanisms around it and it
expects that the Deployment Manager will provide the tooling to protect access to the DAG folder and
make sure that only trusted code is submitted there.

Airflow does not implement any of those feature natively, and delegates it to the deployment managers
to deploy all the necessary infrastructure to protect the deployment - as external infrastructure components.

Limiting access for authenticated UI users
...........................................

Deployment Managers also determine access levels and must understand the potential damage users can cause.
Some Deployment Managers may further limit access through fine-grained privileges for the **Authenticated UI
users**. However, these limitations are outside the basic Airflow's security model and are at the
discretion of Deployment Managers.

Examples of fine-grained access control include (but are not limited to):

*  Limiting login permissions: Restricting the accounts that users can log in with, allowing only specific
   accounts or roles belonging to access the Airflow system.

*  Access restrictions to views or DAGs: Controlling user access to certain views or specific DAGs,
   ensuring that users can only view or interact with authorized components.

Future: multi-tenancy isolation
...............................

These examples showcase ways in which Deployment Managers can refine and limit user privileges within Airflow,
providing tighter control and ensuring that users have access only to the necessary components and
functionalities based on their roles and responsibilities. However, fine-grained access control does not
provide full isolation and separation of access to allow isolation of different user groups in a
multi-tenant fashion yet. In future versions of Airflow, some fine-grained access control features could
become part of the Airflow security model, as the Airflow community is working on a multi-tenant model
currently.

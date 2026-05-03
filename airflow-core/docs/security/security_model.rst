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
* Dag authors - responsible for creating Dags and submitting them to Airflow

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

Dag authors
...........

They can create, modify, and delete Dag files. The
code in Dag files is executed on workers, in the Dag File Processor,
and in the Triggerer.
Therefore, Dag authors can create and change code executed on workers,
the Dag File Processor, and the Triggerer, and potentially access the credentials that the Dag
code uses to access external systems.

In Airflow 3, the level of database isolation depends on the component:

* **Workers**: Task code on workers communicates with the API server exclusively through the
  Execution API. Workers do not receive database credentials and genuinely cannot access the
  metadata database directly.
* **Dag File Processor and Triggerer**: Airflow implements software guards that prevent
  accidental direct database access from Dag author code. However, because Dag parsing and
  trigger execution processes run as the same Unix user as their parent processes (which do
  have database credentials), a deliberately malicious Dag author can potentially retrieve
  credentials from the parent process and gain direct database access. See
  :ref:`jwt-authentication-and-workload-isolation` for details on the specific mechanisms and
  deployment hardening measures.

Authenticated UI users
.......................

They have access to the UI and API. See below for more details on the capabilities
authenticated UI users may have.

Non-authenticated UI users
..........................

Airflow doesn't support unauthenticated users by default. If allowed, potential vulnerabilities
must be assessed and addressed by the Deployment Manager. However, there are exceptions to this.
The ``/health`` endpoint responsible to get health check updates should be publicly accessible.
This is because other systems would want to retrieve that information. Another exception is the
``/login`` endpoint, as the users are expected to be unauthenticated to use it.

Capabilities of authenticated UI users
--------------------------------------

The capabilities of **Authenticated UI users** can vary depending on
what roles have been configured by the Deployment Manager or Admin users
as well as what permissions those roles have. Permissions on roles can be
scoped as tightly as a single Dag, for example, or as broad as Admin.
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
They also have the ability to create a API Server Denial of Service
situation and should be trusted not to misuse this capability.

Only admin users have access to audit logs by default.

Operations users
................

The primary difference between an operator and admin is the ability to manage and grant permissions
to other users, and access audit logs - only admins are able to do this. Otherwise assume they have
the same access as an admin.

.. _connection-configuration-users:

Connection configuration users
..............................

They configure connections and potentially execute code on workers during Dag execution. Trust is
required to prevent misuse of these privileges. They have full write-only access
to sensitive credentials stored in connections and can modify them, but cannot view them.
Access to write sensitive information through connection configuration
should be trusted not to be abused. They also have the ability to configure connections wrongly
that might create a API Server Denial of Service situations and specify insecure connection options
which might create situations where executing Dags will lead to arbitrary Remote Code Execution
for some providers - either community released or custom ones.

Those users should be highly trusted not to misuse this capability.

.. note::

   Before Airflow 3, the **Connection configuration users** role had also access to view the sensitive information this has
   been changed in Airflow 3 to improve security of the accidental spilling of credentials of the connection configuration
   users. Previously - in Airflow 2 - the **Connection configuration users** had deliberately access to view the
   sensitive information and could either reveal it by using Inspect capabilities of the browser or they were plain visible in
   case of the sensitive credentials stored in configuration extras. Airflow 3 and later versions mask these sensitive credentials
   at the API level and do not return them in clear text.

About Sensitive information
...........................

Sensitive information consists of connection details, variables, and configuration. In versions later than Airflow 3.0
sensitive information will not be exposed to users via API, UI, and ``airflowctl``.
However, ``task-sdk`` still provides access to sensitive information (e.g., Use SDK API Client to get
Variables with task-specific ``JWT`` token). Local CLI will only return keys except when using ``--show_values``.
Sensitive information has been masked in logs, UI, and API outputs. In case of Dag author expose sensitive
information in other way (e.g., via environment variables), those values will not be masked.

Audit log users
...............

They can view audit events for the whole Airflow installation.

Regular users
.............

They can view and interact with the UI and API. They are able to view and edit Dags,
task instances, and Dag runs, and view task logs.

Viewer users
............

They can view information related to Dags, in a read only fashion, task logs, and other relevant details.
This role is suitable for users who require read-only access without the ability to trigger or modify Dags.

Viewers also do not have permission to access audit logs.

For more information on the capabilities of authenticated UI users, see
:doc:`apache-airflow-providers-fab:auth-manager/access-control`.

.. _capabilities-of-dag-authors:

Capabilities of Dag authors
---------------------------

Dag authors are able to create or edit code - via Python files placed in a Dag bundle - that will be executed
in a number of circumstances. The code to execute is neither verified, checked nor sand-boxed by Airflow
(that would be very difficult if not impossible to do), so effectively Dag authors can execute arbitrary
code on the workers (part of Celery Workers for Celery Executor, local processes run by scheduler in case
of Local Executor, Task Kubernetes POD in case of Kubernetes Executor), in the Dag Processor
and in the Triggerer.

Dag authors are responsible for the code they write and submit to Airflow, and they should be trusted to
verify that what they implement is safe code that will not cause any harm to the Airflow installation and
will not open way for security vulnerabilities. Since Dag Authors are writing Python code, they can easily write
code that will access sensitive information stored in Airflow or send it outside - but also to open up new
security vulnerabilities. Good example is writing a code that will pass non-sanitized UI user input (such as parameter,
variables, connection configuration) to any code in Operators and Hooks, or third party libraries without properly
sanitizing it first. This can open up windows for Remote Code Execution, Denial of Service vulnerabilities or similar.
Dag authors should be trusted not to write such code and to verify that the code they write is safe and does
not open new security vulnerabilities.

Limiting Dag Author access to subset of Dags
--------------------------------------------

Airflow does not yet provide full task-level isolation between different groups of users when
it comes to task execution. While, in Airflow 3.0 and later, worker task code cannot directly access the
metadata database (it communicates through the Execution API), Dag author code that runs in the Dag File
Processor and Triggerer potentially still has direct database access. Regardless of execution context, Dag authors
have access to all Dags in the Airflow installation and they can
modify any of those Dags - no matter which Dag the task code is executed for. This means that Dag authors can
modify state of any task instance of any Dag, and there are no finer-grained access controls to limit that access.

This applies to every interface a Dag author's code can reach — including the Task Execution API
that workers use via the Task SDK. The Execution JWT issued to a running task does **not** carry
per-Dag authorization: a task holding a valid token can call state-mutating Execution API endpoints
(triggering Dag runs, clearing Dag runs, reading or writing variables, connections and XComs, etc.)
for **any** Dag in the installation. The ``ti:self`` token scope restricts cross-task-instance state
mutation only; it is not a per-Dag access control.

There is an **experimental** multi-team feature in Airflow (``[core] multi_team``) that provides UI-level and
REST API-level RBAC isolation between teams. However, this feature **does not yet guarantee task-level isolation**.
At the task execution level, workloads from different teams still share the same Execution API, signing keys,
connections, and variables. A task from one team can access the same shared resources as a task from another team.
The multi-team feature is a work in progress — task-level isolation and Execution API enforcement of team
boundaries will be improved in future versions of Airflow. Until then, you should assume that all Dag authors
have access to all Dags and shared resources, and can modify their state regardless of team assignment.


Security contexts for Dag author submitted code
-----------------------------------------------

There are several consequences of this model chosen by Airflow, that deployment managers need to be aware of in
terms of how those capabilities of Dag authors map to executed code in different security contexts in Airflow:

Local executor
..............

In case of Local Executor, Dag authors can execute arbitrary code on the machine where scheduler is running.
This means that they can affect the scheduler process itself, and potentially affect the whole Airflow
installation - including modifying cluster-wide policies and changing Airflow configuration. If you are running
Airflow with Local Executor, the Deployment Manager must trust the Dag authors not to abuse this capability.

Celery Executor
...............

In case of Celery Executor, Dag authors can execute arbitrary code on the Celery Workers. This means that
they can potentially influence all the tasks executed on the same worker. If you are running Airflow with
Celery Executor, the Deployment Manager must trust the Dag authors not to abuse this capability and unless
Deployment Manager separates task execution by queues by Cluster Policies, they should assume, there is no
isolation between tasks.

Kubernetes Executor
...................

In case of Kubernetes Executor, Dag authors can execute arbitrary code on the Kubernetes POD they run. Each
task is executed in a separate POD, so there is already isolation between tasks as generally speaking
Kubernetes provides isolation between PODs.

Triggerer
.........

In case of Triggerer, Dag authors can execute arbitrary code in Triggerer. Currently there are no
enforcement mechanisms that would allow to isolate tasks that are using deferrable functionality from
each other and arbitrary code from various tasks can be executed in the same process/machine. The default
deployment runs a single Triggerer instance that handles triggers from all teams — there is no built-in
support for per-team Triggerer instances. Additionally, the Triggerer uses an in-process Execution API
transport that potentially bypasses JWT authentication and potentially has direct access to the metadata
database. For multi-team deployments, Deployment Managers must run separate Triggerer instances per team
as a deployment-level measure, but even then each instance potentially retains direct database access
and a Dag author
whose trigger code runs there can potentially access the database directly — including data belonging
to other teams. Deployment Manager must trust that Dag authors will not abuse this capability.

Dag files not needed for Scheduler and API Server
.................................................

The Deployment Manager might isolate the code execution provided by Dag authors - particularly in
Scheduler and API Server by making sure that the Scheduler and API Server don't even
have access to the Dag Files. Generally speaking - no Dag author provided code should ever be
executed in the Scheduler or API Server process. This means the deployment manager can exclude credentials
needed for Dag bundles on the Scheduler and API Server - but the bundles must still be configured on those
components.


Allowing Dag authors to execute selected code in Scheduler and API Server
.........................................................................

There are a number of functionalities that allow the Dag author to use pre-registered custom code to be
executed in the Scheduler or API Server process - for example they can choose custom Timetables, UI plugins,
Connection UI Fields, Operator extra links, macros, listeners - all of those functionalities allow the
Dag author to choose the code that will be executed in the Scheduler or API Server process. However this
should not be arbitrary code that Dag author can add Dag bundles. All those functionalities are
only available via ``plugins`` and ``providers`` mechanisms where the code that is executed can only be
provided by installed packages (or in case of plugins it can also be added to PLUGINS folder where Dag
authors should not have write access to). PLUGINS_FOLDER is a legacy mechanism coming from Airflow 1.10
- but we recommend using entrypoint mechanism that allows the Deployment Manager to - effectively -
choose and register the code that will be executed in those contexts. Dag author has no access to
install or modify packages installed in Scheduler and API Server, and this is the way to prevent
the Dag author to execute arbitrary code in those processes.

Additionally, if you decide to utilize and configure the PLUGINS_FOLDER, it is essential for the Deployment
Manager to ensure that the Dag author does not have write access to this folder.

The Deployment Manager might decide to introduce additional control mechanisms to prevent Dag authors from
executing arbitrary code. This is all fully in hands of the Deployment Manager and it is discussed in the
following chapter.

Access to all Dags
..................

All Dag authors have access to all Dags in the Airflow deployment. This means that they can view, modify,
and update any Dag without restrictions at any time.

.. _jwt-authentication-and-workload-isolation:

JWT authentication and workload isolation
-----------------------------------------

Airflow uses JWT (JSON Web Token) authentication for both its public REST API and its internal
Execution API. For a detailed description of the JWT authentication flows, token structure, and
configuration, see :doc:`/security/jwt_token_authentication`. For the current state of workload
isolation protections and their limitations, see :ref:`workload-isolation`.

Current isolation limitations
.............................

While Airflow 3 significantly improved the security model by preventing worker task code from
directly accessing the metadata database (workers now communicate exclusively through the
Execution API), **perfect isolation between Dag authors is not yet achieved**. Dag author code
potentially still executes with direct database access in the Dag File Processor and Triggerer.

**Software guards vs. intentional access**
   Airflow implements software-level guards that prevent **accidental and unintentional** direct database
   access from Dag author code. The Dag File Processor removes the database session and connection
   information before forking child processes that parse Dag files, and worker tasks use the Execution
   API exclusively.

   However, these software guards **do not protect against intentional, malicious access**. The child
   processes that parse Dag files and execute trigger code run as the **same Unix user** as their parent
   processes (the Dag File Processor manager and the Triggerer respectively). Because of how POSIX
   process isolation works, a child process running as the same user can retrieve the parent's
   credentials through several mechanisms:

   * **Environment variables**: By default, on Linux, any process can read ``/proc/<PID>/environ`` of another
     process running as the same user — so database credentials passed via environment variables
     (e.g., ``AIRFLOW__DATABASE__SQL_ALCHEMY_CONN``) can be read from the parent process. This can be
     prevented by setting dumpable property of the process which is implemented in supervisor of tasks.
   * **Configuration files**: If configuration is stored in files, those files must be readable by the
     parent process and are therefore also readable by the child process running as the same user.
   * **Command-based secrets** (``_CMD`` suffix options): The child process can execute the same
     commands to retrieve secrets.
   * **Secrets manager access**: If the parent uses a secrets backend, the child can access the same
     secrets manager using credentials available in the process environment or filesystem.

   This means that a deliberately malicious Dag author can retrieve database credentials and gain
   **full read/write access to the metadata database** — including the ability to modify any Dag,
   task instance, connection, or variable. The software guards address accidental access (e.g., a Dag
   author importing ``airflow.settings.Session`` out of habit from Airflow 2) but do not prevent a
   determined actor from circumventing them.

   On workers, the isolation can be stronger when Deployment Manager configures worker processes to
   not receive database credentials at all (neither via environment variables nor configuration).
   Workers should communicate exclusively through the Execution API using short-lived JWT tokens.
   A task running on a worker genuinely should not access the metadata database directly —
   when it is configured to not have any credentials accessible to it.

**Dag File Processor and Triggerer run user code only have soft protection to bypass JWT authentication**
   The Dag File Processor and Triggerer processes that run user code,
   use an in-process transport to access the Execution API, which bypasses JWT authentication.
   Since these components execute user-submitted code (Dag files and trigger code respectively),
   a Dag author whose code runs in these components
   has unrestricted access to all Execution API operations if they bypass the soft protections
   — including the ability to read any connection, variable, or XCom — without needing a valid JWT token.

   Furthermore, the Dag File Processor has direct access to the metadata database (it needs this to
   store serialized Dags). As described above, Dag author code executing in the Dag File Processor
   context could potentially retrieve the database credentials from the parent process and access
   the database directly, including the JWT signing key configuration if it is available in the
   process environment. If a Dag author obtains the JWT signing key, they could forge arbitrary tokens.

**Dag File Processor and Triggerer are shared across teams**
   In the default deployment, a **single Dag File Processor instance** parses all Dag files and a
   **single Triggerer instance** handles all triggers — regardless of team assignment. There is no
   built-in support for running per-team Dag File Processor or Triggerer instances. This means that
   Dag author code from different teams executes within the same process, potentially sharing the
   in-process Execution API and direct database access.

   For multi-team deployments that require separation, Deployment Managers must run **separate
   Dag File Processor and Triggerer instances per team** as a deployment-level measure (for example,
   by configuring each instance to only process bundles belonging to a specific team). However, even
   with separate instances, each Dag File Processor and Triggerer potentially retains direct access
   to the metadata database — a Dag author whose code runs in these components can potentially
   retrieve credentials from the parent process and access the database directly, including reading
   or modifying data belonging to other teams, unless the Deployment Manager implements Unix
   user-level isolation (see :ref:`deployment-hardening-for-improved-isolation`).

**No cross-workload isolation in the Execution API**
   All worker workloads authenticate to the same Execution API with tokens signed by the same key and
   sharing the same audience. While the ``ti:self`` scope enforcement prevents a worker from accessing
   another task's specific endpoints (heartbeat, state transitions), shared resources such as connections,
   variables, and XComs are accessible to all tasks. There is no isolation between tasks belonging to
   different teams or Dag authors at the Execution API level.

**Token signing key might be a shared secret**
   In symmetric key mode (``[api_auth] jwt_secret``), the same secret key is used to both generate and
   validate tokens. Any component that has access to this secret can forge tokens with arbitrary claims,
   including tokens for other task instances or with elevated scopes. This does not impact the security
   of the system though if the secret is only available to api-server and scheduler via deployment
   configuration.

**Sensitive configuration values can be leaked through logs**
   Dag authors can write code that prints environment variables or configuration values to task logs
   (e.g., ``print(os.environ)``). Airflow masks known sensitive values in logs, but masking depends on
   recognizing the value patterns. Dag authors who intentionally or accidentally log raw environment
   variables may expose database credentials, JWT signing keys, Fernet keys, or other secrets in task
   logs. Deployment Managers should restrict access to task logs and ensure that sensitive configuration
   is only provided to components where it is needed (see the sensitive variables tables below).

.. _deployment-hardening-for-improved-isolation:

Deployment hardening for improved isolation
...........................................

Deployment Managers who require stronger isolation between Dag authors and teams can take the following
measures. Note that these are deployment-specific actions that go beyond Airflow's built-in security
model — Airflow does not enforce these natively.

**Mandatory code review of Dag files**
   Implement a review process for all Dag submissions to Dag bundles. This can include:

   * Requiring pull request reviews before Dag files are deployed.
   * Static analysis of Dag code to detect suspicious patterns (e.g., direct database access attempts,
     reading environment variables, importing configuration modules).
   * Automated linting rules that flag potentially dangerous code.

**Restrict sensitive configuration to components that need them**
   Do not share all configuration parameters across all components. In particular:

   * The JWT signing key (``[api_auth] jwt_secret`` or ``[api_auth] jwt_private_key_path``) should only
     be available to components that need to generate tokens (Scheduler/Executor, API Server) and
     components that need to validate tokens (API Server). Workers should not have access to the signing
     key — they only need the tokens provided to them.
   * Connection credentials for external systems (via Secrets Managers) should only be available to the API Server
     (which serves them to workers via the Execution API), not to the Scheduler, Dag File Processor,
     or Triggerer processes directly. This however limits some of the features of Airflow - such as Deadline
     Alerts or triggers that need to authenticate with the external systems.
   * Database connection strings should only be available to components that need direct database access
     (API Server, Scheduler, Dag File Processor, Triggerer), not to workers.

**Pass configuration via environment variables**
   For higher security, pass sensitive configuration values via environment variables rather than
   configuration files. Environment variables are inherently safer than configuration files in
   Airflow's worker processes because of a built-in protection: on Linux, the supervisor process
   calls ``prctl(PR_SET_DUMPABLE, 0)`` before forking the task process, and this flag is inherited
   by the forked child. This marks both processes as non-dumpable, which prevents same-UID sibling
   processes from reading ``/proc/<pid>/environ``, ``/proc/<pid>/mem``, or attaching via
   ``ptrace``. In contrast, configuration files on disk are readable by any process running as
   the same Unix user. Environment variables can also be scoped to individual processes or
   containers, making it easier to restrict which components have access to which secrets.

   The following tables list all security-sensitive configuration variables (marked ``sensitive: true``
   in Airflow's configuration). Deployment Managers should review each variable and ensure it is only
   provided to the components that need it. The "Needed by" column indicates which components
   typically require the variable — but actual needs depend on the specific deployment topology and
   features in use.

   .. START AUTOGENERATED CORE SENSITIVE VARS

   **Core Airflow sensitive configuration variables:**

   .. list-table::
      :header-rows: 1
      :widths: 40 30 30

      * - Environment variable
        - Description
        - Needed by
      * - ``AIRFLOW__API_AUTH__JWT_SECRET``
        - JWT signing key (symmetric mode)
        - API Server, Scheduler
      * - ``AIRFLOW__API__SECRET_KEY``
        - API secret key for log token signing
        - API Server, Scheduler, Workers, Triggerer
      * - ``AIRFLOW__CORE__ASSET_MANAGER_KWARGS``
        - Asset manager credentials
        - Dag File Processor
      * - ``AIRFLOW__CORE__FERNET_KEY``
        - Fernet encryption key for connections/variables at rest
        - API Server, Scheduler, Workers, Dag File Processor, Triggerer
      * - ``AIRFLOW__DATABASE__SQL_ALCHEMY_CONN``
        - Metadata database connection string
        - API Server, Scheduler, Dag File Processor, Triggerer
      * - ``AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_ASYNC``
        - Async metadata database connection string
        - API Server, Scheduler, Dag File Processor, Triggerer
      * - ``AIRFLOW__DATABASE__SQL_ALCHEMY_ENGINE_ARGS``
        - SQLAlchemy engine parameters (may contain credentials)
        - API Server, Scheduler, Dag File Processor, Triggerer
      * - ``AIRFLOW__LOGGING__REMOTE_TASK_HANDLER_KWARGS``
        - Remote logging handler credentials
        - Scheduler, Workers, Triggerer
      * - ``AIRFLOW__SECRETS__BACKEND_KWARGS``
        - Secrets backend credentials (non-worker mode)
        - Scheduler, Dag File Processor, Triggerer
      * - ``AIRFLOW__SENTRY__SENTRY_DSN``
        - Sentry error reporting endpoint
        - Scheduler, Triggerer
      * - ``AIRFLOW__WORKERS__SECRETS_BACKEND_KWARGS``
        - Worker-specific secrets backend credentials
        - Workers

   .. END AUTOGENERATED CORE SENSITIVE VARS

   Note that ``AIRFLOW__API_AUTH__JWT_PRIVATE_KEY_PATH`` (path to the JWT private key for asymmetric
   signing) is not marked as ``sensitive`` in config.yml because it is a file path, not a secret
   value itself. However, access to the file it points to should be restricted to the Scheduler
   (which generates tokens) and the API Server (which validates them).

   .. START AUTOGENERATED PROVIDER SENSITIVE VARS

   **Provider-specific sensitive configuration variables:**

   The following variables are defined by Airflow providers and should only be set on components where
   the corresponding provider functionality is needed. The decision of which components require these
   variables depends on the Deployment Manager's choices about which providers and features are
   enabled in each component.

   .. list-table::
      :header-rows: 1
      :widths: 40 30 30

      * - Environment variable
        - Provider
        - Description
      * - ``AIRFLOW__CELERY_BROKER_TRANSPORT_OPTIONS__SENTINEL_KWARGS``
        - celery
        - Sentinel kwargs
      * - ``AIRFLOW__CELERY_RESULT_BACKEND_TRANSPORT_OPTIONS__SENTINEL_KWARGS``
        - celery
        - Sentinel kwargs
      * - ``AIRFLOW__CELERY__BROKER_URL``
        - celery
        - Broker url
      * - ``AIRFLOW__CELERY__FLOWER_BASIC_AUTH``
        - celery
        - Flower basic auth
      * - ``AIRFLOW__CELERY__RESULT_BACKEND``
        - celery
        - Result backend
      * - ``AIRFLOW__KEYCLOAK_AUTH_MANAGER__CLIENT_SECRET``
        - keycloak
        - Client secret
      * - ``AIRFLOW__OPENSEARCH__PASSWORD``
        - opensearch
        - Password
      * - ``AIRFLOW__OPENSEARCH__USERNAME``
        - opensearch
        - Username

   .. END AUTOGENERATED PROVIDER SENSITIVE VARS

   Deployment Managers should review the full configuration reference and identify any additional
   parameters that contain credentials or secrets relevant to their specific deployment.

**Use asymmetric keys for JWT signing**
   Using asymmetric keys (``[api_auth] jwt_private_key_path`` with a JWKS endpoint) provides better
   security than symmetric keys because:

   * The private key (used for signing) can be restricted to the Scheduler/Executor.
   * The API Server only needs the public key (via JWKS) for validation.
   * Workers cannot forge tokens even if they could access the JWKS endpoint, since they would
     not have the private key.

**Network-level isolation**
   Use network policies, VPCs, or similar mechanisms to restrict which components can communicate
   with each other. For example, workers should only be able to reach the Execution API endpoint,
   not the metadata database or internal services directly. The Dag File Processor and Triggerer
   child processes should ideally not have network access to the metadata database either, if
   Unix user-level isolation is implemented.

**Other measures and future improvements**
   Deployment Managers may need to implement additional measures depending on their security
   requirements. These may include monitoring and auditing of Execution API access patterns,
   runtime sandboxing of Dag code, or dedicated infrastructure per team.

   Future versions of Airflow plan to address these limitations through two approaches:

   * **Strategic (longer-term)**: Move the Dag File Processor and Triggerer to communicate with
     the metadata database exclusively through the API server (similar to how workers use the
     Execution API today). This would eliminate the need for these components to have database
     credentials at all, providing security by design rather than relying on deployment-level
     measures.
   * **Tactical (shorter-term)**: Native support for Unix user impersonation in the Dag File
     Processor and Triggerer child processes, so that Dag author code runs as a different, low-
     privilege user that cannot access the parent's credentials or the database.

   The Airflow community is actively working on these improvements.


Custom RBAC limitations
-----------------------

While RBAC defined in Airflow might limit access for certain UI users to certain Dags and features, when
it comes to custom roles and permissions, some permissions might override individual access to Dags or lack
of those. For example - audit log permission allows the user who has it to see logs of all Dags, even if
they don't have access to those Dags explicitly. This is something that the Deployment Manager
should be aware of when creating custom RBAC roles.

Triggering Dags via Assets
--------------------------

Triggering Dags via Assets is a feature that allows an asset materialization to trigger a Dag. This feature
is designed to allow triggering Dags without giving users specific access to triggering the Dags manually.
The "Trigger Dag" permission only affects triggering dags manually via the UI or API, but it does not affect
triggering Dags via Assets. Dag authors explicitly allow for specific assets to trigger the Dags and
they give anyone who has capability to create those assets to trigger the Dags via Assets.


Responsibilities of Deployment Managers
---------------------------------------

As a Deployment Manager, you should be aware of the capabilities of Dag authors and make sure that
you trust them not to abuse the capabilities they have. You should also make sure that you have
properly configured the Airflow installation to prevent Dag authors from executing arbitrary code
in the Scheduler and API Server processes.

.. _deploying-and-protecting-airflow-installation:

Deploying and protecting Airflow installation
.............................................

Deployment Managers are also responsible for deploying Airflow and make it accessible to the users
in the way that follows best practices of secure deployment applicable to the organization where
Airflow is deployed. This includes but is not limited to:

* protecting communication using TLS/VPC and whatever network security is required by the organization
  that is deploying Airflow
* applying rate-limiting and other forms of protections that is usually applied to web applications
* applying authentication and authorization to the web application so that only known and authorized
  users can have access to Airflow
* any kind of detection of unusual activity and protection against it
* choosing the right session backend and configuring it properly including timeouts for the session

Limiting Dag author capabilities
.................................

The Deployment Manager might also use additional mechanisms to prevent Dag authors from executing
arbitrary code - for example they might introduce tooling around Dag submission that would allow
to review the code before it is deployed, statically-check it and add other ways to prevent malicious
code to be submitted. The way submitting code to a Dag bundle is done and protected is completely
up to the Deployment Manager - Airflow does not provide any tooling or mechanisms around it and it
expects that the Deployment Manager will provide the tooling to protect access to Dag bundles and
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

*  Access restrictions to views or Dags: Controlling user access to certain views or specific Dags,
   ensuring that users can only view or interact with authorized components.

Future: multi-team isolation
............................

These examples showcase ways in which Deployment Managers can refine and limit user privileges within Airflow,
providing tighter control and ensuring that users have access only to the necessary components and
functionalities based on their roles and responsibilities. However, fine-grained access control does not
yet provide full isolation and separation of access between different groups of users.

The experimental multi-team feature (``[core] multi_team``) is a step towards cross-team isolation, but it
currently only enforces team-based isolation at the UI and REST API level. **Task-level isolation is not yet
guaranteed** — workloads from different teams share the same Execution API, JWT signing keys, and access to
connections, variables, and XComs. In deployments where additional hardening measures (described in
:ref:`deployment-hardening-for-improved-isolation`) are not implemented, a task belonging to one team can
potentially access shared resources available to tasks from other teams. Deployment Managers who enable the
multi-team feature should not rely on it alone for security-critical isolation between teams at the task
execution layer — a deep understanding of configuration and deployment security is required by Deployment
Managers to configure it in a way that can guarantee separation between teams.

Future versions of Airflow will improve task-level isolation, including team-scoped Execution API enforcement,
finer-grained JWT token scopes, and better sandboxing of user-submitted code. The Airflow community is
actively working on these improvements.


What is NOT considered a security vulnerability
-----------------------------------------------

The following scenarios are **not** considered security vulnerabilities in Airflow. They are either
intentional design choices, consequences of the trust model described above, or issues that fall
outside Airflow's threat model. Security researchers (and AI agents performing security analysis)
should review this section before reporting issues to the Airflow security team.

For full details on reporting policies, see
`Airflow's Security Policy <https://github.com/apache/airflow/security/policy>`_.

Dag authors executing arbitrary code
.....................................

Dag authors can execute arbitrary code on workers, the Dag File Processor, and the Triggerer. This
includes accessing credentials, environment variables, and (in the case of the Dag File Processor
and Triggerer) potentially the metadata database directly. This is the intended behavior as described in
:ref:`capabilities-of-dag-authors` — Dag authors are trusted users. Reports that a Dag author can
"achieve RCE" or "access the database" by writing Dag code are restating a documented capability,
not discovering a vulnerability.

Dag author code passing unsanitized input to operators and hooks
................................................................

When a Dag author writes code that passes unsanitized UI user input (such as Dag run parameters,
variables, or connection configuration values) to operators, hooks, or third-party libraries, the
responsibility lies with the Dag author. Airflow's hooks and operators are low-level interfaces —
Dag authors are Python programmers who must sanitize inputs before passing them to these interfaces.

SQL injection or command injection is only considered a vulnerability if it can be triggered by a
**non-Dag-author** user role (e.g., an authenticated UI user) **without** the Dag author deliberately
writing code that passes that input unsafely. If the only way to exploit the injection requires writing
or modifying a Dag file, it is not a vulnerability — the Dag author already has the ability to execute
arbitrary code. See also :doc:`/security/sql`.

An exception exists when official Airflow documentation explicitly recommends a pattern that leads to
injection — in that case, the documentation guidance itself is the issue and may warrant an advisory.

Dag File Processor and Triggerer potentially having database access
...................................................................

The Dag File Processor potentially has direct database access to store serialized Dags. The Triggerer
potentially has direct database access to manage trigger state. Both components execute user-submitted
code (Dag files and trigger code respectively) and potentially bypass JWT authentication via an
in-process Execution API transport. These are intentional architectural choices, not vulnerabilities.
They are documented in :ref:`jwt-authentication-and-workload-isolation`.

Workers accessing shared Execution API resources
.................................................

Worker tasks can access connections, variables, and XComs via the Execution API using their JWT token.
While the ``ti:self`` scope prevents cross-task state manipulation, shared resources are accessible to
all tasks. This is the current design — not a vulnerability. Reports that "a task can read another
team's connection" are describing a known limitation of the current isolation model, documented in
:ref:`jwt-authentication-and-workload-isolation`.

Execution API tokens not being revocable
........................................

Execution API tokens issued to workers are short-lived (default 10 minutes) with automatic refresh
and are intentionally not subject to revocation. This is a design choice documented in
:doc:`/security/jwt_token_authentication`, not a missing security control.

Connection configuration capabilities
......................................

Users with the **Connection configuration** role can configure connections with arbitrary credentials
and connection parameters. When the ``test connection`` feature is enabled, these users can potentially
trigger RCE, arbitrary file reads, or Denial of Service through connection parameters. This is by
design — connection configuration users are highly privileged and must be trusted not to abuse these
capabilities. The ``test connection`` feature is disabled by default since Airflow 2.7.0, and enabling
it is an explicit Deployment Manager decision that acknowledges these risks. See
:ref:`connection-configuration-users` for details.

Denial of Service by authenticated users
........................................

Airflow is not designed to be exposed to untrusted users on the public internet. All users who can
access the Airflow UI and API are authenticated and known. Denial of Service scenarios triggered by
authenticated users (such as creating very large Dag runs, submitting expensive queries, or flooding
the API) are not considered security vulnerabilities. They are operational concerns that Deployment
Managers should address through rate limiting, resource quotas, and monitoring — standard measures
for any internal application. See :ref:`deploying-and-protecting-airflow-installation`.

Self-XSS by authenticated users
................................

Cross-site scripting (XSS) scenarios where the only victim is the user who injected the payload
(self-XSS) are not considered security vulnerabilities. Airflow's users are authenticated and
known, and self-XSS does not allow an attacker to compromise other users. If you discover an XSS
scenario where a lower-privileged user can inject a payload that executes in a higher-privileged
user's session without that user's action, that is a valid vulnerability and should be reported.

Simple Auth Manager
...................

The Simple Auth Manager is intended for development and testing only. This is clearly documented and
a prominent warning banner is displayed on the login page. Security issues specific to the Simple
Auth Manager (such as weak password handling, lack of rate limiting, or missing CSRF protections) are
not considered production security vulnerabilities. Production deployments must use a production-grade
auth manager.

Third-party dependency vulnerabilities in Docker images
.......................................................

Airflow's reference Docker images are built with the latest available dependencies at release time.
Vulnerabilities found by scanning these images against CVE databases are expected to appear over time
as new CVEs are published. These should **not** be reported to the Airflow security team. Instead,
users should build their own images with updated dependencies as described in the
`Docker image documentation <https://airflow.apache.org/docs/docker-stack/index.html>`_.

If you discover that a third-party dependency vulnerability is **actually exploitable** in Airflow
(with a proof-of-concept demonstrating the exploitation in Airflow's context), that is a valid
report and should be submitted following the security policy.

Automated scanning results without human verification
.....................................................

Automated security scanner reports that list findings without human verification against Airflow's
security model are not considered valid vulnerability reports. Airflow's trust model differs
significantly from typical web applications — many scanner findings (such as "admin user can execute
code" or "database credentials accessible in configuration") are expected behavior. Reports must
include a proof-of-concept that demonstrates how the finding violates the security model described
in this document, including identifying the specific user role involved and the attack scenario.

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

Workload
========

This topic describes how to configure Airflow to secure your workload.

Impersonation
-------------

Airflow has the ability to impersonate a unix user while running task
instances based on the task's ``run_as_user`` parameter, which takes a user's name.

**NOTE:** For impersonations to work, Airflow requires ``sudo`` as subtasks are run
with ``sudo -u`` and permissions of files are changed. Furthermore, the unix user
needs to exist on the worker. Here is what a simple sudoers file entry could look
like to achieve this, assuming Airflow is running as the ``airflow`` user. This means
the Airflow user must be trusted and treated the same way as the root user.

.. code-block:: none

    airflow ALL=(ALL) NOPASSWD: ALL


Subtasks with impersonation will still log to the same folder, except that the files they
log to will have permissions changed such that only the unix user can write to it.

Default Impersonation
'''''''''''''''''''''
To prevent tasks that don't use impersonation to be run with ``sudo`` privileges, you can set the
``core:default_impersonation`` config which sets a default user impersonate if ``run_as_user`` is
not set.

.. code-block:: ini

    [core]
    default_impersonation = airflow

.. _workload-isolation:

Workload Isolation and Current Limitations
------------------------------------------

This section describes the current state of workload isolation in Apache Airflow,
including the protections that are in place, the known limitations, and planned improvements.

For the full security model and deployment hardening guidance, see :doc:`/security/security_model`.
For details on the JWT authentication flows used by workers and internal components, see
:doc:`/security/jwt_token_authentication`.

Worker process memory protection (Linux)
''''''''''''''''''''''''''''''''''''''''

On Linux, the supervisor process calls ``prctl(PR_SET_DUMPABLE, 0)`` at the start of
``supervise_task()`` before forking the task process. This flag is inherited by the forked
child. Marking processes as non-dumpable prevents same-UID sibling processes from reading
``/proc/<pid>/mem``, ``/proc/<pid>/environ``, or ``/proc/<pid>/maps``, and blocks
``ptrace(PTRACE_ATTACH)``. This is critical because each supervisor holds a distinct JWT
token in memory — without this protection, a malicious task process running as the same
Unix user could steal tokens from sibling supervisor processes.

This protection is one of the reasons that passing sensitive configuration via environment
variables is safer than via configuration files: environment variables are only readable
by the process itself (and root), whereas configuration files on disk are readable by any
process with filesystem access running as the same user.

.. note::

   This protection is Linux-specific. On non-Linux platforms, the
   ``_make_process_nondumpable()`` call is a no-op. Deployment Managers running Airflow
   on non-Linux platforms should implement alternative isolation measures.

No cross-workload isolation
'''''''''''''''''''''''''''

All worker workloads authenticate to the same Execution API with tokens that share the
same signing key, audience, and issuer. While the ``ti:self`` scope enforcement prevents
a worker from accessing *another task instance's* specific endpoints (e.g., heartbeat,
state transitions), the token grants access to shared resources such as connections,
variables, and XComs that are not scoped to individual tasks.

No team-level isolation in Execution API (experimental multi-team feature)
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

The experimental multi-team feature (``[core] multi_team``) provides UI-level and REST
API-level RBAC isolation between teams, but **does not yet guarantee task-level isolation**.
At the Execution API level, there is no enforcement of team-based access boundaries.
A task from one team can access the same connections, variables, and XComs as a task from
another team. All workloads share the same JWT signing keys and audience regardless of team
assignment.

In deployments where additional hardening measures are not implemented at the deployment
level, a task from one team can potentially access resources belonging to another team
(see :doc:`/security/security_model`). A deep understanding of configuration and deployment
security is required by Deployment Managers to configure it in a way that can guarantee
separation between teams. Task-level team isolation will be improved in future versions
of Airflow.

Dag File Processor and Triggerer potentially bypass JWT and access the database
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

As described in :doc:`/security/jwt_token_authentication`, the default deployment runs a
single Dag File Processor and a single Triggerer for all teams. Both potentially bypass
JWT authentication via in-process transport. For multi-team isolation, Deployment Managers
must run separate instances per team, but even then, each instance potentially retains
direct database access. A Dag author whose code runs in these components can potentially
access the database directly — including data belonging to other teams or the JWT signing
key configuration — unless the Deployment Manager restricts the database credentials and
configuration available to each instance.

Planned improvements
''''''''''''''''''''

Future versions of Airflow will address these limitations with:

- Finer-grained token scopes tied to specific resources (connections, variables) and teams.
- Enforcement of team-based isolation in the Execution API.
- Built-in support for per-team Dag File Processor and Triggerer instances.
- Improved sandboxing of user-submitted code in the Dag File Processor and Triggerer.
- Full task-level isolation for the multi-team feature.

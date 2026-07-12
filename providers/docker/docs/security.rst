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

Docker Sandboxes executor
=========================

The Docker Sandboxes executor is an explicitly non-production integration for
development, conformance testing, and end-to-end testing. Docker Sandboxes do
not provide the provider-enforced hard TTL and durable execution record required
by the Common Sandbox production-driver contract. A scheduler, host, or local
sandbox-daemon failure can therefore leave a workload behind. Do not use this
executor for production tasks or as a multi-tenant security boundary.

Access to the local Docker and Docker Sandboxes daemons is privileged. Grant it
only to the scheduler service account and trusted host administrators. Run the
scheduler and daemon on a dedicated host, keep KVM and host software patched,
and do not share the sandbox scratch root with other services.

The executor writes each task specification, including its short-lived
Execution API token and allowlisted environment, beneath ``workspace_root``.
The per-task directory is mode ``0700`` and protocol files are mode ``0600``,
but a host administrator can still inspect them. The directory is also the
sandbox's writable workspace, so task code can alter ``status.json``; that file
coordinates the development adapter and is not a trusted result attestation.
Put the root on a dedicated local filesystem, restrict backups, and let the
executor delete it after the test. Never place long-lived credentials in the
sandbox template or task environment.

Network policy is deployment configuration. Select a non-interactive Docker
Sandboxes policy that permits only the Airflow Execution API, remote log store,
and task-specific services. Dag authors cannot override that policy through
``executor_config``. Remote logging is mandatory because local sandbox state is
ephemeral and is not a durable audit log.

.. include:: /../../../devel-common/src/sphinx_exts/includes/security.rst

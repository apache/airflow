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

Azure Container Apps Jobs
=========================

.. _howto/operator:AzureContainerAppsJobOperator:

Use :class:`~airflow.providers.microsoft.azure.operators.container_apps.AzureContainerAppsJobOperator`
to start an **existing** Azure Container Apps Job and wait for the execution returned by Azure.
The operator does not create, update, or delete jobs. On success, its return value (and default
``return_value`` XCom) is the execution name.

Authentication
--------------

Use the :ref:`Azure connection <howto/connection:azure>` ``azure_default`` or supply
``azure_conn_id``. For a service principal, set login to the client ID, password to the client
secret, and extra ``tenantId`` to the tenant ID. Otherwise the provider uses DefaultAzureCredential.
For workload identity, provision the Azure workload identity environment and federated token file
on **both workers and triggerers**. The Azure connection can supply ``managed_identity_client_id``
and ``workload_identity_tenant_id`` through the provider's existing credential helpers.

Supply ``subscription_id`` explicitly or set ``subscriptionId`` in the connection extra.
``resource_group`` and ``job_name`` are always explicit. The identity needs permission to read
the job and its executions, start executions, and stop the exact execution.

Defaults and overrides
----------------------

Without overrides, Azure uses the job's configured defaults. To override a container, specify
``container_name`` and any of ``image``, ``command``, ``args``, ``env_vars``, ``cpu``, or ``memory``.
Only that named regular container changes. Other regular containers and init containers are
preserved. Supplied ``env_vars`` **replace** that container's environment, including existing
secret references; an empty mapping clears it. Omitted fields keep their configured values.
Target and override fields, including ``container_name``, support templating.

.. exampleinclude:: /../tests/system/microsoft/azure/example_container_apps.py
    :language: python
    :dedent: 4
    :start-after: [START container_apps_jobs]
    :end-before: [END container_apps_jobs]

Waiting, timeouts, and cancellation
-----------------------------------

``deferrable`` defaults to Airflow's ``operators.default_deferrable`` setting. Set it to ``True``
to release the worker while the triggerer polls using the native async Azure SDK, or ``False``
to poll on the worker. The default poll interval is 60 seconds. Optional ``timeout_seconds``
limits the wait after submission. Deferred timeouts keep their original UTC deadline across
triggerer restarts; elapsed time within each process uses a monotonic clock.
Status lookups, including execution history pages and synchronous SDK retry sleeps, use the
remaining deadline budget.
Results returned after the deadline are treated as timeouts, even if Azure reports success.

Both modes follow only the submitted execution. Running, processing, unknown, and empty statuses
remain active. Succeeded completes; other statuses fail with Azure's message or reason when available.
If a direct lookup returns not found, execution history is searched for that exact name. An execution
absent from history fails; absence never implies success.

Set task **retries=0**. Submission transport retries are disabled because an ambiguous response
cannot establish whether Azure started a job; an Airflow retry could submit a duplicate execution.
Reads and exact-execution stops use bounded SDK retries.

On timeout the operator stops the execution best effort and reports failure, even when
``cancel_on_termination=False``. Explicit worker termination also stops it by default.
Timeout handling permits up to 10 additional seconds for a best-effort stop request.
Stops disable SDK operation polling: Azure accepting the request does not confirm that
the execution has finished stopping. Synchronous cancellation applies this budget to SDK
transport timeouts and retry sleeps; asynchronous cancellation also bounds the complete request.
Deferred manual cancellation requires an Airflow version whose triggerer invokes ``BaseTrigger.on_kill``;
older supported versions keep the Azure execution running after a deferred task is manually failed,
cleared, or marked successful. On those versions, stop it in Azure. This provider does not increase
the minimum Airflow version. Generic trigger cleanup, redistribution, and triggerer shutdown do not
stop Azure work. An Airflow-level ``execution_timeout`` can also terminate a deferred task before
this operator's deadline; use ``timeout_seconds`` when the Azure execution must be stopped on timeout.

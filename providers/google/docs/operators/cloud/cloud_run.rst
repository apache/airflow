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

Google Cloud Run Operators
===============================

Cloud Run is used to build and deploy scalable containerized apps written in any language (including Go, Python, Java, Node.js, .NET, and Ruby) on a fully managed platform.

For more information about the service visit `Google Cloud Run documentation <https://cloud.google.com/run/docs/>`__.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

Create a job
---------------------

Before you create a job in Cloud Run, you need to define it.
For more information about the Job object fields, visit `Google Cloud Run Job description <https://cloud.google.com/run/docs/reference/rpc/google.cloud.run.v2#google.cloud.run.v2.Job>`__

A simple job configuration can be created with a Job object:

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_run/example_cloud_run.py
    :language: python
    :dedent: 0
    :start-after: [START howto_cloud_run_job_instance_creation]
    :end-before: [END howto_cloud_run_job_instance_creation]

or with a Python dictionary:

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_run/example_cloud_run.py
    :language: python
    :dedent: 0
    :start-after: [START howto_cloud_run_job_dict_creation]
    :end-before: [END howto_cloud_run_job_dict_creation]

You can create a Cloud Run Job with any of these configurations :
:class:`~airflow.providers.google.cloud.operators.cloud_run.CloudRunCreateJobOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_run/example_cloud_run.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloud_run_create_job]
    :end-before: [END howto_operator_cloud_run_create_job]


Note that this operator only creates the job without executing it. The Job's dictionary representation is pushed to XCom.

Create a service
---------------------

Before you create a service in Cloud Run, you need to define it.
For more information about the Service object fields, visit `Google Cloud Run Service description <https://cloud.google.com/run/docs/reference/rpc/google.cloud.run.v2#google.cloud.run.v2.Service>`__

A simple service configuration can look as follows:

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_run/example_cloud_run_service.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_cloud_run_service_creation]
    :end-before: [END howto_operator_cloud_run_service_creation]


With this configuration we can create the service:
:class:`~airflow.providers.google.cloud.operators.cloud_run.CloudRunCreateServiceOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_run/example_cloud_run_service.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloud_run_create_service]
    :end-before: [END howto_operator_cloud_run_create_service]


Note that this operator only creates the service without executing it. The Service's dictionary representation is pushed to XCom.

Delete a service
---------------------

With this configuration we can delete the service:
:class:`~airflow.providers.google.cloud.operators.cloud_run.CloudRunDeleteServiceOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_run/example_cloud_run_service.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloud_run_delete_service]
    :end-before: [END howto_operator_cloud_run_delete_service]


Note this operator waits for the service to be deleted, and the deleted Service's dictionary representation is pushed to XCom.

Execute a job
---------------------

To execute a job, you can use:

:class:`~airflow.providers.google.cloud.operators.cloud_run.CloudRunExecuteJobOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_run/example_cloud_run.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloud_run_execute_job]
    :end-before: [END howto_operator_cloud_run_execute_job]

or you can define the same operator in the deferrable mode:

:class:`~airflow.providers.google.cloud.operators.cloud_run.CloudRunExecuteJobOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_run/example_cloud_run.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloud_run_execute_job_deferrable_mode]
    :end-before: [END howto_operator_cloud_run_execute_job_deferrable_mode]

Transport
^^^^^^^^^

The :class:`~airflow.providers.google.cloud.operators.cloud_run.CloudRunExecuteJobOperator` accepts an optional ``transport``
parameter to choose the underlying API transport.

* ``transport="grpc"`` (default): use gRPC transport. If ``transport`` is not set, gRPC is used.
* ``transport="rest"``: use REST/HTTP transport.

In deferrable mode, when using gRPC (explicitly or by default), the trigger uses an async gRPC client internally; for
non-deferrable execution, the operator uses the regular (synchronous) gRPC client.

In general, it is better to use gRPC (or leave ``transport`` unset) unless there is a specific reason you must use REST (for example,
if gRPC is not available or fails in your environment).

.. rubric:: Deferrable mode considerations

When using deferrable mode, the operator defers to an async trigger that polls the long-running operation status.

* With gRPC (explicitly or by default), the trigger uses the native async gRPC client internally. The ``grpc_asyncio`` transport is
    an implementation detail of the Google client library and is not a user-facing ``transport`` value.
* With REST, the REST transport is synchronous-only in the Google Cloud library. To remain compatible with deferrable mode, the
    trigger performs REST calls using the synchronous client wrapped in a background thread.

REST can be used with deferrable mode, but it may be less efficient than gRPC and is generally best reserved for cases where gRPC
cannot be used.

You can also specify overrides that allow you to give a new entrypoint command to the job and more:

:class:`~airflow.providers.google.cloud.operators.cloud_run.CloudRunExecuteJobOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_run/example_cloud_run.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloud_run_execute_job_with_overrides]
    :end-before: [END howto_operator_cloud_run_execute_job_with_overrides]


Update a job
------------------

To update a job, you can use:

:class:`~airflow.providers.google.cloud.operators.cloud_run.CloudRunUpdateJobOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_run/example_cloud_run.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloud_update_job]
    :end-before: [END howto_operator_cloud_update_job]


The Job's dictionary representation is pushed to XCom.


List jobs
----------------------

To list the jobs, you can use:

:class:`~airflow.providers.google.cloud.operators.cloud_run.CloudRunListJobsOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_run/example_cloud_run.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloud_run_list_jobs]
    :end-before: [END howto_operator_cloud_run_list_jobs]

The operator takes two optional parameters: "limit" to limit the number of tasks returned, and "show_deleted" to include deleted jobs in the result.


Delete a job
-----------------

To delete a job you can use:

:class:`~airflow.providers.google.cloud.operators.cloud_run.CloudRunDeleteJobOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_run/example_cloud_run.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloud_delete_job]
    :end-before: [END howto_operator_cloud_delete_job]

Note this operator waits for the job to be deleted, and the deleted Job's dictionary representation is pushed to XCom.

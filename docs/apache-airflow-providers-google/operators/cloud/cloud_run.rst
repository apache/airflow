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

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_run/example_cloud_run.py
    :language: python
    :dedent: 0
    :start-after: [START howto_cloud_run_job_instance_creation]
    :end-before: [END howto_cloud_run_job_instance_creation]

or with a Python dictionary:

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_run/example_cloud_run.py
    :language: python
    :dedent: 0
    :start-after: [START howto_cloud_run_job_dict_creation]
    :end-before: [END howto_cloud_run_job_dict_creation]

You can create a Cloud Run Job with any of these configurations :
:class:`~airflow.providers.google.cloud.operators.cloud_run.CloudRunCreateJobOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_run/example_cloud_run.py
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

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_run/example_cloud_run_service.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_cloud_run_service_creation]
    :end-before: [END howto_operator_cloud_run_service_creation]


With this configuration we can create the service:
:class:`~airflow.providers.google.cloud.operators.cloud_run.CloudRunCreateServiceOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_run/example_cloud_run_service.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloud_run_create_service]
    :end-before: [END howto_operator_cloud_run_create_service]


Note that this operator only creates the service without executing it. The Service's dictionary representation is pushed to XCom.

Delete a service
---------------------

With this configuration we can delete the service:
:class:`~airflow.providers.google.cloud.operators.cloud_run.CloudRunDeleteServiceOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_run/example_cloud_run_service.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloud_run_delete_service]
    :end-before: [END howto_operator_cloud_run_delete_service]


Note this operator waits for the service to be deleted, and the deleted Service's dictionary representation is pushed to XCom.

Execute a job
---------------------

To execute a job, you can use:

:class:`~airflow.providers.google.cloud.operators.cloud_run.CloudRunExecuteJobOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_run/example_cloud_run.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloud_run_execute_job]
    :end-before: [END howto_operator_cloud_run_execute_job]

or you can define the same operator in the deferrable mode:

:class:`~airflow.providers.google.cloud.operators.cloud_run.CloudRunExecuteJobOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_run/example_cloud_run.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloud_run_execute_job_deferrable_mode]
    :end-before: [END howto_operator_cloud_run_execute_job_deferrable_mode]

You can also specify overrides that allow you to give a new entrypoint command to the job and more:

:class:`~airflow.providers.google.cloud.operators.cloud_run.CloudRunExecuteJobOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_run/example_cloud_run.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloud_run_execute_job_with_overrides]
    :end-before: [END howto_operator_cloud_run_execute_job_with_overrides]


Update a job
------------------

To update a job, you can use:

:class:`~airflow.providers.google.cloud.operators.cloud_run.CloudRunUpdateJobOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_run/example_cloud_run.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloud_update_job]
    :end-before: [END howto_operator_cloud_update_job]


The Job's dictionary representation is pushed to XCom.


List jobs
----------------------

To list the jobs, you can use:

:class:`~airflow.providers.google.cloud.operators.cloud_run.CloudRunListJobsOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_run/example_cloud_run.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloud_run_list_jobs]
    :end-before: [END howto_operator_cloud_run_list_jobs]

The operator takes two optional parameters: "limit" to limit the number of tasks returned, and "show_deleted" to include deleted jobs in the result.


Delete a job
-----------------

To delete a job you can use:

:class:`~airflow.providers.google.cloud.operators.cloud_run.CloudRunDeleteJobOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_run/example_cloud_run.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloud_delete_job]
    :end-before: [END howto_operator_cloud_delete_job]

Note this operator waits for the job to be deleted, and the deleted Job's dictionary representation is pushed to XCom.

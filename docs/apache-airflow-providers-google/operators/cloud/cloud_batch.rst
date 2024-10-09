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

Google Cloud Batch Operators
===============================

Cloud Batch is a fully managed batch service to schedule, queue, and execute batch jobs on Google's infrastructure.

For more information about the service visit `Google Cloud Batch documentation <https://cloud.google.com/batch>`__.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

Submit a job
---------------------

Before you submit a job in Cloud Batch, you need to define it.
For more information about the Job object fields, visit `Google Cloud Batch Job description <https://cloud.google.com/python/docs/reference/batch/latest/google.cloud.batch_v1.types.Job>`__.

A simple job configuration can look as follows:

.. exampleinclude:: /../../providers/tests/system/google/cloud/cloud_batch/example_cloud_batch.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_batch_job_creation]
    :end-before: [END howto_operator_batch_job_creation]

With this configuration we can submit the job:
:class:`~airflow.providers.google.cloud.operators.cloud_batch.CloudBatchSubmitJobOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/cloud_batch/example_cloud_batch.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_batch_submit_job]
    :end-before: [END howto_operator_batch_submit_job]

or you can define the same operator in the deferrable mode:
:class:`~airflow.providers.google.cloud.operators.cloud_batch.CloudBatchSubmitJobOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/cloud_batch/example_cloud_batch.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_batch_submit_job_deferrable_mode]
    :end-before: [END howto_operator_batch_submit_job_deferrable_mode]

Note that this operator waits for the job complete its execution, and the Job's dictionary representation is pushed to XCom.

List a job's tasks
------------------

To list the tasks of a certain job, you can use:

:class:`~airflow.providers.google.cloud.operators.cloud_batch.CloudBatchListTasksOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/cloud_batch/example_cloud_batch.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_batch_list_tasks]
    :end-before: [END howto_operator_batch_list_tasks]

The operator takes two optional parameters: "limit" to limit the number of tasks returned, and "filter" to only list the tasks matching the `filter <https://cloud.google.com/sdk/gcloud/reference/topic/filters>`__.

List jobs
----------------------

To list the jobs, you can use:

:class:`~airflow.providers.google.cloud.operators.cloud_batch.CloudBatchListJobsOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/cloud_batch/example_cloud_batch.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_batch_list_jobs]
    :end-before: [END howto_operator_batch_list_jobs]

The operator takes two optional parameters: "limit" to limit the number of tasks returned, and "filter" to only list the tasks matching the `filter <https://cloud.google.com/sdk/gcloud/reference/topic/filters>`__.

Delete a job
-----------------

To delete a job you can use:

:class:`~airflow.providers.google.cloud.operators.cloud_batch.CloudBatchDeleteJobOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/cloud_batch/example_cloud_batch.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_delete_job]
    :end-before: [END howto_operator_delete_job]


Note that this operator waits for the job to be deleted, and the deleted Job's dictionary representation is pushed to XCom.

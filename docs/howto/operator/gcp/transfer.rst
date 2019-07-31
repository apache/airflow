..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Google Cloud Transfer Service Operators
=======================================

.. contents::
  :depth: 1
  :local:

.. _howto/operator:GcpTransferServiceJobCreateOperator:

GcpTransferServiceJobCreateOperator
-----------------------------------

Create a transfer job.

The function accepts dates in two formats:

- consistent with `Google API <https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs#TimeOfDay>`_ ::

    { "year": 2019, "month": 2, "day": 11 }

- as an :class:`~datetime.datetime` object

The function accepts time in two formats:

- consistent with `Google API <https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs#TimeOfDay>`_ ::

    { "hours": 12, "minutes": 30, "seconds": 0 }

- as an :class:`~datetime.time` object

If you want to create a job transfer that copies data from AWS S3 then you must have a connection configured. Information about configuration for AWS is available: :doc:`../../connection/aws`
The selected connection for AWS can be indicated by the parameter ``aws_conn_id``.

Arguments
"""""""""

Some arguments in the example DAG are taken from the OS environment variables:

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_transfer.py
      :language: python
      :start-after: [START howto_operator_gcp_transfer_common_variables]
      :end-before: [END howto_operator_gcp_transfer_common_variables]

Using the operator
""""""""""""""""""

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_transfer.py
      :language: python
      :start-after: [START howto_operator_gcp_transfer_create_job_body_gcp]
      :end-before: [END howto_operator_gcp_transfer_create_job_body_gcp]

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_transfer.py
      :language: python
      :start-after: [START howto_operator_gcp_transfer_create_job_body_aws]
      :end-before: [END howto_operator_gcp_transfer_create_job_body_aws]

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_transfer.py
      :language: python
      :dedent: 4
      :start-after: [START howto_operator_gcp_transfer_create_job]
      :end-before: [END howto_operator_gcp_transfer_create_job]

Templating
""""""""""

.. literalinclude:: ../../../../airflow/contrib/operators/gcp_transfer_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_transfer_job_create_template_fields]
    :end-before: [END gcp_transfer_job_create_template_fields]

More information
""""""""""""""""

See `Google Cloud Transfer Service - Method: transferJobs.create
<https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/create>`_.

.. _howto/operator:GcpTransferServiceJobDeleteOperator:

GcpTransferServiceJobDeleteOperator
-----------------------------------

Deletes a transfer job.

Arguments
"""""""""

Some arguments in the example DAG are taken from the OS environment variables:

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_transfer.py
      :language: python
      :start-after: [START howto_operator_gcp_transfer_common_variables]
      :end-before: [END howto_operator_gcp_transfer_common_variables]

Using the operator
""""""""""""""""""

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_transfer.py
      :language: python
      :dedent: 4
      :start-after: [START howto_operator_gcp_transfer_delete_job]
      :end-before: [END howto_operator_gcp_transfer_delete_job]

Templating
""""""""""

.. literalinclude:: ../../../../airflow/contrib/operators/gcp_transfer_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_transfer_job_delete_template_fields]
    :end-before: [END gcp_transfer_job_delete_template_fields]

More information
""""""""""""""""

See `Google Cloud Transfer Service - REST Resource: transferJobs - Status
<https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs#Status>`_

.. _howto/operator:GcpTransferServiceJobUpdateOperator:

GcpTransferServiceJobUpdateOperator
-----------------------------------

Updates a transfer job.

Arguments
"""""""""

Some arguments in the example DAG are taken from the OS environment variables:

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_transfer.py
      :language: python
      :start-after: [START howto_operator_gcp_transfer_common_variables]
      :end-before: [END howto_operator_gcp_transfer_common_variables]

Using the operator
""""""""""""""""""

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_transfer.py
      :language: python
      :start-after: [START howto_operator_gcp_transfer_update_job_body]
      :end-before: [END howto_operator_gcp_transfer_update_job_body]

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_transfer.py
      :language: python
      :dedent: 4
      :start-after: [START howto_operator_gcp_transfer_update_job]
      :end-before: [END howto_operator_gcp_transfer_update_job]

Templating
""""""""""

.. literalinclude:: ../../../../airflow/contrib/operators/gcp_transfer_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_transfer_job_update_template_fields]
    :end-before: [END gcp_transfer_job_update_template_fields]

More information
""""""""""""""""

See `Google Cloud Transfer Service - Method: transferJobs.patch
<https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/patch>`_

.. _howto/operator:GcpTransferServiceOperationCancelOperator:

GcpTransferServiceOperationCancelOperator
-----------------------------------------

Gets a transfer operation. The result is returned to XCOM.

Arguments
"""""""""

Some arguments in the example DAG are taken from the OS environment variables:

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_transfer.py
      :language: python
      :start-after: [START howto_operator_gcp_transfer_common_variables]
      :end-before: [END howto_operator_gcp_transfer_common_variables]

Using the operator
""""""""""""""""""

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_transfer.py
      :language: python
      :dedent: 4
      :start-after: [START howto_operator_gcp_transfer_cancel_operation]
      :end-before: [END howto_operator_gcp_transfer_cancel_operation]

Templating
""""""""""

.. literalinclude:: ../../../../airflow/contrib/operators/gcp_transfer_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_transfer_operation_cancel_template_fields]
    :end-before: [END gcp_transfer_operation_cancel_template_fields]

More information
""""""""""""""""

See `Google Cloud Transfer Service - Method: transferOperations.cancel
<https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations/cancel>`_


.. _howto/operator:GcpTransferServiceOperationGetOperator:

GcpTransferServiceOperationGetOperator
--------------------------------------

Gets a transfer operation. The result is returned to XCOM.

Arguments
"""""""""

Some arguments in the example DAG are taken from the OS environment variables:

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_transfer.py
      :language: python
      :start-after: [START howto_operator_gcp_transfer_common_variables]
      :end-before: [END howto_operator_gcp_transfer_common_variables]

Using the operator
""""""""""""""""""

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_transfer.py
      :language: python
      :dedent: 4
      :start-after: [START howto_operator_gcp_transfer_get_operation]
      :end-before: [END howto_operator_gcp_transfer_get_operation]

Templating
""""""""""

.. literalinclude:: ../../../../airflow/contrib/operators/gcp_transfer_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_transfer_operation_get_template_fields]
    :end-before: [END gcp_transfer_operation_get_template_fields]

More information
""""""""""""""""

See `Google Cloud Transfer Service - Method: transferOperations.get
<https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations/get>`_

.. _howto/operator:GcpTransferServiceOperationsListOperator:

GcpTransferServiceOperationsListOperator
----------------------------------------

List a transfer operations. The result is returned to XCOM.

Arguments
"""""""""

Some arguments in the example DAG are taken from the OS environment variables:

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_transfer.py
      :language: python
      :start-after: [START howto_operator_gcp_transfer_common_variables]
      :end-before: [END howto_operator_gcp_transfer_common_variables]

Using the operator
""""""""""""""""""

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_transfer.py
      :language: python
      :dedent: 4
      :start-after: [START howto_operator_gcp_transfer_list_operations]
      :end-before: [END howto_operator_gcp_transfer_list_operations]

Templating
""""""""""

.. literalinclude:: ../../../../airflow/contrib/operators/gcp_transfer_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_transfer_operations_list_template_fields]
    :end-before: [END gcp_transfer_operations_list_template_fields]

More information
""""""""""""""""

See `Google Cloud Transfer Service - Method: transferOperations.list
<https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations/list>`_

.. _howto/operator:GcpTransferServiceOperationPauseOperator:

GcpTransferServiceOperationPauseOperator
----------------------------------------

Pauses a transfer operations.

Arguments
"""""""""

Some arguments in the example DAG are taken from the OS environment variables:

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_transfer.py
      :language: python
      :start-after: [START howto_operator_gcp_transfer_common_variables]
      :end-before: [END howto_operator_gcp_transfer_common_variables]

Using the operator
""""""""""""""""""

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_transfer.py
      :language: python
      :dedent: 4
      :start-after: [START howto_operator_gcp_transfer_pause_operation]
      :end-before: [END howto_operator_gcp_transfer_pause_operation]

Templating
""""""""""

.. literalinclude:: ../../../../airflow/contrib/operators/gcp_transfer_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_transfer_operation_pause_template_fields]
    :end-before: [END gcp_transfer_operation_pause_template_fields]

More information
""""""""""""""""

See `Google Cloud Transfer Service - Method: transferOperations.pause
<https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations/pause>`_

.. _howto/operator:GcpTransferServiceOperationResumeOperator:

GcpTransferServiceOperationResumeOperator
-----------------------------------------

Resumes a transfer operations.

Arguments
"""""""""

Some arguments in the example DAG are taken from the OS environment variables:

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_transfer.py
      :language: python
      :start-after: [START howto_operator_gcp_transfer_common_variables]
      :end-before: [END howto_operator_gcp_transfer_common_variables]

Using the operator
""""""""""""""""""

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_transfer.py
      :language: python
      :dedent: 4
      :start-after: [START howto_operator_gcp_transfer_resume_operation]
      :end-before: [END howto_operator_gcp_transfer_resume_operation]

Templating
""""""""""

.. literalinclude:: ../../../../airflow/contrib/operators/gcp_transfer_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_transfer_operation_resume_template_fields]
    :end-before: [END gcp_transfer_operation_resume_template_fields]

More information
""""""""""""""""

See `Google Cloud Transfer Service - Method: transferOperations.resume
<https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations/resume>`_


GCPTransferServiceWaitForJobStatusSensor
----------------------------------------

Waits for at least one operation belonging to the job to have the expected status.

Arguments
"""""""""

Some arguments in the example DAG are taken from the OS environment variables:

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_transfer.py
      :language: python
      :start-after: [START howto_operator_gcp_transfer_common_variables]
      :end-before: [END howto_operator_gcp_transfer_common_variables]

Using the operator
""""""""""""""""""

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_transfer.py
      :language: python
      :dedent: 4
      :start-after: [START howto_operator_gcp_transfer_wait_operation]
      :end-before: [END howto_operator_gcp_transfer_wait_operation]

Templating
""""""""""

.. literalinclude:: ../../../../airflow/contrib/sensors/gcp_transfer_sensor.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_transfer_job_sensor_template_fields]
    :end-before: [END gcp_transfer_job_sensor_template_fields]

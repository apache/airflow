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


Amazon S3 Glacier Operators
===========================

`Amazon Glacier <https://docs.aws.amazon.com/amazonglacier/latest/dev/introduction.html>`_ is a secure, durable, and extremely low-cost Amazon S3 cloud storage class for data archiving and long-term backup.

Prerequisite Tasks
------------------

.. include:: _partials/prerequisite_tasks.rst

.. _howto/operator:GlacierCreateJobOperator:

Amazon Glacier Create Job Operator
""""""""""""""""""""""""""""""""""

To initiate an Amazon Glacier inventory retrieval job
use :class:`~airflow.providers.amazon.aws.transfers.glacier_to_gcs.GlacierCreateJobOperator`

This Operator returns a dictionary of information related to the initiated job such as *jobId*, which is required for subsequent tasks.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_glacier_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_glacier_create_job_operator]
    :end-before: [END howto_glacier_create_job_operator]

.. _howto/operator:GlacierJobOperationSensor:

Amazon Glacier Job Sensor
"""""""""""""""""""""""""

To wait on the status of an Amazon Glacier Job to reach a terminal state
use :class:`~airflow.providers.amazon.aws.sensors.glacier.GlacierJobOperationSensor`

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_glacier_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_glacier_job_operation_sensor]
    :end-before: [END howto_glacier_job_operation_sensor]


References
----------

For further information, look at:

* `Boto3 Library Documentation for Amazon Glacier <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html>`__

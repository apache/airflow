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


Amazon Glacier Operator
=======================

Amazon Glacier is a secure, durable, and extremely low-cost Amazon S3 cloud storage classes for data archiving and long-term backup.
For more information about the service visit `Amazon Glacier API documentation <https://docs.aws.amazon.com/code-samples/latest/catalog/code-catalog-python-example_code-glacier.html>`_

.. _howto/operator:GlacierCreateJobOperator:

GlacierCreateJobOperator
^^^^^^^^^^^^^^^^^^^^^^^^

Operator task is to initiate an Amazon Glacier inventory-retrieval job.
The operation returns dictionary of information related to the initiated job like *jobId* what is required for subsequent tasks.

To get more information about operator visit:
:class:`~airflow.providers.amazon.aws.transfers.glacier_to_gcs.GlacierCreateJobOperator`

Example usage:

.. exampleinclude:: /../airflow/providers/amazon/aws/example_dags/example_glacier_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_glacier_create_job_operator]
    :end-before: [END howto_glacier_create_job_operator]

.. _howto/operator:GlacierJobOperationSensor:

GlacierJobOperationSensor
^^^^^^^^^^^^^^^^^^^^^^^^^

Operator task is to wait until task *create_glacier_job* will be completed.
When sensor returns *true* then subsequent tasks can be executed.
In this case subsequent tasks are: *GlacierDownloadArchive* and *GlacierTransferDataToGCS*.

Job states:

* *Succeeded* – job is finished and for example archives from the vault can be downloaded
* *InProgress* – job is in progress and you have to wait until it's done (*Succeeded*)

GlacierJobOperationSensor checks the job status.
If response status code is *succeeded* then sensor returns *true* and subsequent tasks will be executed.
If response code is *InProgress* then sensor returns *false* and reschedule task with *poke_interval=60 * 20*.
Which means that every next request will be sent every 20 minutes.

To get more information about operator visit:
:class:`~airflow.providers.amazon.aws.sensors.glacier.GlacierJobOperationSensor`

Example usage:

.. exampleinclude:: /../airflow/providers/amazon/aws/example_dags/example_glacier_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_glacier_transfer_data_to_gcs]
    :end-before: [END howto_glacier_transfer_data_to_gcs]

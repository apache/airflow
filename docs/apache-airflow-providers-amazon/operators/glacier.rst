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

=================
Amazon S3 Glacier
=================

`Amazon Glacier <https://docs.aws.amazon.com/amazonglacier/latest/dev/introduction.html>`_ is a secure, durable,
and extremely low-cost Amazon S3 cloud storage class for data archiving and long-term backup.

Prerequisite Tasks
------------------

.. include:: _partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:GlacierCreateJobOperator:

Create an Amazon Glacier job
============================

To initiate an Amazon Glacier inventory retrieval job
use :class:`~airflow.providers.amazon.aws.transfers.glacier_to_gcs.GlacierCreateJobOperator`

This Operator returns a dictionary of information related to the initiated job such as *jobId*, which is required for subsequent tasks.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_glacier_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_glacier_create_job]
    :end-before: [END howto_operator_glacier_create_job]

.. _howto/operator:GlacierUploadArchiveOperator:

Upload archive to an Amazon Glacier
===================================

To add an archive to an Amazon S3 Glacier vault
use :class:`~airflow.providers.amazon.aws.transfers.glacier_to_gcs.GlacierUploadArchiveOperator`

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_glacier_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_glacier_upload_archive]
    :end-before: [END howto_operator_glacier_upload_archive]

Sensors
-------

.. _howto/sensor:GlacierJobOperationSensor:

Wait on an Amazon Glacier job state
===================================

To wait on the status of an Amazon Glacier Job to reach a terminal state
use :class:`~airflow.providers.amazon.aws.sensors.glacier.GlacierJobOperationSensor`

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_glacier_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_glacier_job_operation]
    :end-before: [END howto_sensor_glacier_job_operation]

References
----------

* `AWS boto3 library documentation for Amazon Glacier <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html>`__

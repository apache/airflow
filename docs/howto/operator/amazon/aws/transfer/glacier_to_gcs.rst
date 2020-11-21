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


Amazon Glacier Transfer Operator
================================

Amazon Glacier is a secure, durable, and extremely low-cost Amazon S3 cloud storage classes for data archiving and long-term backup.
For more information about the service visit `Amazon Glacier API documentation <https://docs.aws.amazon.com/code-samples/latest/catalog/code-catalog-python-example_code-glacier.html>`_

.. _howto/operator:GlacierToGCSOperator:

GlacierToGCSOperator
^^^^^^^^^^^^^^^^^^^^

Operator task is transfer data from Glacier vault to Google Cloud Storage.

.. note::
    Please be warn that GlacierToGCSOperator may depends on memory usage.
    Transferring big files may not working well.

To get more information about operator visit:
:class:`~airflow.providers.amazon.aws.transfers.glacier_to_gcs.GlacierToGCSOperator`

Example usage:

.. exampleinclude:: /../airflow/providers/amazon/aws/example_dags/example_glacier_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_glacier_transfer_data_to_gcs]
    :end-before: [END howto_glacier_transfer_data_to_gcs]

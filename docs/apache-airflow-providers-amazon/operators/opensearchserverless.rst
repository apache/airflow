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

============================
Amazon OpenSearch Serverless
============================

`Amazon OpenSearch Serverless <https://aws.amazon.com/opensearch-service/>`__  is an
on-demand, auto-scaling configuration for Amazon OpenSearch Service. An OpenSearch
Serverless collection is an OpenSearch cluster that scales compute capacity based on
your application's needs. This contrasts with OpenSearch Service provisioned OpenSearch
domains, which you manually manage capacity for.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Generic Parameters
------------------

.. include:: ../_partials/generic_parameters.rst

Sensors
-------

.. _howto/sensor:OpenSearchServerlessCollectionAvailableSensor:

Wait for an Amazon OpenSearch Serverless Collection to become active
====================================================================

To wait on the state of an Amazon Bedrock customize model job until it reaches a terminal state you can use
:class:`~airflow.providers.amazon.aws.sensors.bedrock.OpenSearchServerlessCollectionActiveSensor`

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_bedrock_retrieve_and_generate.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_opensearch_collection_active]
    :end-before: [END howto_sensor_opensearch_collection_active]

Reference
---------

* `AWS boto3 library documentation for Amazon OpenSearch Service <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/opensearch.html>`__
* `AWS boto3 library documentation for Amazon OpenSearch Serverless <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/opensearchserverless.html>`__

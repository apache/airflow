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

Salesforce To S3 Operator
==============================

.. contents::
  :depth: 1
  :local:

.. _howto/operator:SalesforceToS3Operator:

Overview
--------

Use the
:class:`~airflow.providers.amazon.aws.transfers.salesforce_to_s3.SalesforceToS3Operator`
to execute a Salesforce query to fetch data and upload to S3.  The results of the query
are initially written to a local, temporary directory and then uploaded to an S3 bucket.

Extract Customer Data from Salesforce
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following example demonstrates a use case of extracting customer data from a Salesforce
instance and upload to a "landing" bucket in S3.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_salesforce_to_s3.py
    :language: python
    :start-after: [START howto_operator_salesforce_to_s3_transfer]
    :end-before: [END howto_operator_salesforce_to_s3_transfer]

Reference
---------

This operator uses the :class:`~airflow.providers.salesforce.hooks.salesforce.SalesforceHook`
to interact with Salesforce.  This hook is built with functionality from the Simple Salesforce
package.

For further information, review the `Simple Salesforce Documentation <https://simple-salesforce.readthedocs.io/en/latest/>`__.

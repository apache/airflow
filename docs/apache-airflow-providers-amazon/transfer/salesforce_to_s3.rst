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

=======================
Salesforce to Amazon S3
=======================

Use the ``SalesforceToS3Operator`` transfer to execute a Salesforce query to fetch data and upload to an Amazon Simple
Storage Service (S3) file.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:SalesforceToS3Operator:

Extract data from Salesforce to Amazon S3 transfer operator
===========================================================

The following example demonstrates a use case of extracting account data from a Salesforce
instance and upload to an Amazon S3 bucket.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_salesforce_to_s3.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_salesforce_to_s3]
    :end-before: [END howto_transfer_salesforce_to_s3]

Reference
---------

* `Simple Salesforce Documentation <https://simple-salesforce.readthedocs.io/en/latest/>`__
* `AWS boto3 library documentation for Amazon S3 <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html>`__

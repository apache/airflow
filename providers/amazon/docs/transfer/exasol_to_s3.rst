.. Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements. See the NOTICE file
   distributed with this work for additional details regarding 
   copyright ownership. The ASF licenses this file under the 
   Apache License, Version 2.0 (the "License"); you may not use 
   this file except in compliance with the License. 

   You may obtain a copy of the License at:

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, 
   software distributed under the License is provided on an 
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
   either express or implied. See the License for the specific 
   language governing permissions and limitations under the License.

==================================
Exasol to Amazon S3 Transfer
==================================

The ``ExasolToS3Operator`` allows you to transfer data from an Exasol database 
to an Amazon Simple Storage Service (S3) file.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

For more information about exasol connection in the Airflow, see :ref:`howto/connection:exasol`.

Operators
---------

.. _howto/operator:ExasolToS3Operator:

Exasol to Amazon S3 Transfer Operator
=====================================

This operator extracts query results from an Exasol database and saves them as a file in an Amazon S3 bucket.

For more details about this operator, refer to:
:class:`~airflow.providers.amazon.aws.transfers.exasol_to_s3.ExasolToS3Operator`

Example Usage
-------------

The following example demonstrates how to use ``ExasolToS3Operator``:

.. exampleinclude:: /../../providers/amazon/tests/system/amazon/aws/example_exasol_to_s3.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_exasol_to_s3]
    :end-before: [END howto_transfer_exasol_to_s3]

Reference
---------

* `AWS Boto3 documentation for Amazon S3 <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html>`__
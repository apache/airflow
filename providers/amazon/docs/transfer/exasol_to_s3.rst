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

====================
Exasol to Amazon S3
====================

Use the ``ExasolToS3Operator`` transfer to copy data from an Exasol database into an Amazon Simple Storage
Service (S3) file.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:ExasolToS3Operator:

Exasol To Amazon S3 transfer operator
======================================

This operator exports the result of a SQL query or the full contents of a table from an Exasol database
to a file in an Amazon S3 bucket. Use the ``query_or_table`` parameter to select either a SQL statement to
execute or the name of the table to export.

To get more information about this operator visit:
:class:`~airflow.providers.amazon.aws.transfers.exasol_to_s3.ExasolToS3Operator`

Example usage:

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_exasol_to_s3.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_exasol_to_s3]
    :end-before: [END howto_transfer_exasol_to_s3]

Reference
---------

* `Exasol documentation <https://docs.exasol.com/>`__
* `AWS boto3 library documentation for Amazon S3 <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html>`__

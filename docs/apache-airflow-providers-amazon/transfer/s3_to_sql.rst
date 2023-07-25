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

================
Amazon S3 to SQL
================

Use the ``S3ToSqlOperator`` transfer to copy data from an Amazon Simple Storage Service (S3)
file into an existing SQL table. By providing a parser function which is applied to the
downloaded file, this operator can accept a variety of file formats.


Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:S3ToSqlOperator:

Amazon S3 To SQL Transfer Operator
==================================

To get more information about this operator visit:
:class:`~airflow.providers.amazon.aws.transfers.s3_to_sql.S3ToSqlOperator`

Example usage with a parser for a csv file. This parser loads the
file into memory and returns a list of rows:

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_s3_to_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_s3_to_sql]
    :end-before: [END howto_transfer_s3_to_sql]


Example usage with a parser function that returns a generator.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_s3_to_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_s3_to_sql_generator]
    :end-before: [END howto_transfer_s3_to_sql_generator]


Reference
---------

* `csv.reader documentation <https://docs.python.org/3/library/csv.html>`__

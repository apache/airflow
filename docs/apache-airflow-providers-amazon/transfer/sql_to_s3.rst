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
SQL to Amazon S3
================

Use ``SqlToS3Operator`` to copy data from a SQL server to an Amazon Simple Storage Service (S3) file.
``SqlToS3Operator`` is compatible with any SQL connection as long as the SQL hook has function that
converts the SQL result to `pandas dataframe <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__
(e.g. MySQL, Hive, ...).

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:SqlToS3Operator:

MySQL to Amazon S3 transfer operator
====================================

This example sends the response of a MySQL query to an Amazon S3 file.

To get more information about this operator visit:
:class:`~airflow.providers.amazon.aws.transfers.sql_to_s3.SqlToS3Operator`

Example usage:

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_sql_to_s3.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_sql_to_s3]
    :end-before: [END howto_transfer_sql_to_s3]

Grouping
--------

We can group the data in the table by passing the ``groupby_kwargs`` param. This param accepts a ``dict`` which will be passed to pandas `groupby() <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.groupby.html#pandas.DataFrame.groupby>`_ as kwargs.

Example usage:

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_sql_to_s3.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_sql_to_s3_with_groupby_param]
    :end-before: [END howto_transfer_sql_to_s3_with_groupby_param]

Reference
---------

* `AWS boto3 library documentation for Amazon S3 <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html>`__

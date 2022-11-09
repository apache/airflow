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
Amazon Redshift to Amazon S3
============================

Use the ``RedshiftToS3Operator`` transfer to copy the data from an Amazon Redshift table into an Amazon Simple Storage
Service (S3) file.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:RedshiftToS3Operator:

Amazon Redshift To Amazon S3 transfer operator
==============================================

This operator loads data from an Amazon Redshift table to an existing Amazon S3 bucket.

To get more information about this operator visit:
:class:`~airflow.providers.amazon.aws.transfers.redshift_to_s3.RedshiftToS3Operator`

Example usage:

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_redshift_s3_transfers.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_redshift_to_s3]
    :end-before: [END howto_transfer_redshift_to_s3]

You can find more information to the ``UNLOAD`` command used
`here <https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html>`__.

Reference
---------

* `AWS UNLOAD from Amazon Redshift documentation <https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html>`__
* `AWS boto3 library documentation for Amazon Redshift <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift.html>`__
* `AWS boto3 library documentation for Amazon S3 <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html>`__

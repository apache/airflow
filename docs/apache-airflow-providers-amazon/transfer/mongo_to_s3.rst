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
MongoDB to Amazon S3
====================

Use the ``MongoToS3Operator`` transfer to copy data from a MongoDB collection into an Amazon Simple Storage Service
(S3) file.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:MongoToS3Operator:

MongoDB To Amazon S3 transfer operator
======================================

This operator copies a set of data from a MongoDB collection to an Amazon S3 files.
In order to select the data you want to copy, you need to use the ``mongo_query`` parameter.

To get more information about this operator visit:
:class:`~airflow.providers.amazon.aws.transfers.mongo_to_s3.MongoToS3Operator`

Example usage:

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_mongo_to_s3.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_mongo_to_s3]
    :end-before: [END howto_transfer_mongo_to_s3]

You can find more information about ``PyMongo`` used by Airflow to communicate with MongoDB
`here <https://pymongo.readthedocs.io/en/stable/tutorial.html>`__.

Reference
---------

* `PyMongo <https://pymongo.readthedocs.io/en/stable/tutorial.html>`__
* `AWS boto3 library documentation for Amazon S3 <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html>`__

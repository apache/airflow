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

=================
Amazon S3 Vectors
=================

Amazon S3 Vectors provides native vector storage in Amazon S3 for AI/ML applications.
Use the operators below to manage S3 Vectors resources.

.. _howto/operator:S3VectorsCreateVectorBucketOperator:

Create a Vector Bucket
----------------------

To create an Amazon S3 Vectors vector bucket, use
:class:`~airflow.providers.amazon.aws.operators.s3_vectors.S3VectorsCreateVectorBucketOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_s3_vectors.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_s3vectors_create_vector_bucket]
    :end-before: [END howto_operator_s3vectors_create_vector_bucket]

.. _howto/operator:S3VectorsCreateIndexOperator:

Create an Index
---------------

To create an index in an Amazon S3 Vectors vector bucket, use
:class:`~airflow.providers.amazon.aws.operators.s3_vectors.S3VectorsCreateIndexOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_s3_vectors.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_s3vectors_create_index]
    :end-before: [END howto_operator_s3vectors_create_index]

.. _howto/operator:S3VectorsDeleteIndexOperator:

Delete an Index
---------------

To delete an index from an Amazon S3 Vectors vector bucket, use
:class:`~airflow.providers.amazon.aws.operators.s3_vectors.S3VectorsDeleteIndexOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_s3_vectors.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_s3vectors_delete_index]
    :end-before: [END howto_operator_s3vectors_delete_index]

.. _howto/operator:S3VectorsDeleteVectorBucketOperator:

Delete a Vector Bucket
----------------------

To delete an Amazon S3 Vectors vector bucket, use
:class:`~airflow.providers.amazon.aws.operators.s3_vectors.S3VectorsDeleteVectorBucketOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_s3_vectors.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_s3vectors_delete_vector_bucket]
    :end-before: [END howto_operator_s3vectors_delete_vector_bucket]

Reference
---------

* `AWS boto3 Library Documentation for S3 Vectors <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3vectors.html>`__

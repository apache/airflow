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

=======================================
Amazon Elastic Container Registry (ECR)
=======================================

`Amazon Elastic Container Registry (ECR) <https://aws.amazon.com/ecr/>`__ is a managed container registry
for storing, sharing, and deploying container images and artifacts.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Generic Parameters
------------------

.. include:: ../_partials/generic_parameters.rst

Operators
---------

.. _howto/operator:EcrCreateRepositoryOperator:

Create an ECR repository
========================

To create an ECR repository, use
:class:`~airflow.providers.amazon.aws.operators.ecr.EcrCreateRepositoryOperator`.
This operator can provision a repository before a workflow builds or pushes container images.
The operator returns the complete response from the Boto3 ``create_repository`` API operation.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_ecr.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ecr_create_repository]
    :end-before: [END howto_operator_ecr_create_repository]

.. _howto/operator:EcrSetRepositoryPolicyOperator:

Set an ECR repository policy
============================

To set the repository policy for an ECR repository, use
:class:`~airflow.providers.amazon.aws.operators.ecr.EcrSetRepositoryPolicyOperator`.
This operator can configure permissions such as cross-account access to images stored in the repository.
The operator returns the complete response from the Boto3 ``set_repository_policy`` API operation.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_ecr.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ecr_set_repository_policy]
    :end-before: [END howto_operator_ecr_set_repository_policy]

.. _howto/operator:EcrDeleteRepositoryOperator:

Delete an ECR repository
========================

To delete an ECR repository, use
:class:`~airflow.providers.amazon.aws.operators.ecr.EcrDeleteRepositoryOperator`.
This operator can clean up temporary repositories or repositories that are no longer needed.
Set ``force=True`` to delete a repository that contains images. The operator returns the complete response
from the Boto3 ``delete_repository`` API operation.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_ecr.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ecr_delete_repository]
    :end-before: [END howto_operator_ecr_delete_repository]

Reference
---------

* `AWS Boto3 library documentation for ECR <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecr.html>`__

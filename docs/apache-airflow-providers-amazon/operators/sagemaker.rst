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


Amazon Sagemaker Operators
========================================

Prerequisite Tasks
------------------

.. include:: _partials/prerequisite_tasks.rst

Overview
--------

Airflow to Amazon Sagemaker integration provides several operators to create and interact with
Sagemaker Jobs.

  - :class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerDeleteModelOperator`
  - :class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerModelOperator`
  - :class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerProcessingOperator`
  - :class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerTrainingOperator`
  - :class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerTransformOperator`
  - :class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerTuningOperator`

One example_dag is provided which showcases some of these operators in action.

 - example_sagemaker.py

--------------------------------------------------------------

Purpose
"""""""

This example DAG ``example_sagemaker.py`` uses ``SageMakerProcessingOperator``, ``SageMakerTrainingOperator``,
``SageMakerModelOperator``, ``SageMakerDeleteModelOperator`` and ``SageMakerTransformOperator`` to
create Sagemaker processing job, run the training job,
generate the models artifact in s3, delete the model in case
the model with same name exist, create the model,
and run Sagemaker Batch inference.

Defining tasks
""""""""""""""

In the following code we create a Sagemaker processing, training, batch transform job

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_sagemaker.py
    :language: python
    :start-after: [START howto_operator_sagemaker]
    :end-before: [END howto_operator_sagemaker]

Reference
---------

For further information, look at:

* `Boto3 Library Documentation for Sagemaker <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html>`__

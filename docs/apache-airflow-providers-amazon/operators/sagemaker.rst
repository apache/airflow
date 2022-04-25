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

Amazon SageMaker Operators
==========================

`Amazon SageMaker <https://docs.aws.amazon.com/sagemaker>`__ is a fully managed
machine learning service. With Amazon SageMaker, data scientists and developers
can quickly build and train machine learning models, and then deploy them into a
production-ready hosted environment.

Airflow provides operators to create and interact with SageMaker Jobs.

Prerequisite Tasks
------------------

.. include:: _partials/prerequisite_tasks.rst

Manage Amazon SageMaker Jobs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. _howto/operator:SageMakerProcessingOperator:

Create an Amazon SageMaker Processing Job
"""""""""""""""""""""""""""""""""""""""""

To create an Amazon Sagemaker processing job to sanitize your dataset you can use
:class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerProcessingOperator`.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_processing]
    :end-before: [END howto_operator_sagemaker_processing]


.. _howto/operator:SageMakerTrainingOperator:

Create an Amazon SageMaker Training Job
"""""""""""""""""""""""""""""""""""""""

To create an Amazon Sagemaker training job you can use
:class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerTrainingOperator`.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_training]
    :end-before: [END howto_operator_sagemaker_training]

.. _howto/operator:SageMakerModelOperator:

Create an Amazon SageMaker Model
""""""""""""""""""""""""""""""""

To create an Amazon Sagemaker model you can use
:class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerModelOperator`.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_model]
    :end-before: [END howto_operator_sagemaker_model]

.. _howto/operator:SageMakerTuningOperator:

Start a Hyperparameter Tuning Job
"""""""""""""""""""""""""""""""""

To start a hyperparameter tuning job for an Amazon Sagemaker model you can use
:class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerTuningOperator`.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_tuning]
    :end-before: [END howto_operator_sagemaker_tuning]

.. _howto/operator:SageMakerDeleteModelOperator:

Delete an Amazon SageMaker Model
""""""""""""""""""""""""""""""""

To delete an Amazon Sagemaker model you can use
:class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerDeleteModelOperator`.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_delete_model]
    :end-before: [END howto_operator_sagemaker_delete_model]

.. _howto/operator:SageMakerTransformOperator:

Create an Amazon SageMaker Transform Job
""""""""""""""""""""""""""""""""""""""""

To create an Amazon Sagemaker transform job you can use
:class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerTransformOperator`.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_transform]
    :end-before: [END howto_operator_sagemaker_transform]

.. _howto/operator:SageMakerEndpointConfigOperator:

Create an Amazon SageMaker Endpoint Config Job
""""""""""""""""""""""""""""""""""""""""""""""

To create an Amazon Sagemaker endpoint config job you can use
:class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerEndpointConfigOperator`.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_sagemaker_endpoint.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_endpoint_config]
    :end-before: [END howto_operator_sagemaker_endpoint_config]

.. _howto/operator:SageMakerEndpointOperator:

Create an Amazon SageMaker Endpoint Job
"""""""""""""""""""""""""""""""""""""""

To create an Amazon Sagemaker endpoint you can use
:class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerEndpointOperator`.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_sagemaker_endpoint.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_endpoint]
    :end-before: [END howto_operator_sagemaker_endpoint]


Amazon SageMaker Sensors
^^^^^^^^^^^^^^^^^^^^^^^^

.. _howto/sensor:SageMakerTrainingSensor:

Amazon SageMaker Training Sensor
""""""""""""""""""""""""""""""""

To check the state of an Amazon Sagemaker training job until it reaches a terminal state
you can use :class:`~airflow.providers.amazon.aws.sensors.sagemaker.SageMakerTrainingSensor`.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_training_sensor]
    :end-before: [END howto_operator_sagemaker_training_sensor]

.. _howto/sensor:SageMakerTransformSensor:

Amazon SageMaker Transform Sensor
"""""""""""""""""""""""""""""""""""

To check the state of an Amazon Sagemaker transform job until it reaches a terminal state
you can use :class:`~airflow.providers.amazon.aws.operators.sagemaker.SageMakerTransformOperator`.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_transform_sensor]
    :end-before: [END howto_operator_sagemaker_transform_sensor]

.. _howto/sensor:SageMakerTuningSensor:

Amazon SageMaker Tuning Sensor
""""""""""""""""""""""""""""""

To check the state of an Amazon Sagemaker hyperparameter tuning job until it reaches a terminal state
you can use :class:`~airflow.providers.amazon.aws.sensors.sagemaker.SageMakerTuningSensor`.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_tuning_sensor]
    :end-before: [END howto_operator_sagemaker_tuning_sensor]

.. _howto/sensor:SageMakerEndpointSensor:

Amazon SageMaker Endpoint Sensor
""""""""""""""""""""""""""""""""

To check the state of an Amazon Sagemaker hyperparameter tuning job until it reaches a terminal state
you can use :class:`~airflow.providers.amazon.aws.sensors.sagemaker.SageMakerEndpointSensor`.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_sagemaker_endpoint.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_endpoint_sensor]
    :end-before: [END howto_operator_sagemaker_endpoint_sensor]

Reference
^^^^^^^^^

For further information, look at:

* `Boto3 Library Documentation for Sagemaker <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html>`__
* `Amazon SageMaker Developer Guide <https://docs.aws.amazon.com/sagemaker/latest/dg/whatis.html>`__

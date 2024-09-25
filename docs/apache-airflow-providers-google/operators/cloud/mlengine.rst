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



Google Cloud AI Platform Operators
==================================

`Google Cloud AI Platform <https://cloud.google.com/ai-platform/>`__ (formerly known
as ML Engine) can be used to train machine learning models at scale, host trained models
in the cloud, and use models to make predictions for new data. AI Platform is a collection
of tools for training, evaluating, and tuning machine learning models. AI Platform can also
be used to deploy a trained model, make predictions, and manage various model versions.

The legacy versions of AI Platform Training, AI Platform Prediction, AI Platform Pipelines,
and AI Platform Data Labeling Service are deprecated and will no longer be available on
Google Cloud after their shutdown date. All the functionality of legacy AI Platform and new
features are available on the Vertex AI platform.

Prerequisite tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:MLEngineStartTrainingJobOperator:

Launching a Job
^^^^^^^^^^^^^^^
To start a machine learning operation with AI Platform, you must launch a training job.
This creates a virtual machine that can run code specified in the trainer file, which
contains the main application code. A job can be initiated with the
:class:`~airflow.providers.google.cloud.operators.mlengine.MLEngineStartTrainingJobOperator`.

This operator is deprecated. Please, use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.custom_job.CreateCustomPythonPackageTrainingJobOperator`
instead.

.. exampleinclude:: /../../providers/tests/system/google/cloud/ml_engine/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_custom_python_training_job_v1]
    :end-before: [END howto_operator_create_custom_python_training_job_v1]

.. _howto/operator:MLEngineCreateModelOperator:

Creating a model
^^^^^^^^^^^^^^^^
A model is a container that can hold multiple model versions. A new model can be created through the
:class:`~airflow.providers.google.cloud.operators.mlengine.MLEngineCreateModelOperator`.
The ``model`` field should be defined with a dictionary containing the information about the model.
``name`` is a required field in this dictionary.

This operator is deprecated. The model is created as a result of running Vertex AI operators that create training jobs
of any types. For example, you can use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.custom_job.CreateCustomPythonPackageTrainingJobOperator`.
The result of running this operator will be ready-to-use model saved in Model Registry.

.. exampleinclude:: /../../providers/tests/system/google/cloud/ml_engine/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_custom_python_training_job_v1]
    :end-before: [END howto_operator_create_custom_python_training_job_v1]

.. _howto/operator:MLEngineGetModelOperator:

Getting a model
^^^^^^^^^^^^^^^
The :class:`~airflow.providers.google.cloud.operators.mlengine.MLEngineGetModelOperator`
can be used to obtain a model previously created. To obtain the correct model, ``model_name``
must be defined in the operator.

This operator is deprecated. Please, use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.model_service.GetModelOperator`
instead.

.. exampleinclude:: /../../providers/tests/system/google/cloud/ml_engine/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_mlengine_get_model]
    :end-before: [END howto_operator_gcp_mlengine_get_model]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with the ``project_id`` and ``model``
fields to dynamically determine their values. The result are saved to :ref:`XCom <concepts:xcom>`,
allowing them to be used by other operators. In this case, the
:class:`~airflow.operators.bash.BashOperator` is used to print the model information.

.. exampleinclude:: /../../providers/tests/system/google/cloud/ml_engine/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_mlengine_print_model]
    :end-before: [END howto_operator_gcp_mlengine_print_model]

.. _howto/operator:MLEngineCreateVersionOperator:

Creating model versions
^^^^^^^^^^^^^^^^^^^^^^^
A model version is a subset of the model container where the code runs. A new version of the model can be created
through the :class:`~airflow.providers.google.cloud.operators.mlengine.MLEngineCreateVersionOperator`.
The model must be specified by ``model_name``, and the ``version`` parameter should contain a dictionary of
all the information about the version. Within the ``version`` parameter's dictionary, the ``name`` field is
required.

This operator is deprecated. Please, use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.custom_job.CreateCustomPythonPackageTrainingJobOperator`
instead. In this case, the new version of specific model could be created by specifying existing model id in
``parent_model`` parameter when running Training Job. This will ensure that new version of model will be trained except
of creating new model.

.. exampleinclude:: /../../providers/tests/system/google/cloud/ml_engine/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_custom_python_training_job_v1]
    :end-before: [END howto_operator_create_custom_python_training_job_v1]

The :class:`~airflow.providers.google.cloud.operators.vertex_ai.custom_job.CreateCustomPythonPackageTrainingJobOperator`
can also be used to create more versions with varying parameters.

.. exampleinclude:: /../../providers/tests/system/google/cloud/ml_engine/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_custom_python_training_job_v2]
    :end-before: [END howto_operator_create_custom_python_training_job_v2]

.. _howto/operator:MLEngineSetDefaultVersionOperator:
.. _howto/operator:MLEngineListVersionsOperator:

Managing model versions
^^^^^^^^^^^^^^^^^^^^^^^
By default, the model code will run using the default model version. You can set the model version through the
:class:`~airflow.providers.google.cloud.operators.mlengine.MLEngineSetDefaultVersionOperator`
by specifying the ``model_name`` and ``version_name`` parameters.

This operator is deprecated. Please, use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.model_service.SetDefaultVersionOnModelOperator`
instead. The desired model version to be set as default could be passed with the model ID in ``model_id`` parameter
in format ``projects/{project}/locations/{location}/models/{model_id}@{version_id}`` or
``projects/{project}/locations/{location}/models/{model_id}@{version_alias}``. By default, the first model version
created will be marked as default.

.. exampleinclude:: /../../providers/tests/system/google/cloud/ml_engine/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_mlengine_default_version]
    :end-before: [END howto_operator_gcp_mlengine_default_version]

To list the model versions available, use the
:class:`~airflow.providers.google.cloud.operators.mlengine.MLEngineListVersionsOperator`
while specifying the ``model_name`` parameter.

This operator is deprecated. Please, use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.model_service.ListModelVersionsOperator`
instead. You can pass the name of the desired model in ``model_id`` parameter. If the model ID is passed
with version aliases, the operator will output all the versions available for this model.

.. exampleinclude:: /../../providers/tests/system/google/cloud/ml_engine/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_mlengine_list_versions]
    :end-before: [END howto_operator_gcp_mlengine_list_versions]

.. _howto/operator:MLEngineStartBatchPredictionJobOperator:

Making predictions
^^^^^^^^^^^^^^^^^^
A Google Cloud AI Platform prediction job can be started with the
:class:`~airflow.providers.google.cloud.operators.mlengine.MLEngineStartBatchPredictionJobOperator`.
For specifying the model origin, you need to provide either the ``model_name``, ``uri``, or ``model_name`` and
``version_name``. If you do not provide the ``version_name``, the operator will use the default model version.

This operator is deprecated. Please, use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.batch_prediction_job.CreateBatchPredictionJobOperator`
instead.

.. exampleinclude:: /../../providers/tests/system/google/cloud/ml_engine/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_batch_prediction]
    :end-before: [END howto_operator_start_batch_prediction]

.. _howto/operator:MLEngineDeleteVersionOperator:
.. _howto/operator:MLEngineDeleteModelOperator:

Cleaning up
^^^^^^^^^^^
A model version can be deleted with the
:class:`~airflow.providers.google.cloud.operators.mlengine.MLEngineDeleteVersionOperator` by
the ``version_name`` and ``model_name`` parameters.

This operator is deprecated. Please, use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.model_service.DeleteModelVersionOperator`
instead. The default version could not be deleted on the model.

.. exampleinclude:: /../../providers/tests/system/google/cloud/ml_engine/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_mlengine_delete_version]
    :end-before: [END howto_operator_gcp_mlengine_delete_version]

You can also delete a model with the
:class:`~airflow.providers.google.cloud.operators.mlengine.MLEngineDeleteModelOperator`
by providing the ``model_name`` parameter.

This operator is deprecated. Please, use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.model_service.DeleteModelOperator`
instead.

.. exampleinclude:: /../../providers/tests/system/google/cloud/ml_engine/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_mlengine_delete_model]
    :end-before: [END howto_operator_gcp_mlengine_delete_model]

Evaluating a model
^^^^^^^^^^^^^^^^^^

This function is deprecated. All the functionality of legacy MLEngine and new features are available on
the Vertex AI platform.
To create and view Model Evaluation, please check the documentation:
`Evaluate models using Vertex AI
<https://cloud.google.com/vertex-ai/docs/evaluation/using-model-evaluation#create_an_evaluation>`__

Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://cloud.google.com/ai-platform/prediction/docs/reference/rest>`__
* `Product Documentation <https://cloud.google.com/ai-platform/docs/>`__

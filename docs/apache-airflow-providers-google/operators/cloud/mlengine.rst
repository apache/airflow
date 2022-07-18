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

Prerequisite tasks
^^^^^^^^^^^^^^^^^^

.. include:: ../_partials/prerequisite_tasks.rst

.. _howto/operator:MLEngineStartTrainingJobOperator:

Launching a Job
^^^^^^^^^^^^^^^
To start a machine learning operation with AI Platform, you must launch a training job.
This creates a virtual machine that can run code specified in the trainer file, which
contains the main application code. A job can be initiated with the
:class:`~airflow.providers.google.cloud.operators.mlengine.MLEngineStartTrainingJobOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_mlengine_training]
    :end-before: [END howto_operator_gcp_mlengine_training]

.. _howto/operator:MLEngineCreateModelOperator:

Creating a model
^^^^^^^^^^^^^^^^
A model is a container that can hold multiple model versions. A new model can be created through the
:class:`~airflow.providers.google.cloud.operators.mlengine.MLEngineCreateModelOperator`.
The ``model`` field should be defined with a dictionary containing the information about the model.
``name`` is a required field in this dictionary.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_mlengine_create_model]
    :end-before: [END howto_operator_gcp_mlengine_create_model]

.. _howto/operator:MLEngineGetModelOperator:

Getting a model
^^^^^^^^^^^^^^^
The :class:`~airflow.providers.google.cloud.operators.mlengine.MLEngineGetModelOperator`
can be used to obtain a model previously created. To obtain the correct model, ``model_name``
must be defined in the operator.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_mlengine_get_model]
    :end-before: [END howto_operator_gcp_mlengine_get_model]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with the ``project_id`` and ``model``
fields to dynamically determine their values. The result are saved to :ref:`XCom <concepts:xcom>`,
allowing them to be used by other operators. In this case, the
:class:`~airflow.operators.bash.BashOperator` is used to print the model information.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_mlengine.py
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

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_mlengine_create_version1]
    :end-before: [END howto_operator_gcp_mlengine_create_version1]

The :class:`~airflow.providers.google.cloud.operators.mlengine.MLEngineCreateVersionOperator`
can also be used to create more versions with varying parameters.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_mlengine_create_version2]
    :end-before: [END howto_operator_gcp_mlengine_create_version2]

.. _howto/operator:MLEngineSetDefaultVersionOperator:
.. _howto/operator:MLEngineListVersionsOperator:

Managing model versions
^^^^^^^^^^^^^^^^^^^^^^^
By default, the model code will run using the default model version. You can set the model version through the
:class:`~airflow.providers.google.cloud.operators.mlengine.MLEngineSetDefaultVersionOperator`
by specifying the ``model_name`` and ``version_name`` parameters.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_mlengine_default_version]
    :end-before: [END howto_operator_gcp_mlengine_default_version]

To list the model versions available, use the
:class:`~airflow.providers.google.cloud.operators.mlengine.MLEngineListVersionsOperator`
while specifying the ``model_name`` parameter.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_mlengine_list_versions]
    :end-before: [END howto_operator_gcp_mlengine_list_versions]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with the ``project_id`` and ``model``
fields to dynamically determine their values. The result are saved to :ref:`XCom <concepts:xcom>`,
allowing them to be used by other operators. In this case, the
:class:`~airflow.operators.bash.BashOperator` is used to print the version information.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_mlengine_print_versions]
    :end-before: [END howto_operator_gcp_mlengine_print_versions]

.. _howto/operator:MLEngineStartBatchPredictionJobOperator:

Making predictions
^^^^^^^^^^^^^^^^^^
A Google Cloud AI Platform prediction job can be started with the
:class:`~airflow.providers.google.cloud.operators.mlengine.MLEngineStartBatchPredictionJobOperator`.
For specifying the model origin, you need to provide either the ``model_name``, ``uri``, or ``model_name`` and
``version_name``. If you do not provide the ``version_name``, the operator will use the default model version.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_mlengine_get_prediction]
    :end-before: [END howto_operator_gcp_mlengine_get_prediction]

.. _howto/operator:MLEngineDeleteVersionOperator:
.. _howto/operator:MLEngineDeleteModelOperator:

Cleaning up
^^^^^^^^^^^
A model version can be deleted with the
:class:`~airflow.providers.google.cloud.operators.mlengine.MLEngineDeleteVersionOperator` by
the ``version_name`` and ``model_name`` parameters.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_mlengine_delete_version]
    :end-before: [END howto_operator_gcp_mlengine_delete_version]

You can also delete a model with the
:class:`~airflow.providers.google.cloud.operators.mlengine.MLEngineDeleteModelOperator`
by providing the ``model_name`` parameter.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_mlengine_delete_model]
    :end-before: [END howto_operator_gcp_mlengine_delete_model]

Evaluating a model
^^^^^^^^^^^^^^^^^^
To evaluate a prediction and model, specify a metric function to generate a summary and customize
the evaluation of the model. This function receives a dictionary derived from a json in the batch
prediction result, then returns a tuple of metrics.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_mlengine_get_metric]
    :end-before: [END howto_operator_gcp_mlengine_get_metric]

To evaluate a prediction and model, it's useful to have a function to validate the summary result.
This function receives a dictionary of the averaged metrics the function above generated. It then
raises an exception if a task fails or should not proceed.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_mlengine_validate_error]
    :end-before: [END howto_operator_gcp_mlengine_validate_error]

Prediction results and a model summary can be generated through a function such as
:class:`~airflow.providers.google.cloud.utils.mlengine_operator_utils.create_evaluate_ops`.
It makes predictions using the specified inputs and then summarizes and validates the result. The
functions created above should be passed in through the ``metric_fn_and_keys`` and ``validate_fn`` fields.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_mlengine_evaluate]
    :end-before: [END howto_operator_gcp_mlengine_evaluate]

Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://cloud.google.com/ai-platform/prediction/docs/reference/rest>`__
* `Product Documentation <https://cloud.google.com/ai-platform/docs/>`__

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

Google Cloud AutoML Operators
=============================

.. warning::
  The AutoML API is deprecated. Planned removal date is September 30, 2025, but some operators might be deleted
  earlier, according to the docs and deprecation warnings!
  The replacement suggestions can be found in the deprecation warnings or in the doc below.
  Please note that AutoML for translation API functionality has been moved to the Advanced Translation service,
  the operators can be found at ``airflow.providers.google.cloud.operators.translate`` module.

The `Google Cloud AutoML <https://cloud.google.com/automl/docs/>`__
makes the power of machine learning available to you even if you have limited knowledge
of machine learning. You can use AutoML to build on Google's machine learning capabilities
to create your own custom machine learning models that are tailored to your business needs,
and then integrate those models into your applications and web sites.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:CloudAutoMLDocuments:
.. _howto/operator:AutoMLCreateDatasetOperator:
.. _howto/operator:AutoMLImportDataOperator:
.. _howto/operator:AutoMLTablesUpdateDatasetOperator:

Creating Datasets
^^^^^^^^^^^^^^^^^

To create a Google AutoML dataset you can use
:class:`~airflow.providers.google.cloud.operators.automl.AutoMLCreateDatasetOperator`.
The operator returns dataset id in :ref:`XCom <concepts:xcom>` under ``dataset_id`` key.

This operator is deprecated when running for text, video and vision prediction and will be removed after September 30, 2025.
All the functionality of legacy AutoML Natural Language, Vision, Video Intelligence and new features are
available on the Vertex AI platform. Please use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.dataset.CreateDatasetOperator`
:class:`~airflow.providers.google.cloud.operators.translate.TranslateCreateDatasetOperator`.

After creating a dataset you can use it to import some data using
:class:`~airflow.providers.google.cloud.operators.automl.AutoMLImportDataOperator`.

To update dataset you can use
:class:`~airflow.providers.google.cloud.operators.automl.AutoMLTablesUpdateDatasetOperator`.

.. warning::
    This operator is deprecated when running for text, video and vision prediction and will be removed soon.
    All the functionality of legacy AutoML Natural Language, Vision, Video Intelligence and new features are
    available on the Vertex AI platform. Please use
    :class:`~airflow.providers.google.cloud.operators.vertex_ai.dataset.UpdateDatasetOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/vertex_ai/example_vertex_ai_dataset.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_update_dataset_operator]
    :end-before: [END how_to_cloud_vertex_ai_update_dataset_operator]

.. _howto/operator:AutoMLTablesListTableSpecsOperator:
.. _howto/operator:AutoMLTablesListColumnSpecsOperator:

Listing Table And Columns Specs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To list table specs you can use
:class:`~airflow.providers.google.cloud.operators.automl.AutoMLTablesListTableSpecsOperator`.

To list column specs you can use
:class:`~airflow.providers.google.cloud.operators.automl.AutoMLTablesListColumnSpecsOperator`.

AutoML Tables related operators are deprecated. Please use related Vertex AI Tabular operators.

.. _howto/operator:AutoMLTrainModelOperator:
.. _howto/operator:AutoMLGetModelOperator:
.. _howto/operator:AutoMLDeployModelOperator:
.. _howto/operator:AutoMLDeleteModelOperator:

Operations On Models
^^^^^^^^^^^^^^^^^^^^

To create a Google AutoML model you can use
:class:`~airflow.providers.google.cloud.operators.automl.AutoMLTrainModelOperator`.
The operator will wait for the operation to complete. Additionally the operator
returns the id of model in :ref:`XCom <concepts:xcom>` under ``model_id`` key.

.. warning::
    This operator is deprecated when running for text, video and vision prediction and will be removed after September 30, 2025.
    All the functionality of legacy AutoML Natural Language, Vision, Video Intelligence and new features are
    available on the Vertex AI platform. Please use
    :class:`~airflow.providers.google.cloud.operators.vertex_ai.auto_ml.CreateAutoMLTabularTrainingJobOperator`,
    :class:`~airflow.providers.google.cloud.operators.vertex_ai.auto_ml.CreateAutoMLVideoTrainingJobOperator`,
    :class:`~airflow.providers.google.cloud.operators.vertex_ai.auto_ml.CreateAutoMLImageTrainingJobOperator`,
    :class:`~airflow.providers.google.cloud.operators.vertex_ai.generative_model.SupervisedFineTuningTrainOperator`,
    :class:`~airflow.providers.google.cloud.operators.translate.TranslateCreateModelOperator`.

When running Vertex AI Operator for training data, please ensure that your data is correctly stored in Vertex AI
datasets. To create and import data to the dataset please use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.dataset.CreateDatasetOperator`
and
:class:`~airflow.providers.google.cloud.operators.vertex_ai.dataset.ImportDataOperator`

For the AutoML translation please use the
:class:`~airflow.providers.google.cloud.operators.translate.TranslateTextOperator`
or
:class:`~airflow.providers.google.cloud.operators.translate.TranslateTextBatchOperator`.

To get existing model one can use
:class:`~airflow.providers.google.cloud.operators.automl.AutoMLGetModelOperator`.

This operator deprecated for tables, video intelligence, vision and natural language is deprecated
and will be removed after 31.03.2024. Please use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.model_service.GetModelOperator` instead.
You can find example on how to use VertexAI operators here:

.. exampleinclude:: /../../google/tests/system/google/cloud/vertex_ai/example_vertex_ai_model_service.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_get_model_operator]
    :end-before: [END how_to_cloud_vertex_ai_get_model_operator]

Once a model is created it could be deployed using
:class:`~airflow.providers.google.cloud.operators.automl.AutoMLDeployModelOperator`.

This operator deprecated for tables, video intelligence, vision and natural language is deprecated
and will be removed after 31.03.2024. Please use
:class:`airflow.providers.google.cloud.operators.vertex_ai.endpoint_service.DeployModelOperator` instead.
You can find example on how to use VertexAI operators here:

.. exampleinclude:: /../../google/tests/system/google/cloud/vertex_ai/example_vertex_ai_endpoint.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_deploy_model_operator]
    :end-before: [END how_to_cloud_vertex_ai_deploy_model_operator]

If you wish to delete a model you can use
:class:`~airflow.providers.google.cloud.operators.automl.AutoMLDeleteModelOperator`.

This operator deprecated for tables, video intelligence, vision and natural language is deprecated
and will be removed after 31.03.2024. Please use
:class:`airflow.providers.google.cloud.operators.vertex_ai.model_service.DeleteModelOperator` instead.
You can find example on how to use VertexAI operators here:

.. exampleinclude:: /../../google/tests/system/google/cloud/vertex_ai/example_vertex_ai_model_service.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_delete_model_operator]
    :end-before: [END how_to_cloud_vertex_ai_delete_model_operator]

.. _howto/operator:AutoMLPredictOperator:

Making Predictions
^^^^^^^^^^^^^^^^^^

To obtain predictions from Google Cloud AutoML model you can use
:class:`~airflow.providers.google.cloud.operators.automl.AutoMLPredictOperator`. In the first case
the model must be deployed.


For tables, video intelligence, vision and natural language you can use the following operators:

:class:`airflow.providers.google.cloud.operators.vertex_ai.batch_prediction_job.CreateBatchPredictionJobOperator`,
:class:`airflow.providers.google.cloud.operators.vertex_ai.batch_prediction_job.GetBatchPredictionJobOperator`,
:class:`airflow.providers.google.cloud.operators.vertex_ai.batch_prediction_job.ListBatchPredictionJobsOperator`,
:class:`airflow.providers.google.cloud.operators.vertex_ai.batch_prediction_job.DeleteBatchPredictionJobOperator`.
You can find examples on how to use VertexAI operators here:

.. exampleinclude:: /../../google/tests/system/google/cloud/vertex_ai/example_vertex_ai_batch_prediction_job.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_create_batch_prediction_job_operator]
    :end-before: [END how_to_cloud_vertex_ai_create_batch_prediction_job_operator]

.. exampleinclude:: /../../google/tests/system/google/cloud/vertex_ai/example_vertex_ai_batch_prediction_job.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_list_batch_prediction_job_operator]
    :end-before: [END how_to_cloud_vertex_ai_list_batch_prediction_job_operator]

.. exampleinclude:: /../../google/tests/system/google/cloud/vertex_ai/example_vertex_ai_batch_prediction_job.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_delete_batch_prediction_job_operator]
    :end-before: [END how_to_cloud_vertex_ai_delete_batch_prediction_job_operator]

.. _howto/operator:AutoMLListDatasetOperator:
.. _howto/operator:AutoMLDeleteDatasetOperator:

Listing And Deleting Datasets
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can get a list of AutoML datasets using
:class:`~airflow.providers.google.cloud.operators.automl.AutoMLListDatasetOperator`. The operator returns list
of datasets ids in :ref:`XCom <concepts:xcom>` under ``dataset_id_list`` key.

This operator deprecated for tables, video intelligence, vision and natural language is deprecated
and will be removed after 31.03.2024. Please use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.dataset.ListDatasetsOperator`,
:class:`~airflow.providers.google.cloud.operators.translate.TranslateDatasetsListOperator`
instead.
You can find example on how to use VertexAI operators here:

.. exampleinclude:: /../../google/tests/system/google/cloud/vertex_ai/example_vertex_ai_dataset.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_list_dataset_operator]
    :end-before: [END how_to_cloud_vertex_ai_list_dataset_operator]

To delete a dataset you can use :class:`~airflow.providers.google.cloud.operators.automl.AutoMLDeleteDatasetOperator`.
The delete operator allows also to pass list or coma separated string of datasets ids to be deleted.

This operator deprecated for tables, video intelligence, vision and natural language is deprecated
and will be removed after 31.03.2024. Please use
:class:`airflow.providers.google.cloud.operators.vertex_ai.dataset.DeleteDatasetOperator` instead.
You can find example on how to use VertexAI operators here:

.. exampleinclude:: /../../google/tests/system/google/cloud/vertex_ai/example_vertex_ai_dataset.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_delete_dataset_operator]
    :end-before: [END how_to_cloud_vertex_ai_delete_dataset_operator]

Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://googleapis.github.io/google-cloud-python/latest/automl/index.html>`__
* `Product Documentation <https://cloud.google.com/automl/docs/>`__

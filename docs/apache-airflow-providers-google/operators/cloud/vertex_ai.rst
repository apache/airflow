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

Google Cloud VertexAI Operators
=======================================

The `Google Cloud VertexAI <https://cloud.google.com/vertex-ai/docs>`__
brings AutoML and AI Platform together into a unified API, client library, and user
interface. AutoML lets you train models on image, tabular, text, and video datasets
without writing code, while training in AI Platform lets you run custom training code.
With Vertex AI, both AutoML training and custom training are available options.
Whichever option you choose for training, you can save models, deploy models, and
request predictions with Vertex AI.

Creating Datasets
^^^^^^^^^^^^^^^^^

To create a Google VertexAI dataset you can use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.dataset.CreateDatasetOperator`.
The operator returns dataset id in :ref:`XCom <concepts:xcom>` under ``dataset_id`` key.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_dataset.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_create_dataset_operator]
    :end-before: [END how_to_cloud_vertex_ai_create_dataset_operator]

After creating a dataset you can use it to import some data using
:class:`~airflow.providers.google.cloud.operators.vertex_ai.dataset.ImportDataOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_dataset.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_import_data_operator]
    :end-before: [END how_to_cloud_vertex_ai_import_data_operator]

To export dataset you can use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.dataset.ExportDataOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_dataset.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_export_data_operator]
    :end-before: [END how_to_cloud_vertex_ai_export_data_operator]

To delete dataset you can use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.dataset.DeleteDatasetOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_dataset.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_delete_dataset_operator]
    :end-before: [END how_to_cloud_vertex_ai_delete_dataset_operator]

To get dataset you can use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.dataset.GetDatasetOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_dataset.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_get_dataset_operator]
    :end-before: [END how_to_cloud_vertex_ai_get_dataset_operator]

To get a dataset list you can use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.dataset.ListDatasetsOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_dataset.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_list_dataset_operator]
    :end-before: [END how_to_cloud_vertex_ai_list_dataset_operator]

To update dataset you can use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.dataset.UpdateDatasetOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_dataset.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_update_dataset_operator]
    :end-before: [END how_to_cloud_vertex_ai_update_dataset_operator]

Creating a Training Jobs
^^^^^^^^^^^^^^^^^^^^^^^^

To create a Google Vertex AI training jobs you have three operators
:class:`~airflow.providers.google.cloud.operators.vertex_ai.custom_job.CreateCustomContainerTrainingJobOperator`,
:class:`~airflow.providers.google.cloud.operators.vertex_ai.custom_job.CreateCustomPythonPackageTrainingJobOperator`,
:class:`~airflow.providers.google.cloud.operators.vertex_ai.custom_job.CreateCustomTrainingJobOperator`.
Each of them will wait for the operation to complete. The results of each operator will be a model
which was trained by user using these operators.

Preparation step

For each operator you must prepare and create dataset. Then put dataset id to ``dataset_id`` parameter in operator.

How to run Container Training Job
:class:`~airflow.providers.google.cloud.operators.vertex_ai.custom_job.CreateCustomContainerTrainingJobOperator`

Before start running this Job you should create a docker image with training script inside. Documentation how to
create image you can find by this link: https://cloud.google.com/vertex-ai/docs/training/create-custom-container
After that you should put link to the image in ``container_uri`` parameter. Also you can type executing command
for container which will be created from this image in ``command`` parameter.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_custom_container.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_create_custom_container_training_job_operator]
    :end-before: [END how_to_cloud_vertex_ai_create_custom_container_training_job_operator]

How to run Python Package Training Job
:class:`~airflow.providers.google.cloud.operators.vertex_ai.custom_job.CreateCustomPythonPackageTrainingJobOperator`

Before start running this Job you should create a python package with training script inside. Documentation how to
create you can find by this link: https://cloud.google.com/vertex-ai/docs/training/create-python-pre-built-container
Next you should put link to the package in ``python_package_gcs_uri`` parameter, also ``python_module_name``
parameter should has the name of script which will run your training task.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_custom_job_python_package.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_create_custom_python_package_training_job_operator]
    :end-before: [END how_to_cloud_vertex_ai_create_custom_python_package_training_job_operator]

How to run Training Job
:class:`~airflow.providers.google.cloud.operators.vertex_ai.custom_job.CreateCustomTrainingJobOperator`.

For this Job you should put path to your local training script inside ``script_path`` parameter.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_custom_job.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_create_custom_training_job_operator]
    :end-before: [END how_to_cloud_vertex_ai_create_custom_training_job_operator]

You can get a list of Training Jobs using
:class:`~airflow.providers.google.cloud.operators.vertex_ai.custom_job.ListCustomTrainingJobOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_list_custom_jobs.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_list_custom_training_job_operator]
    :end-before: [END how_to_cloud_vertex_ai_list_custom_training_job_operator]

If you wish to delete a Custom Training Job you can use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.custom_job.DeleteCustomTrainingJobOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_custom_job.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_delete_custom_training_job_operator]
    :end-before: [END how_to_cloud_vertex_ai_delete_custom_training_job_operator]

Creating an AutoML Training Jobs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To create a Google Vertex AI Auto ML training jobs you have five operators
:class:`~airflow.providers.google.cloud.operators.vertex_ai.auto_ml.CreateAutoMLForecastingTrainingJobOperator`
:class:`~airflow.providers.google.cloud.operators.vertex_ai.auto_ml.CreateAutoMLImageTrainingJobOperator`
:class:`~airflow.providers.google.cloud.operators.vertex_ai.auto_ml.CreateAutoMLTabularTrainingJobOperator`
:class:`~airflow.providers.google.cloud.operators.vertex_ai.auto_ml.CreateAutoMLTextTrainingJobOperator`
:class:`~airflow.providers.google.cloud.operators.vertex_ai.auto_ml.CreateAutoMLVideoTrainingJobOperator`
Each of them will wait for the operation to complete. The results of each operator will be a model
which was trained by user using these operators.

How to run AutoML Forecasting Training Job
:class:`~airflow.providers.google.cloud.operators.vertex_ai.auto_ml.CreateAutoMLForecastingTrainingJobOperator`

Before start running this Job you must prepare and create ``TimeSeries`` dataset. After that you should
put dataset id to ``dataset_id`` parameter in operator.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_auto_ml_forecasting_training.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_create_auto_ml_forecasting_training_job_operator]
    :end-before: [END how_to_cloud_vertex_ai_create_auto_ml_forecasting_training_job_operator]

How to run AutoML Image Training Job
:class:`~airflow.providers.google.cloud.operators.vertex_ai.auto_ml.CreateAutoMLImageTrainingJobOperator`

Before start running this Job you must prepare and create ``Image`` dataset. After that you should
put dataset id to ``dataset_id`` parameter in operator.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_auto_ml_image_training.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_create_auto_ml_image_training_job_operator]
    :end-before: [END how_to_cloud_vertex_ai_create_auto_ml_image_training_job_operator]

How to run AutoML Tabular Training Job
:class:`~airflow.providers.google.cloud.operators.vertex_ai.auto_ml.CreateAutoMLTabularTrainingJobOperator`

Before start running this Job you must prepare and create ``Tabular`` dataset. After that you should
put dataset id to ``dataset_id`` parameter in operator.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_auto_ml_tabular_training.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_create_auto_ml_tabular_training_job_operator]
    :end-before: [END how_to_cloud_vertex_ai_create_auto_ml_tabular_training_job_operator]

How to run AutoML Text Training Job
:class:`~airflow.providers.google.cloud.operators.vertex_ai.auto_ml.CreateAutoMLTextTrainingJobOperator`

Before start running this Job you must prepare and create ``Text`` dataset. After that you should
put dataset id to ``dataset_id`` parameter in operator.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_auto_ml_text_training.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_create_auto_ml_text_training_job_operator]
    :end-before: [END how_to_cloud_vertex_ai_create_auto_ml_text_training_job_operator]

How to run AutoML Video Training Job
:class:`~airflow.providers.google.cloud.operators.vertex_ai.auto_ml.CreateAutoMLVideoTrainingJobOperator`

Before start running this Job you must prepare and create ``Video`` dataset. After that you should
put dataset id to ``dataset_id`` parameter in operator.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_auto_ml_video_training.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_create_auto_ml_video_training_job_operator]
    :end-before: [END how_to_cloud_vertex_ai_create_auto_ml_video_training_job_operator]

You can get a list of AutoML Training Jobs using
:class:`~airflow.providers.google.cloud.operators.vertex_ai.auto_ml.ListAutoMLTrainingJobOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_auto_ml_list_training.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_list_auto_ml_training_job_operator]
    :end-before: [END how_to_cloud_vertex_ai_list_auto_ml_training_job_operator]

If you wish to delete a Auto ML Training Job you can use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.auto_ml.DeleteAutoMLTrainingJobOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_auto_ml_forecasting_training.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_delete_auto_ml_training_job_operator]
    :end-before: [END how_to_cloud_vertex_ai_delete_auto_ml_training_job_operator]

Creating a Batch Prediction Jobs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To create a Google VertexAI Batch Prediction Job you can use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.batch_prediction_job.CreateBatchPredictionJobOperator`.
The operator returns batch prediction job id in :ref:`XCom <concepts:xcom>` under ``batch_prediction_job_id`` key.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_batch_prediction_job.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_create_batch_prediction_job_operator]
    :end-before: [END how_to_cloud_vertex_ai_create_batch_prediction_job_operator]

To delete batch prediction job you can use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.batch_prediction_job.DeleteBatchPredictionJobOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_batch_prediction_job.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_delete_batch_prediction_job_operator]
    :end-before: [END how_to_cloud_vertex_ai_delete_batch_prediction_job_operator]

To get a batch prediction job list you can use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.batch_prediction_job.ListBatchPredictionJobsOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_batch_prediction_job.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_list_batch_prediction_job_operator]
    :end-before: [END how_to_cloud_vertex_ai_list_batch_prediction_job_operator]

Creating an Endpoint Service
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To create a Google VertexAI endpoint you can use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.endpoint_service.CreateEndpointOperator`.
The operator returns endpoint id in :ref:`XCom <concepts:xcom>` under ``endpoint_id`` key.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_endpoint.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_create_endpoint_operator]
    :end-before: [END how_to_cloud_vertex_ai_create_endpoint_operator]

After creating an endpoint you can use it to deploy some model using
:class:`~airflow.providers.google.cloud.operators.vertex_ai.endpoint_service.DeployModelOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_endpoint.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_deploy_model_operator]
    :end-before: [END how_to_cloud_vertex_ai_deploy_model_operator]

To un deploy model you can use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.endpoint_service.UndeployModelOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_endpoint.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_undeploy_model_operator]
    :end-before: [END how_to_cloud_vertex_ai_undeploy_model_operator]

To delete endpoint you can use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.endpoint_service.DeleteEndpointOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_endpoint.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_delete_endpoint_operator]
    :end-before: [END how_to_cloud_vertex_ai_delete_endpoint_operator]

To get an endpoint list you can use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.endpoint_service.ListEndpointsOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_endpoint.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_list_endpoints_operator]
    :end-before: [END how_to_cloud_vertex_ai_list_endpoints_operator]

Creating a Hyperparameter Tuning Jobs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To create a Google VertexAI hyperparameter tuning job you can use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.hyperparameter_tuning_job.CreateHyperparameterTuningJobOperator`.
The operator returns hyperparameter tuning job id in :ref:`XCom <concepts:xcom>` under ``hyperparameter_tuning_job_id`` key.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_hyperparameter_tuning_job.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_create_hyperparameter_tuning_job_operator]
    :end-before: [END how_to_cloud_vertex_ai_create_hyperparameter_tuning_job_operator]

To delete hyperparameter tuning job you can use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.hyperparameter_tuning_job.DeleteHyperparameterTuningJobOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_hyperparameter_tuning_job.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_delete_hyperparameter_tuning_job_operator]
    :end-before: [END how_to_cloud_vertex_ai_delete_hyperparameter_tuning_job_operator]

To get hyperparameter tuning job you can use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.hyperparameter_tuning_job.GetHyperparameterTuningJobOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_hyperparameter_tuning_job.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_get_hyperparameter_tuning_job_operator]
    :end-before: [END how_to_cloud_vertex_ai_get_hyperparameter_tuning_job_operator]

To get a hyperparameter tuning job list you can use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.hyperparameter_tuning_job.ListHyperparameterTuningJobOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_hyperparameter_tuning_job.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_list_hyperparameter_tuning_job_operator]
    :end-before: [END how_to_cloud_vertex_ai_list_hyperparameter_tuning_job_operator]

Creating a Model Service
^^^^^^^^^^^^^^^^^^^^^^^^

To upload a Google VertexAI model you can use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.model_service.UploadModelOperator`.
The operator returns model id in :ref:`XCom <concepts:xcom>` under ``model_id`` key.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_model_service.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_upload_model_operator]
    :end-before: [END how_to_cloud_vertex_ai_upload_model_operator]

To export model you can use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.model_service.ExportModelOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_model_service.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_export_model_operator]
    :end-before: [END how_to_cloud_vertex_ai_export_model_operator]

To delete model you can use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.model_service.DeleteModelOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_model_service.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_delete_model_operator]
    :end-before: [END how_to_cloud_vertex_ai_delete_model_operator]

To get a model list you can use
:class:`~airflow.providers.google.cloud.operators.vertex_ai.model_service.ListModelsOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/vertex_ai/example_vertex_ai_model_service.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_vertex_ai_list_models_operator]
    :end-before: [END how_to_cloud_vertex_ai_list_models_operator]

Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://googleapis.dev/python/aiplatform/latest/index.html>`__
* `Product Documentation <https://cloud.google.com/ai-platform/docs>`__

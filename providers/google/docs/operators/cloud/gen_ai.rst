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

Google Cloud Generative AI Operators
====================================

The Google Cloud Generative AI Operators ecosystem is anchored by the Gemini family of multimodal models, which
provide interfaces for generating and processing diverse inputs like text, images, and audio. By leveraging
these foundation models, developers can securely prompt, tune, and ground AI using their own proprietary data.
This capability enables the construction of versatile applications, ranging from custom chatbots and code assistants to
automated content summarization tools.


Interacting with Generative AI on Vertex AI
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The `Google Cloud VertexAI <https://cloud.google.com/vertex-ai/generative-ai/docs/>`__
extends Vertex AI with powerful foundation models capable of generating text, images, and other modalities.
It provides access to Google Gemini family of multimodal models and other pre-trained generative models through
a unified API, SDK, and console. Developers can prompt, tune, and ground these models using their own data to build
applications such as chat bots, content creation tools, code assistants, and summarization systems.
With Vertex AI, you can securely integrate generative capabilities into enterprise workflows, monitor usage,
evaluate model quality, and deploy models at scale — all within the same managed ML platform.

To generate text embeddings you can use
:class:`~airflow.providers.google.cloud.operators.gen_ai.GenAIGenerateEmbeddingsOperator`.
The operator returns the model's response in :ref:`XCom <concepts:xcom>` under ``model_response`` key.

.. exampleinclude:: /../../google/tests/system/google/cloud/gen_ai/example_gen_ai_generative_model.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_gen_ai_generate_embeddings_task]
    :end-before: [END how_to_cloud_gen_ai_generate_embeddings_task]


To generate content with a generative model you can use
:class:`~airflow.providers.google.cloud.operators.gen_ai.GenAIGenerateContentOperator`.
The operator returns the model's response in :ref:`XCom <concepts:xcom>` under ``model_response`` key.

.. exampleinclude:: /../../google/tests/system/google/cloud/gen_ai/example_gen_ai_generative_model.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_gen_ai_generate_content_operator]
    :end-before: [END how_to_cloud_gen_ai_generate_content_operator]


To run a supervised fine tuning job you can use
:class:`~airflow.providers.google.cloud.operators.gen_ai.GenAISupervisedFineTuningTrainOperator`.
The operator returns the tuned model's endpoint name in :ref:`XCom <concepts:xcom>` under ``tuned_model_endpoint_name`` key.

.. exampleinclude:: /../../google/tests/system/google/cloud/gen_ai/example_gen_ai_generative_model_tuning.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_gen_ai_supervised_fine_tuning_train_operator]
    :end-before: [END how_to_cloud_gen_ai_supervised_fine_tuning_train_operator]

You can also use supervised fine tuning job for video tasks (training and tracking):

.. exampleinclude:: /../../google/tests/system/google/cloud/gen_ai/example_gen_ai_generative_model_tuning.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_gen_ai_supervised_fine_tuning_train_operator_for_video]
    :end-before: [END how_to_cloud_gen_ai_supervised_fine_tuning_train_operator_for_video]


To calculates the number of input tokens before sending a request to the Gemini API you can use:
:class:`~airflow.providers.google.cloud.operators.gen_ai.GenAICountTokensOperator`.
The operator returns the total tokens in :ref:`XCom <concepts:xcom>` under ``total_tokens`` key.

.. exampleinclude:: /../../google/tests/system/google/cloud/gen_ai/example_gen_ai_generative_model.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_gen_ai_count_tokens_operator]
    :end-before: [END how_to_cloud_gen_ai_count_tokens_operator]


To create cached content you can use
:class:`~airflow.providers.google.cloud.operators.gen_ai.GenAICreateCachedContentOperator`.
The operator returns the cached content resource name in :ref:`XCom <concepts:xcom>` under ``cached_content`` key.

.. exampleinclude:: /../../google/tests/system/google/cloud/gen_ai/example_gen_ai_generative_model.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_gen_ai_create_cached_content_operator]
    :end-before: [END how_to_cloud_gen_ai_create_cached_content_operator]


To generate a response from cached content you can use
:class:`~airflow.providers.google.cloud.operators.gen_ai.GenAIGenerateContentOperator`.
The operator returns the cached content response in :ref:`XCom <concepts:xcom>` under ``model_response`` key.

.. exampleinclude:: /../../google/tests/system/google/cloud/gen_ai/example_gen_ai_generative_model.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_gen_ai_generate_from_cached_content_operator]
    :end-before: [END how_to_cloud_gen_ai_generate_from_cached_content_operator]

Interacting with Gemini Batch API
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The `Gemini Batch API <https://ai.google.dev/gemini-api/docs/batch-api>`__ is designed to process large volumes
of requests asynchronously at 50% of the standard cost. The target turnaround time is 24 hours,
but in majority of cases, it is much quicker. Use Batch API for large-scale, non-urgent tasks such as
data pre-processing or running evaluations where an immediate response is not required.

Create batch job
""""""""""""""""

To create batch job via Batch API you can use
:class:`~airflow.providers.google.cloud.operators.gen_ai.GenAIGeminiCreateBatchJobOperator`.
The operator returns the job name in :ref:`XCom <concepts:xcom>` under ``job_name`` key.

Two option of input source is allowed: inline requests, file.

If you use inline requests take a look at this example:

.. exampleinclude:: /../../google/tests/system/google/cloud/gen_ai/example_gen_ai_gemini_batch_api.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_gen_ai_batch_api_create_batch_job_with_inlined_requests_task]
    :end-before: [END how_to_cloud_gen_ai_batch_api_create_batch_job_with_inlined_requests_task]

If you use file take a look at this example:

.. exampleinclude:: /../../google/tests/system/google/cloud/gen_ai/example_gen_ai_gemini_batch_api.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_gen_ai_batch_api_create_batch_job_with_file_task]
    :end-before: [END how_to_cloud_gen_ai_batch_api_create_batch_job_with_file_task]

Get batch job
"""""""""""""

To get batch job you can use
:class:`~airflow.providers.google.cloud.operators.gen_ai.GenAIGeminiGetBatchJobOperator`.
The operator returns the job name in :ref:`XCom <concepts:xcom>` under ``job_name`` key.

.. exampleinclude:: /../../google/tests/system/google/cloud/gen_ai/example_gen_ai_gemini_batch_api.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_gen_ai_batch_api_get_batch_job_task]
    :end-before: [END how_to_cloud_gen_ai_batch_api_get_batch_job_task]

List batch jobs
"""""""""""""""

To list batch jobs via Batch API you can use
:class:`~airflow.providers.google.cloud.operators.gen_ai.GenAIGeminiListBatchJobsOperator`.
The operator returns the job names in :ref:`XCom <concepts:xcom>` under ``job_names`` key.

.. exampleinclude:: /../../google/tests/system/google/cloud/gen_ai/example_gen_ai_gemini_batch_api.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_gen_ai_batch_api_list_batch_jobs_task]
    :end-before: [END how_to_cloud_gen_ai_batch_api_list_batch_jobs_task]

Cancel batch job
""""""""""""""""

To cancel batch job via Batch API you can use
:class:`~airflow.providers.google.cloud.operators.gen_ai.GenAIGeminiCancelBatchJobOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/gen_ai/example_gen_ai_gemini_batch_api.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_gen_ai_batch_api_cancel_batch_job_task]
    :end-before: [END how_to_cloud_gen_ai_batch_api_cancel_batch_job_task]

Delete batch job
""""""""""""""""

To queue batch job for deletion via Batch API you can use
:class:`~airflow.providers.google.cloud.operators.gen_ai.GenAIGeminiDeleteBatchJobOperator`.
The job will not be deleted immediately. After submitting it for deletion, it will still be available
through GenAIGeminiListBatchJobsOperator or GenAIGeminiGetBatchJobOperator for some time. It is behavior of the API
not the operator.

.. exampleinclude:: /../../google/tests/system/google/cloud/gen_ai/example_gen_ai_gemini_batch_api.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_gen_ai_batch_api_delete_batch_job_task]
    :end-before: [END how_to_cloud_gen_ai_batch_api_delete_batch_job_task]

Create embeddings
"""""""""""""""""

To create embeddings batch job via Batch API you can use
:class:`~airflow.providers.google.cloud.operators.gen_ai.GenAIGeminiCreateEmbeddingsBatchJobOperator`.
The operator returns the job name in :ref:`XCom <concepts:xcom>` under ``job_name`` key.

Two option of input source is allowed: inline requests, file.

If you use inline requests take a look at this example:

.. exampleinclude:: /../../google/tests/system/google/cloud/gen_ai/example_gen_ai_gemini_batch_api.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_gen_ai_batch_api_create_embeddings_with_inlined_requests_task]
    :end-before: [END how_to_cloud_gen_ai_batch_api_create_embeddings_with_inlined_requests_task]

If you use file take a look at this example:

.. exampleinclude:: /../../google/tests/system/google/cloud/gen_ai/example_gen_ai_gemini_batch_api.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_gen_ai_batch_api_create_embeddings_with_file_task]
    :end-before: [END how_to_cloud_gen_ai_batch_api_create_embeddings_with_file_task]

Interacting with Gemini Files API
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The `Gemini Files API <https://ai.google.dev/gemini-api/docs/files>`__ helps Gemini to handle various types of
input data, including text, images, and audio, at the same time. Please note that Gemini Batch API mostly works with
files that were uploaded via Gemini Files API. The Files API lets you store up to 20GB of files per project, with each
file not exceeding 2GB in size. Files are stored for 48 hours.

Upload file
"""""""""""

To upload file via Files API you can use
:class:`~airflow.providers.google.cloud.operators.gen_ai.GenAIGeminiUploadFileOperator`.
The operator returns the file name in :ref:`XCom <concepts:xcom>` under ``file_name`` key.

.. exampleinclude:: /../../google/tests/system/google/cloud/gen_ai/example_gen_ai_gemini_batch_api.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_gen_ai_files_api_upload_file_task]
    :end-before: [END how_to_cloud_gen_ai_files_api_upload_file_task]

Get file
""""""""

To get file via Files API you can use
:class:`~airflow.providers.google.cloud.operators.gen_ai.GenAIGeminiGetFileOperator`.
The operator returns the file_name in :ref:`XCom <concepts:xcom>` under ``file_name`` key.

.. exampleinclude:: /../../google/tests/system/google/cloud/gen_ai/example_gen_ai_gemini_batch_api.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_gen_ai_files_api_get_file_task]
    :end-before: [END how_to_cloud_gen_ai_files_api_get_file_task]

List files
""""""""""

To list files via Files API you can use
:class:`~airflow.providers.google.cloud.operators.gen_ai.GenAIGeminiListFilesOperator`.
The operator returns file names in :ref:`XCom <concepts:xcom>` under ``file_names`` key.

.. exampleinclude:: /../../google/tests/system/google/cloud/gen_ai/example_gen_ai_gemini_batch_api.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_gen_ai_files_api_list_files_task]
    :end-before: [END how_to_cloud_gen_ai_files_api_list_files_task]

Delete file
"""""""""""

To delete file via Files API you can use
:class:`~airflow.providers.google.cloud.operators.gen_ai.GenAIGeminiDeleteFileOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/gen_ai/example_gen_ai_gemini_batch_api.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_gen_ai_files_api_delete_file_task]
    :end-before: [END how_to_cloud_gen_ai_files_api_delete_file_task]

Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://cloud.google.com/vertex-ai/generative-ai/docs/sdks/overview>`__
* `Gemini Batch API Documentation <https://ai.google.dev/gemini-api/docs/batch-api>`__
* `Gemini Files API Documentation <https://ai.google.dev/gemini-api/docs/files>`__

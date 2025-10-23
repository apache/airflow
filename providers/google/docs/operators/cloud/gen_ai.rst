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

Google Cloud Generative AI on Vertex AI Operators
=================================================

The `Google Cloud VertexAI <https://cloud.google.com/vertex-ai/generative-ai/docs/>`__
extends Vertex AI with powerful foundation models capable of generating text, images, and other modalities.
It provides access to Google Gemini family of multimodal models and other pre-trained generative models through
a unified API, SDK, and console. Developers can prompt, tune, and ground these models using their own data to build
applications such as chat bots, content creation tools, code assistants, and summarization systems.
With Vertex AI, you can securely integrate generative capabilities into enterprise workflows, monitor usage,
evaluate model quality, and deploy models at scale — all within the same managed ML platform.


Interacting with Generative AI
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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


Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://cloud.google.com/vertex-ai/generative-ai/docs/sdks/overview>`__

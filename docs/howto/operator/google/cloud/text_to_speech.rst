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


Google Cloud Text to Speech Operators
=====================================

Prerequisite Tasks
------------------

.. include:: /howto/operator/google/_partials/prerequisite_tasks.rst

.. _howto/operator:CloudTextToSpeechSynthesizeOperator:

CloudTextToSpeechSynthesizeOperator
-----------------------------------

Synthesizes text to audio file and stores it to Google Cloud Storage

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.text_to_speech.CloudTextToSpeechSynthesizeOperator`

Arguments
"""""""""

The ``input``, ``voice`` and ``audio_config`` arguments need to be dicts or objects of corresponding classes from
``google.cloud.texttospeech_v1.types`` module

for more information, see: https://googleapis.github.io/google-cloud-python/latest/texttospeech/gapic/v1/api.html#google.cloud.texttospeech_v1.TextToSpeechClient.synthesize_speech

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_text_to_speech.py
      :language: python
      :start-after: [START howto_operator_text_to_speech_api_arguments]
      :end-before: [END howto_operator_text_to_speech_api_arguments]

The ``filename`` argument is a simple string argument:

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_text_to_speech.py
      :language: python
      :start-after: [START howto_operator_text_to_speech_gcp_filename]
      :end-before: [END howto_operator_text_to_speech_gcp_filename]

Using the operator
""""""""""""""""""

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_text_to_speech.py
      :language: python
      :dedent: 4
      :start-after: [START howto_operator_text_to_speech_synthesize]
      :end-before: [END howto_operator_text_to_speech_synthesize]

Templating
""""""""""

.. literalinclude:: ../../../../../airflow/providers/google/cloud/operators/text_to_speech.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_text_to_speech_synthesize_template_fields]
    :end-before: [END gcp_text_to_speech_synthesize_template_fields]

Reference
---------

For further information, look at:

* `Client Library Documentation <https://googleapis.github.io/google-cloud-python/latest/speech/>`__
* `Product Documentation <https://cloud.google.com/speech/>`__

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


Google Cloud Speech to Text Operators
=====================================

Prerequisite Tasks
------------------

.. include::/operators/_partials/prerequisite_tasks.rst

.. _howto/operator:CloudSpeechToTextRecognizeSpeechOperator:

CloudSpeechToTextRecognizeSpeechOperator
----------------------------------------

Recognizes speech in audio input and returns text.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.speech_to_text.CloudSpeechToTextRecognizeSpeechOperator`

Arguments
"""""""""

config and audio arguments need to be dicts or objects of corresponding classes from
google.cloud.speech_v1.types module

for more information, see: https://googleapis.github.io/google-cloud-python/latest/speech/gapic/v1/api.html#google.cloud.speech_v1.SpeechClient.recognize

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_speech_to_text.py
      :language: python
      :start-after: [START howto_operator_text_to_speech_api_arguments]
      :end-before: [END howto_operator_text_to_speech_api_arguments]

filename is a simple string argument:

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_speech_to_text.py
      :language: python
      :start-after: [START howto_operator_speech_to_text_api_arguments]
      :end-before: [END howto_operator_speech_to_text_api_arguments]

Using the operator
""""""""""""""""""

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_speech_to_text.py
      :language: python
      :dedent: 4
      :start-after: [START howto_operator_speech_to_text_recognize]
      :end-before: [END howto_operator_speech_to_text_recognize]

Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/speech_to_text.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_speech_to_text_synthesize_template_fields]
    :end-before: [END gcp_speech_to_text_synthesize_template_fields]

Reference
---------

For further information, look at:

* `Client Library Documentation <https://googleapis.github.io/google-cloud-python/latest/speech/>`__
* `Product Documentation <https://cloud.google.com/speech/>`__

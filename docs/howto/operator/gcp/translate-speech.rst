..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..  http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Google Cloud Speech Translate Operators
=======================================

.. contents::
  :depth: 1
  :local:

.. _howto/operator:GcpTranslateSpeechOperator:

GcpTranslateSpeechOperator
--------------------------

Recognizes speech in audio input and translates it.

For parameter definition, take a look at
:class:`airflow.gcp.operators.translate_speech.GcpTranslateSpeechOperator`

Arguments
"""""""""

Config and audio arguments need to be dicts or objects of corresponding classes from
``google.cloud.speech_v1.types`` module.

for more information, see: https://googleapis.github.io/google-cloud-python/latest/speech/gapic/v1/api.html#google.cloud.speech_v1.SpeechClient.recognize

.. exampleinclude:: ../../../../airflow/gcp/example_dags/example_speech.py
      :language: python
      :start-after: [START howto_operator_speech_to_text_api_arguments]
      :end-before: [END howto_operator_speech_to_text_api_arguments]

Arguments for translation need to be specified.

.. exampleinclude:: ../../../../airflow/gcp/example_dags/example_speech.py
      :language: python
      :start-after: [START howto_operator_translate_speech_arguments]
      :end-before: [END howto_operator_translate_speech_arguments]


Using the operator
""""""""""""""""""

.. exampleinclude:: ../../../../airflow/gcp/example_dags/example_speech.py
      :language: python
      :dedent: 4
      :start-after: [START howto_operator_translate_speech]
      :end-before: [END howto_operator_translate_speech]

Templating
""""""""""

.. literalinclude:: ../../../../airflow/gcp/operators/translate_speech.py
    :language: python
    :dedent: 4
    :start-after: [START translate_speech_template_fields]
    :end-before: [END translate_speech_template_fields]

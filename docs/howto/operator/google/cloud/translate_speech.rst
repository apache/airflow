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

Google Cloud Speech Translate Operators
=======================================

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
------------------

.. include:: /howto/operator/google/_partials/prerequisite_tasks.rst

.. _howto/operator:CloudTranslateSpeechOperator:

CloudTranslateSpeechOperator
----------------------------

Recognizes speech in audio input and translates it.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.translate_speech.CloudTranslateSpeechOperator`

Arguments
"""""""""

Config and audio arguments need to be dicts or objects of corresponding classes from
``google.cloud.speech_v1.types`` module.

for more information, see: https://googleapis.github.io/google-cloud-python/latest/speech/gapic/v1/api.html#google.cloud.speech_v1.SpeechClient.recognize

Arguments for translation need to be specified.

.. exampleinclude:: /../airflow/providers/google/cloud/example_dags/example_translate_speech.py
      :language: python
      :start-after: [START howto_operator_translate_speech_arguments]
      :end-before: [END howto_operator_translate_speech_arguments]


Using the operator
""""""""""""""""""

.. exampleinclude:: /../airflow/providers/google/cloud/example_dags/example_translate_speech.py
      :language: python
      :dedent: 4
      :start-after: [START howto_operator_translate_speech]
      :end-before: [END howto_operator_translate_speech]

Templating
""""""""""

.. literalinclude:: /../airflow/providers/google/cloud/operators/translate_speech.py
    :language: python
    :dedent: 4
    :start-after: [START translate_speech_template_fields]
    :end-before: [END translate_speech_template_fields]

Reference
---------

For further information, look at:

* `Google Cloud Translate - Client Library Documentation <https://googleapis.github.io/google-cloud-python/latest/translate/index.html>`__
* `Google Cloud Translate - Product Documentation <https://cloud.google.com/translate/docs/>`__
* `Google Cloud Speech - Client Library Documentation <https://googleapis.github.io/google-cloud-python/latest/speech/>`__
* `Google Cloud Speech - Product Documentation <https://cloud.google.com/speech/>`__

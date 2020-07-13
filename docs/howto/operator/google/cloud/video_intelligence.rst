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

Google Cloud Video Intelligence Operators
=========================================

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
------------------

.. include:: /howto/operator/google/_partials/prerequisite_tasks.rst

.. _howto/operator:CloudVideoIntelligenceDetectVideoLabelsOperator:

CloudVideoIntelligenceDetectVideoLabelsOperator
-----------------------------------------------

Performs video annotation, annotating video labels.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.video_intelligence.CloudVideoIntelligenceDetectVideoLabelsOperator`

Using the operator
""""""""""""""""""

Input uri is an uri to a file in Google Cloud Storage

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_video_intelligence.py
      :language: python
      :start-after: [START howto_operator_video_intelligence_other_args]
      :end-before: [END howto_operator_video_intelligence_other_args]

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_video_intelligence.py
      :language: python
      :dedent: 4
      :start-after: [START howto_operator_video_intelligence_detect_labels]
      :end-before: [END howto_operator_video_intelligence_detect_labels]

You can use the annotation output via Xcom:

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_video_intelligence.py
      :language: python
      :dedent: 4
      :start-after: [START howto_operator_video_intelligence_detect_labels_result]
      :end-before: [END howto_operator_video_intelligence_detect_labels_result]

Templating
""""""""""

.. literalinclude:: ../../../../../airflow/providers/google/cloud/operators/video_intelligence.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_video_intelligence_detect_labels_template_fields]
    :end-before: [END gcp_video_intelligence_detect_labels_template_fields]

.. _howto/operator:CloudVideoIntelligenceDetectVideoExplicitContentOperator:

More information
""""""""""""""""

Note: The duration of video annotation operation is equal or longer than the annotation video itself.


CloudVideoIntelligenceDetectVideoExplicitContentOperator
--------------------------------------------------------

Performs video annotation, annotating explicit content.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.video_intelligence.CloudVideoIntelligenceDetectVideoExplicitContentOperator`

Arguments
"""""""""

Input uri is an uri to a file in Google Cloud Storage

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_video_intelligence.py
      :language: python
      :start-after: [START howto_operator_video_intelligence_other_args]
      :end-before: [END howto_operator_video_intelligence_other_args]

Using the operator
""""""""""""""""""

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_video_intelligence.py
      :language: python
      :dedent: 4
      :start-after: [START howto_operator_video_intelligence_detect_explicit_content]
      :end-before: [END howto_operator_video_intelligence_detect_explicit_content]

You can use the annotation output via Xcom:

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_video_intelligence.py
      :language: python
      :dedent: 4
      :start-after: [START howto_operator_video_intelligence_detect_explicit_content_result]
      :end-before: [END howto_operator_video_intelligence_detect_explicit_content_result]

Templating
""""""""""

.. literalinclude:: ../../../../../airflow/providers/google/cloud/operators/video_intelligence.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_video_intelligence_detect_explicit_content_template_fields]
    :end-before: [END gcp_video_intelligence_detect_explicit_content_template_fields]

.. _howto/operator:CloudVideoIntelligenceDetectVideoShotsOperator:

More information
""""""""""""""""

Note: The duration of video annotation operation is equal or longer than the annotation video itself.


CloudVideoIntelligenceDetectVideoShotsOperator
----------------------------------------------

Performs video annotation, annotating explicit content.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.video_intelligence.CloudVideoIntelligenceDetectVideoShotsOperator`

Arguments
"""""""""

Input uri is an uri to a file in Google Cloud Storage

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_video_intelligence.py
      :language: python
      :start-after: [START howto_operator_video_intelligence_other_args]
      :end-before: [END howto_operator_video_intelligence_other_args]

Using the operator
""""""""""""""""""

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_video_intelligence.py
      :language: python
      :dedent: 4
      :start-after: [START howto_operator_video_intelligence_detect_video_shots]
      :end-before: [END howto_operator_video_intelligence_detect_video_shots]

You can use the annotation output via Xcom:

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_video_intelligence.py
      :language: python
      :dedent: 4
      :start-after: [START howto_operator_video_intelligence_detect_video_shots_result]
      :end-before: [END howto_operator_video_intelligence_detect_video_shots_result]

Templating
""""""""""

.. literalinclude:: ../../../../../airflow/providers/google/cloud/operators/video_intelligence.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_video_intelligence_detect_video_shots_template_fields]
    :end-before: [END gcp_video_intelligence_detect_video_shots_template_fields]

More information
""""""""""""""""

Note: The duration of video annotation operation is equal or longer than the annotation video itself.

Reference
---------

For further information, look at:

* `Client Library Documentation <https://googleapis.github.io/google-cloud-python/latest/videointelligence/index.html>`__
* `Product Documentation <https://cloud.google.com/video-intelligence/docs/>`__

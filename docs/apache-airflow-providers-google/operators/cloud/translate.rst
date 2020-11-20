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



Google Cloud Translate Operators
--------------------------------

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:CloudTranslateTextOperator:

CloudTranslateTextOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^

Translate a string or list of strings.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.translate.CloudTranslateTextOperator`

Using the operator
""""""""""""""""""

Basic usage of the operator:

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_translate.py
      :language: python
      :dedent: 4
      :start-after: [START howto_operator_translate_text]
      :end-before: [END howto_operator_translate_text]

The result of translation is available as dictionary or array of dictionaries accessible via the usual
XCom mechanisms of Airflow:

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_translate.py
      :language: python
      :dedent: 4
      :start-after: [START howto_operator_translate_access]
      :end-before: [END howto_operator_translate_access]


Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/translate.py
    :language: python
    :dedent: 4
    :start-after: [START translate_template_fields]
    :end-before: [END translate_template_fields]

More information
""""""""""""""""

See `Google Cloud Translate documentation <https://cloud.google.com/translate/docs/translating-text>`_.

Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://googleapis.github.io/google-cloud-python/latest/translate/index.html>`__
* `Product Documentation <https://cloud.google.com/translate/docs/>`__

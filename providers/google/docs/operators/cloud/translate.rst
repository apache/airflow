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
=======================================

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

.. exampleinclude:: /../../google/tests/system/google/cloud/translate/example_translate.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_translate_text]
    :end-before: [END howto_operator_translate_text]

The result of translation is available as dictionary or array of dictionaries accessible via the usual
XCom mechanisms of Airflow:

.. exampleinclude:: /../../google/tests/system/google/cloud/translate/example_translate.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_translate_access]
    :end-before: [END howto_operator_translate_access]


Templating
""""""""""

.. literalinclude:: /../../google/src/airflow/providers/google/cloud/operators/translate.py
    :language: python
    :dedent: 4
    :start-after: [START translate_template_fields]
    :end-before: [END translate_template_fields]

.. _howto/operator:TranslateTextOperator:

TranslateTextOperator
^^^^^^^^^^^^^^^^^^^^^

Translate an array of one or more text (or html) items.
Intended to use for moderate amount of text data, for large volumes please use the
:class:`~airflow.providers.google.cloud.operators.translate.TranslateTextBatchOperator`

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.translate.TranslateTextOperator`

Using the operator
""""""""""""""""""

Basic usage of the operator:

.. exampleinclude:: /../../google/tests/system/google/cloud/translate/example_translate_text.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_translate_text_advanced]
    :end-before: [END howto_operator_translate_text_advanced]


.. _howto/operator:TranslateTextBatchOperator:

TranslateTextBatchOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^
Translate large amount of text data into up to 10 target languages in a single run.
List of files and other options provided by input configuration.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.translate.TranslateTextBatchOperator`


.. _howto/operator:TranslateCreateDatasetOperator:

TranslateCreateDatasetOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Create a native translation dataset using Cloud Translate API (Advanced V3).

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.translate.TranslateCreateDatasetOperator`

Using the operator
""""""""""""""""""

Basic usage of the operator:

.. exampleinclude:: /../../google/tests/system/google/cloud/translate/example_translate_dataset.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_translate_automl_create_dataset]
    :end-before: [END howto_operator_translate_automl_create_dataset]


.. _howto/operator:TranslateImportDataOperator:

TranslateImportDataOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^
Import data to the existing native dataset, using Cloud Translate API (Advanced V3).

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.translate.TranslateImportDataOperator`

Using the operator
""""""""""""""""""

Basic usage of the operator:

.. exampleinclude:: /../../google/tests/system/google/cloud/translate/example_translate_dataset.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_translate_automl_import_data]
    :end-before: [END howto_operator_translate_automl_import_data]


.. _howto/operator:TranslateDatasetsListOperator:

TranslateDatasetsListOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Get list of translation datasets using Cloud Translate API (Advanced V3).

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.translate.TranslateDatasetsListOperator`

Using the operator
""""""""""""""""""

Basic usage of the operator:

.. exampleinclude:: /../../google/tests/system/google/cloud/translate/example_translate_dataset.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_translate_automl_list_datasets]
    :end-before: [END howto_operator_translate_automl_list_datasets]


.. _howto/operator:TranslateDeleteDatasetOperator:

TranslateDeleteDatasetOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Delete a native translation dataset using Cloud Translate API (Advanced V3).

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.translate.TranslateDeleteDatasetOperator`

Using the operator
""""""""""""""""""

Basic usage of the operator:

.. exampleinclude:: /../../google/tests/system/google/cloud/translate/example_translate_dataset.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_translate_automl_delete_dataset]
    :end-before: [END howto_operator_translate_automl_delete_dataset]


.. _howto/operator:TranslateCreateModelOperator:

TranslateCreateModelOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Create a native translation model using Cloud Translate API (Advanced V3).

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.translate.TranslateCreateModelOperator`

Using the operator
""""""""""""""""""

Basic usage of the operator:

.. exampleinclude:: /../../google/tests/system/google/cloud/translate/example_translate_model.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_translate_automl_create_model]
    :end-before: [END howto_operator_translate_automl_create_model]


.. _howto/operator:TranslateModelsListOperator:

TranslateModelsListOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^
Get list of native translation models using Cloud Translate API (Advanced V3).

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.translate.TranslateModelsListOperator`

Using the operator
""""""""""""""""""

Basic usage of the operator:

.. exampleinclude:: /../../google/tests/system/google/cloud/translate/example_translate_model.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_translate_automl_list_models]
    :end-before: [END howto_operator_translate_automl_list_models]


.. _howto/operator:TranslateDeleteModelOperator:

TranslateDeleteModelOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Delete a native translation model using Cloud Translate API (Advanced V3).

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.translate.TranslateDeleteModelOperator`

Using the operator
""""""""""""""""""

Basic usage of the operator:

.. exampleinclude:: /../../google/tests/system/google/cloud/translate/example_translate_model.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_translate_automl_delete_model]
    :end-before: [END howto_operator_translate_automl_delete_model]


.. _howto/operator:TranslateDocumentOperator:

TranslateDocumentOperator
^^^^^^^^^^^^^^^^^^^^^^^^^
Translate Document using Cloud Translate API (Advanced V3).

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.translate.TranslateDocumentOperator`

Using the operator
""""""""""""""""""

Basic usage of the operator:

.. exampleinclude:: /../../google/tests/system/google/cloud/translate/example_translate_document.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_translate_document]
    :end-before: [END howto_operator_translate_document]


.. _howto/operator:TranslateDocumentBatchOperator:

TranslateDocumentBatchOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Translate Documents using Cloud Translate API (Advanced V3), by given input configs.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.translate.TranslateDocumentBatchOperator`

Using the operator
""""""""""""""""""

Basic usage of the operator:

.. exampleinclude:: /../../google/tests/system/google/cloud/translate/example_translate_document.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_translate_document_batch]
    :end-before: [END howto_operator_translate_document_batch]


.. _howto/operator:TranslateCreateGlossaryOperator:

TranslateCreateGlossaryOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Create a translation glossary, using Cloud Translate API (Advanced V3).

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.translate.TranslateCreateGlossaryOperator`

Using the operator
""""""""""""""""""

Basic usage of the operator:

.. exampleinclude:: /../../google/tests/system/google/cloud/translate/example_translate_glossary.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_translate_create_glossary]
    :end-before: [END howto_operator_translate_create_glossary]


.. _howto/operator:TranslateUpdateGlossaryOperator:

TranslateUpdateGlossaryOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Updates translation glossary, using Cloud Translate API (Advanced V3).
Only ``display_name`` and ``input_config`` fields available for update.
By updating input_config - the glossary dictionary updates.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.translate.TranslateUpdateGlossaryOperator`

Using the operator
""""""""""""""""""

Basic usage of the operator:

.. exampleinclude:: /../../google/tests/system/google/cloud/translate/example_translate_glossary.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_translate_update_glossary]
    :end-before: [END howto_operator_translate_update_glossary]


.. _howto/operator:TranslateListGlossariesOperator:

TranslateListGlossariesOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
List all available translation glossaries on the project.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.translate.TranslateListGlossariesOperator`

Using the operator
""""""""""""""""""

Basic usage of the operator:

.. exampleinclude:: /../../google/tests/system/google/cloud/translate/example_translate_glossary.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_translate_list_glossaries]
    :end-before: [END howto_operator_translate_list_glossaries]


.. _howto/operator:TranslateDeleteGlossaryOperator:

TranslateDeleteGlossaryOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Delete the translation glossary resource.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.translate.TranslateDeleteGlossaryOperator`

Using the operator
""""""""""""""""""

Basic usage of the operator:

.. exampleinclude:: /../../google/tests/system/google/cloud/translate/example_translate_glossary.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_translate_delete_glossary]
    :end-before: [END howto_operator_translate_delete_glossary]


More information
""""""""""""""""""
See:
Base (V2) `Google Cloud Translate documentation <https://cloud.google.com/translate/docs/translating-text>`_.
Advanced (V3) `Google Cloud Translate (Advanced) documentation <https://cloud.google.com/translate/docs/advanced/translating-text-v3>`_.
Datasets `Legacy and native dataset comparison <https://cloud.google.com/translate/docs/advanced/automl-upgrade>`_.


Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://googleapis.github.io/google-cloud-python/latest/translate/index.html>`__
* `Product Documentation <https://cloud.google.com/translate/docs/>`__

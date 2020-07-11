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



Google Cloud Natural Language Operators
=======================================

The `Google Cloud Natural Language <https://cloud.google.com/natural-language/>`__
can be used to reveal the structure and meaning of text via powerful machine
learning models. You can use it to extract information about people, places,
events and much more, mentioned in text documents, news articles or blog posts.
You can use it to understand sentiment about your product on social media or
parse intent from customer conversations happening in a call center or a
messaging app.

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /howto/operator/google/_partials/prerequisite_tasks.rst


.. _howto/operator:CloudNaturalLanguageDocuments:

Documents
^^^^^^^^^

Each operator uses a :class:`~google.cloud.language_v1.types.Document` for
representing text.

Here is an example of document with text provided as a string:

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_natural_language.py
    :language: python
    :start-after: [START howto_operator_gcp_natural_language_document_text]
    :end-before: [END howto_operator_gcp_natural_language_document_text]

In addition to supplying string, a document can refer to content stored in Google Cloud Storage.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_natural_language.py
    :language: python
    :start-after: [START howto_operator_gcp_natural_language_document_gcs]
    :end-before: [END howto_operator_gcp_natural_language_document_gcs]

.. _howto/operator:CloudNaturalLanguageAnalyzeEntitiesOperator:

Analyzing Entities
^^^^^^^^^^^^^^^^^^

Entity Analysis inspects the given text for known entities (proper nouns such as
public figures, landmarks, etc.), and returns information about those entities.
Entity analysis is performed with the
:class:`~airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageAnalyzeEntitiesOperator` operator.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_natural_language.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_natural_language_analyze_entities]
    :end-before: [END howto_operator_gcp_natural_language_analyze_entities]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageAnalyzeEntitiesOperator`
parameters which allows you to dynamically determine values. The result is saved to :ref:`XCom <concepts:xcom>`, which allows it
to be used by other operators.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_natural_language.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_natural_language_analyze_entities_result]
    :end-before: [END howto_operator_gcp_natural_language_analyze_entities_result]

.. _howto/operator:CloudNaturalLanguageAnalyzeEntitySentimentOperator:

Analyzing Entity Sentiment
^^^^^^^^^^^^^^^^^^^^^^^^^^

Sentiment Analysis inspects the given text and identifies the prevailing
emotional opinion within the text, especially to determine a writer's attitude
as positive, negative, or neutral. Sentiment analysis is performed through
the :class:`~airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageAnalyzeEntitySentimentOperator`
operator.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_natural_language.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_natural_language_analyze_entity_sentiment]
    :end-before: [END howto_operator_gcp_natural_language_analyze_entity_sentiment]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageAnalyzeEntitiesOperator`
parameters which allows you to dynamically determine values. The result is saved to :ref:`XCom <concepts:xcom>`, which allows it
to be used by other operators.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_natural_language.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_natural_language_analyze_entity_sentiment_result]
    :end-before: [END howto_operator_gcp_natural_language_analyze_entity_sentiment_result]

.. _howto/operator:CloudNaturalLanguageAnalyzeSentimentOperator:

Analyzing Sentiment
^^^^^^^^^^^^^^^^^^^

Sentiment Analysis inspects the given text and identifies the prevailing
emotional opinion within the text, especially to determine a writer's
attitude as positive, negative, or neutral. Sentiment analysis is performed
through the
:class:`~airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageAnalyzeSentimentOperator`
operator.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_natural_language.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_natural_language_analyze_sentiment]
    :end-before: [END howto_operator_gcp_natural_language_analyze_sentiment]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageAnalyzeSentimentOperator`
parameters which allows you to dynamically determine values. The result is saved to :ref:`XCom <concepts:xcom>`, which allows it
to be used by other operators.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_natural_language.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_natural_language_analyze_sentiment_result]
    :end-before: [END howto_operator_gcp_natural_language_analyze_sentiment_result]

.. _howto/operator:CloudNaturalLanguageClassifyTextOperator:

Classifying Content
^^^^^^^^^^^^^^^^^^^

Content Classification analyzes a document and returns a list of content
categories that apply to the text found in the document. To classify the
content in a document, use the
:class:`~airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageClassifyTextOperator`
operator.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_natural_language.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_natural_language_analyze_classify_text]
    :end-before: [END howto_operator_gcp_natural_language_analyze_classify_text]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageClassifyTextOperator`
parameters which allows you to dynamically determine values. The result is saved to :ref:`XCom <concepts:xcom>`, which allows it
to be used by other operators.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_natural_language.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_natural_language_analyze_classify_text_result]
    :end-before: [END howto_operator_gcp_natural_language_analyze_classify_text_result]


Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://googleapis.github.io/google-cloud-python/latest/language/index.html>`__
* `Product Documentation <https://cloud.google.com/natural-language/docs/>`__

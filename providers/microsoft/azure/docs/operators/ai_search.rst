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



.. _howto/hook:AzureAISearchHook:

Azure AI Search
===============

`Azure AI Search <https://learn.microsoft.com/en-us/azure/search/>`__ is the full-text and vector
store behind retrieval-augmented generation on Azure. The
:class:`~airflow.providers.microsoft.azure.hooks.ai_search.AzureAISearchHook` reads and writes the
documents of one index, from an async task: every method is a coroutine, and
`async Python callables <https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/operators/python.html>`__
are supported since Airflow 3.2. Creating and changing indexes is left to the
``azure-search-documents`` SDK.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

The hook uses the ``azure_ai_search_default`` connection by default, see
:ref:`howto/connection:azure_ai_search`. The index comes from the hook (``index=``) or from the
``index`` extra of the connection.

Writing documents
-----------------

``upload`` inserts documents and replaces the ones whose key exists. The documents are sent in batches
of ``batch_size`` (1000 by default, the limit of the service), and the hook raises
:class:`~airflow.providers.microsoft.azure.exceptions.AzureAISearchIndexingError` with the keys and
reasons when the service rejects some of them.

.. exampleinclude:: /../tests/system/microsoft/azure/example_azure_ai_search.py
    :language: python
    :dedent: 0
    :start-after: [START howto_hook_azure_ai_search_upload]
    :end-before: [END howto_hook_azure_ai_search_upload]

Searching documents
-------------------

``search`` is an async generator over every matching document, following the pages of the service.
Select the fields you need, and filter and sort with
`OData expressions <https://learn.microsoft.com/en-us/azure/search/search-query-odata-filter>`__.

.. exampleinclude:: /../tests/system/microsoft/azure/example_azure_ai_search.py
    :language: python
    :dedent: 0
    :start-after: [START howto_hook_azure_ai_search_search]
    :end-before: [END howto_hook_azure_ai_search_search]

Updating and counting documents
-------------------------------

``merge`` changes the given fields of existing documents and ``merge_or_upload`` also inserts the
documents whose key is new. ``count`` returns the number of documents of the index, or of those
matching a filter.

.. exampleinclude:: /../tests/system/microsoft/azure/example_azure_ai_search.py
    :language: python
    :dedent: 0
    :start-after: [START howto_hook_azure_ai_search_merge]
    :end-before: [END howto_hook_azure_ai_search_merge]

Reading one document
--------------------

``get_document`` returns the document with a key, or ``None`` when the index has none.

.. exampleinclude:: /../tests/system/microsoft/azure/example_azure_ai_search.py
    :language: python
    :dedent: 0
    :start-after: [START howto_hook_azure_ai_search_get_document]
    :end-before: [END howto_hook_azure_ai_search_get_document]

Deleting documents
------------------

``delete`` removes documents by key.

.. exampleinclude:: /../tests/system/microsoft/azure/example_azure_ai_search.py
    :language: python
    :dedent: 0
    :start-after: [START howto_hook_azure_ai_search_delete]
    :end-before: [END howto_hook_azure_ai_search_delete]

Reference
---------

For further information, look at:

* `Azure AI Search documentation <https://learn.microsoft.com/en-us/azure/search/>`__
* `azure-search-documents SDK <https://learn.microsoft.com/en-us/python/api/overview/azure/search-documents-readme>`__

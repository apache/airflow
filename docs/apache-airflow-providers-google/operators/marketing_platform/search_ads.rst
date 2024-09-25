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

Google Search Ads Operators
=======================================

Create, manage, and track high-impact campaigns across multiple search engines with one centralized tool.
For more information check `Google Search Ads <https://developers.google.com/search-ads/>`__.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:GoogleSearchAdsSearchOperator:

Querying a report
^^^^^^^^^^^^^^^^^^

To query a Search Ads report use the
:class:`~airflow.providers.google.marketing_platform.operators.search_ads.GoogleSearchAdsSearchOperator`.

.. exampleinclude:: /../../tests/system/providers/google/marketing_platform/example_search_ads.py
    :language: python
    :dedent: 4
    :start-after: [START howto_search_ads_search_query_reports]
    :end-before: [END howto_search_ads_search_query_reports]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.search_ads.GoogleSearchAdsSearchOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:GoogleSearchAdsGetFieldOperator:

Retrieve a field metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^

To retrieve metadata of a field use
:class:`~airflow.providers.google.marketing_platform.operators.search_ads.GoogleSearchAdsGetFieldOperator`.

.. exampleinclude:: /../../tests/system/providers/google/marketing_platform/example_search_ads.py
    :language: python
    :dedent: 4
    :start-after: [START howto_search_ads_get_field]
    :end-before: [END howto_search_ads_get_field]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.search_ads.GoogleSearchAdsGetFieldOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:GoogleSearchAdsSearchFieldsOperator:

Retrieve metadata for multiple fields
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To retrieve metadata of multiple fields use the
:class:`~airflow.providers.google.marketing_platform.operators.search_ads.GoogleSearchAdsSearchFieldsOperator`.

.. exampleinclude:: /../../tests/system/providers/google/marketing_platform/example_search_ads.py
    :language: python
    :dedent: 4
    :start-after: [START howto_search_ads_search_fields]
    :end-before: [END howto_search_ads_search_fields]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.search_ads.GoogleSearchAdsSearchFieldsOperator`
parameters which allows you to dynamically determine values.


.. _howto/operator:GoogleSearchAdsGetCustomColumnOperator:

Retrieve a custom column details
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To retrieve details of a custom column use
:class:`~airflow.providers.google.marketing_platform.operators.search_ads.GoogleSearchAdsGetCustomColumnOperator`.

.. exampleinclude:: /../../tests/system/providers/google/marketing_platform/example_search_ads.py
    :language: python
    :dedent: 4
    :start-after: [START howto_search_ads_get_custom_column]
    :end-before: [END howto_search_ads_get_custom_column]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.search_ads.GoogleSearchAdsGetCustomColumnOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:GoogleSearchAdsListCustomColumnsOperator:

Retrieve a custom column details
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To retrieve the list of all custom columns use

:class:`~airflow.providers.google.marketing_platform.operators.search_ads.GoogleSearchAdsListCustomColumnsOperator`.

.. exampleinclude:: /../../tests/system/providers/google/marketing_platform/example_search_ads.py
    :language: python
    :dedent: 4
    :start-after: [START howto_search_ads_list_custom_columns]
    :end-before: [END howto_search_ads_list_custom_columns]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.search_ads.GoogleSearchAdsListCustomColumnsOperator`
parameters which allows you to dynamically determine values.

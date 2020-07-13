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

Google Analytics 360 Operators
==============================

Google Analytics 360 operators allow you to lists all accounts to which the user has access.
For more information about the Google Analytics 360 API check
`official documentation <https://developers.google.com/analytics/devguides/config/mgmt/v3>`__.


.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /howto/operator/google/_partials/prerequisite_tasks.rst

.. _howto/operator:GoogleAnalyticsListAccountsOperator:

List the Accounts
^^^^^^^^^^^^^^^^^

To list accounts from Analytics you can use the
:class:`~airflow.providers.google.marketing_platform.operators.analytics.GoogleAnalyticsListAccountsOperator`.

.. exampleinclude:: ../../../../../airflow/providers/google/marketing_platform/example_dags/example_analytics.py
    :language: python
    :dedent: 4
    :start-after: [START howto_marketing_platform_list_accounts_operator]
    :end-before: [END howto_marketing_platform_list_accounts_operator]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.analytics.GoogleAnalyticsListAccountsOperator`

.. _howto/operator:GoogleAnalyticsGetAdsLinkOperator:

Get Ad Words Link
^^^^^^^^^^^^^^^^^

Returns a web property-Google Ads link to which the user has access.
To list web property-Google Ads link you can use the
:class:`~airflow.providers.google.marketing_platform.operators.analytics.GoogleAnalyticsGetAdsLinkOperator`.

.. exampleinclude:: ../../../../../airflow/providers/google/marketing_platform/example_dags/example_analytics.py
    :language: python
    :dedent: 4
    :start-after: [START howto_marketing_platform_get_ads_link_operator]
    :end-before: [END howto_marketing_platform_get_ads_link_operator]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.analytics.GoogleAnalyticsGetAdsLinkOperator`

.. _howto/operator:GoogleAnalyticsRetrieveAdsLinksListOperator:

List Google Ads Links
^^^^^^^^^^^^^^^^^^^^^

Operator returns a list of entity Google Ads links.
To list Google Ads links you can use the
:class:`~airflow.providers.google.marketing_platform.operators.analytics.GoogleAnalyticsRetrieveAdsLinksListOperator`.

.. exampleinclude:: ../../../../../airflow/providers/google/marketing_platform/example_dags/example_analytics.py
    :language: python
    :dedent: 4
    :start-after: [START howto_marketing_platform_retrieve_ads_links_list_operator]
    :end-before: [END howto_marketing_platform_retrieve_ads_links_list_operator]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.analytics.GoogleAnalyticsRetrieveAdsLinksListOperator`

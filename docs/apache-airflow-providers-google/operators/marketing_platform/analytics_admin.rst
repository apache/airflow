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

Google Analytics (GA4) Admin Operators
======================================

Google Analytics (GA4) Admin operators allow you to lists all accounts to which the user has access.
For more information about the Google Analytics 360 API check
`official documentation <https://developers.google.com/analytics/devguides/config/admin/v1>`__.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:GoogleAnalyticsAdminListAccountsOperator:

List the Accounts
^^^^^^^^^^^^^^^^^

To list accounts from Analytics you can use the
:class:`~airflow.providers.google.marketing_platform.operators.analytics_admin.GoogleAnalyticsAdminListAccountsOperator`.

.. exampleinclude:: /../../providers/tests/system/google/marketing_platform/example_analytics_admin.py
    :language: python
    :dedent: 4
    :start-after: [START howto_marketing_platform_list_accounts_operator]
    :end-before: [END howto_marketing_platform_list_accounts_operator]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.analytics_admin.GoogleAnalyticsAdminListAccountsOperator`

.. _howto/operator:GoogleAnalyticsAdminCreatePropertyOperator:

Create Property
^^^^^^^^^^^^^^^

Creates a property.
To create a property you can use the
:class:`~airflow.providers.google.marketing_platform.operators.analytics_admin.GoogleAnalyticsAdminCreatePropertyOperator`.

.. exampleinclude:: /../../providers/tests/system/google/marketing_platform/example_analytics_admin.py
    :language: python
    :dedent: 4
    :start-after: [START howto_marketing_platform_create_property_operator]
    :end-before: [END howto_marketing_platform_create_property_operator]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.analytics_admin.GoogleAnalyticsAdminCreatePropertyOperator`

.. _howto/operator:GoogleAnalyticsAdminDeletePropertyOperator:

Delete Property
^^^^^^^^^^^^^^^

Deletes a property.
To delete a property you can use the
:class:`~airflow.providers.google.marketing_platform.operators.analytics_admin.GoogleAnalyticsAdminDeletePropertyOperator`.

.. exampleinclude:: /../../providers/tests/system/google/marketing_platform/example_analytics_admin.py
    :language: python
    :dedent: 4
    :start-after: [START howto_marketing_platform_delete_property_operator]
    :end-before: [END howto_marketing_platform_delete_property_operator]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.analytics_admin.GoogleAnalyticsAdminDeletePropertyOperator`

.. _howto/operator:GoogleAnalyticsAdminCreateDataStreamOperator:

Create Data stream
^^^^^^^^^^^^^^^^^^

Creates a data stream.
To create a data stream you can use the
:class:`~airflow.providers.google.marketing_platform.operators.analytics_admin.GoogleAnalyticsAdminCreateDataStreamOperator`.

.. exampleinclude:: /../../providers/tests/system/google/marketing_platform/example_analytics_admin.py
    :language: python
    :dedent: 4
    :start-after: [START howto_marketing_platform_create_data_stream_operator]
    :end-before: [END howto_marketing_platform_create_data_stream_operator]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.analytics_admin.GoogleAnalyticsAdminCreateDataStreamOperator`

.. _howto/operator:GoogleAnalyticsAdminDeleteDataStreamOperator:

Delete Data stream
^^^^^^^^^^^^^^^^^^

Deletes a data stream.
To delete a data stream you can use the
:class:`~airflow.providers.google.marketing_platform.operators.analytics_admin.GoogleAnalyticsAdminDeleteDataStreamOperator`.

.. exampleinclude:: /../../providers/tests/system/google/marketing_platform/example_analytics_admin.py
    :language: python
    :dedent: 4
    :start-after: [START howto_marketing_platform_delete_data_stream_operator]
    :end-before: [END howto_marketing_platform_delete_data_stream_operator]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.analytics_admin.GoogleAnalyticsAdminDeleteDataStreamOperator`

.. _howto/operator:GoogleAnalyticsAdminListGoogleAdsLinksOperator:

List Google Ads Links
^^^^^^^^^^^^^^^^^^^^^

To list Google Ads links you can use the
:class:`~airflow.providers.google.marketing_platform.operators.analytics_admin.GoogleAnalyticsAdminListGoogleAdsLinksOperator`.

.. exampleinclude:: /../../providers/tests/system/google/marketing_platform/example_analytics_admin.py
    :language: python
    :dedent: 4
    :start-after: [START howto_marketing_platform_list_google_ads_links]
    :end-before: [END howto_marketing_platform_list_google_ads_links]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.analytics_admin.GoogleAnalyticsAdminListGoogleAdsLinksOperator`

.. _howto/operator:GoogleAnalyticsAdminGetGoogleAdsLinkOperator:

Get the Google Ads link
^^^^^^^^^^^^^^^^^^^^^^^

To list Google Ads links you can use the
:class:`~airflow.providers.google.marketing_platform.operators.analytics_admin.GoogleAnalyticsAdminGetGoogleAdsLinkOperator`.

.. exampleinclude:: /../../providers/tests/system/google/marketing_platform/example_analytics_admin.py
    :language: python
    :dedent: 4
    :start-after: [START howto_marketing_platform_get_google_ad_link]
    :end-before: [END howto_marketing_platform_get_google_ad_link]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.analytics_admin.GoogleAnalyticsAdminGetGoogleAdsLinkOperator`

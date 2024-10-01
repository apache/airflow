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

Please note that the Google Analytics 360 API is replaced by
`Google Analytics 4 <https://developers.google.com/analytics/devguides/config/admin/v1>`__ and is
`turned down on July 1, 2024 <https://support.google.com/analytics/answer/11583528>`__.
Thus consider using new :doc:`Google Analytics (GA4) Admin Operators </operators/marketing_platform/analytics_admin>`.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

List the Accounts
^^^^^^^^^^^^^^^^^

To list accounts from Analytics you can use the
:class:`~airflow.providers.google.marketing_platform.operators.analytics.GoogleAnalyticsListAccountsOperator`.

Get Ad Words Link
^^^^^^^^^^^^^^^^^

Returns a web property-Google Ads link to which the user has access.
To list web property-Google Ads link you can use the
:class:`~airflow.providers.google.marketing_platform.operators.analytics.GoogleAnalyticsGetAdsLinkOperator`.

List Google Ads Links
^^^^^^^^^^^^^^^^^^^^^

Operator returns a list of entity Google Ads links.
To list Google Ads links you can use the
:class:`~airflow.providers.google.marketing_platform.operators.analytics.GoogleAnalyticsRetrieveAdsLinksListOperator`.

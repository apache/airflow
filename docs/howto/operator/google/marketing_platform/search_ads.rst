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

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /howto/operator/google/_partials/prerequisite_tasks.rst

.. _howto/operator:GoogleSearchAdsInsertReportOperator:

Inserting a report
^^^^^^^^^^^^^^^^^^

To insert a Search Ads report use the
:class:`~airflow.providers.google.marketing_platform.operators.search_ads.GoogleSearchAdsInsertReportOperator`.

.. exampleinclude:: /../airflow/providers/google/marketing_platform/example_dags/example_search_ads.py
    :language: python
    :dedent: 4
    :start-after: [START howto_search_ads_generate_report_operator]
    :end-before: [END howto_search_ads_generate_report_operator]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.search_ads.GoogleSearchAdsInsertReportOperator`
parameters which allows you to dynamically determine values. You can provide report definition using ``
.json`` file as this operator supports this template extension.
The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators:

.. exampleinclude:: /../airflow/providers/google/marketing_platform/example_dags/example_search_ads.py
    :language: python
    :dedent: 4
    :start-after: [START howto_search_ads_get_report_id]
    :end-before: [END howto_search_ads_get_report_id]

.. _howto/operator:GoogleSearchAdsReportSensor:

Awaiting for a report
^^^^^^^^^^^^^^^^^^^^^

To wait for a report to be ready for download use
:class:`~airflow.providers.google.marketing_platform.sensors.search_ads.GoogleSearchAdsReportSensor`.

.. exampleinclude:: /../airflow/providers/google/marketing_platform/example_dags/example_search_ads.py
    :language: python
    :dedent: 4
    :start-after: [START howto_search_ads_get_report_operator]
    :end-before: [END howto_search_ads_get_report_operator]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.sensors.search_ads.GoogleSearchAdsReportSensor`
parameters which allows you to dynamically determine values.

.. _howto/operator:GoogleSearchAdsGetfileReportOperator:

Downloading a report
^^^^^^^^^^^^^^^^^^^^

To download a Search Ads report to Google Cloud Storage bucket use the
:class:`~airflow.providers.google.marketing_platform.operators.search_ads.GoogleSearchAdsDownloadReportOperator`.

.. exampleinclude:: /../airflow/providers/google/marketing_platform/example_dags/example_search_ads.py
    :language: python
    :dedent: 4
    :start-after: [START howto_search_ads_getfile_report_operator]
    :end-before: [END howto_search_ads_getfile_report_operator]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.search_ads.GoogleSearchAdsDownloadReportOperator`
parameters which allows you to dynamically determine values.
The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

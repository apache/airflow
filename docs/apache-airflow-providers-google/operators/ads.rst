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

Google Ads Operators
=======================================
`Google Ads <https://ads.google.com/home/>`__, formerly Google AdWords and Google AdWords Express, is a platform which allows
businesses to advertise on Google Search, YouTube and other sites across the web.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:GoogleAdsToGcsOperator:

Google Ads to GCS
^^^^^^^^^^^^^^^^^

To query the Google Ads API and generate a CSV report of the results use ``GoogleAdsToGcsOperator``.

.. exampleinclude:: /../../providers/tests/system/google/ads/example_ads.py
    :language: python
    :dedent: 4
    :start-after: [START howto_google_ads_to_gcs_operator]
    :end-before: [END howto_google_ads_to_gcs_operator]

Use :ref:`Jinja templating <concepts:jinja-templating>` with those parameters which allow you to
dynamically determine values:

* ``client_ids``
* ``query``
* ``attributes``
* ``bucket``
* ``obj``
* ``impersonation_chain``

The result is saved to :ref:`XCom <concepts:xcom>`, which allows the result to be used by other operators.

.. _howto/operator:GoogleAdsListAccountsOperator:

Upload Google Ads Accounts to GCS
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To upload Google Ads accounts to Google Cloud Storage bucket use the
``GoogleAdsListAccountsOperator``.

.. exampleinclude:: /../../providers/tests/system/google/ads/example_ads.py
    :language: python
    :dedent: 4
    :start-after: [START howto_ads_list_accounts_operator]
    :end-before: [END howto_ads_list_accounts_operator]

Use :ref:`Jinja templating <concepts:jinja-templating>` with those parameters which allow you to
dynamically determine values:

* ``bucket``
* ``object_name``
* ``impersonation_chain``

parameters which allow you to dynamically determine values.
The result is saved to :ref:`XCom <concepts:xcom>`, which allows the result to be used by other operators.

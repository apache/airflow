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



Facebook Ads To GCS Operators
==============================

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: _partials/prerequisite_tasks.rst

.. _howto/operator:FacebookAdsReportToGcsOperator:

FacebookAdsReportToGcsOperator
------------------------------

Use the
:class:`~airflow.providers.google.cloud.transfers.facebook_ads_to_gcs.FacebookAdsReportToGcsOperator`
to execute a Facebook ads report fetch and load to GCS.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_facebook_ads_to_gcs.py
    :language: python
    :start-after: [START howto_operator_facebook_ads_to_gcs]
    :end-before: [END howto_operator_facebook_ads_to_gcs]

Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://github.com/facebook/facebook-python-business-sdk>`__
* `Product Documentation <https://developers.facebook.com/docs/business-manager-api/>`__

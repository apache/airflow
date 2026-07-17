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

Google Bid Manager API Operators
=======================================
`Google Bid Manager API <https://developers.google.com/bid-manager/>`__ is a programmatic interface for the Display & Video 360 reporting feature.
It lets users build and run report queries, and download the resulting report file.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:GoogleBidManagerCreateQueryOperator:

Creating a Query
^^^^^^^^^^^^^^^^

To create a query using Bid Manager, use
:class:`~airflow.providers.google.marketing_platform.operators.bid_manager.GoogleBidManagerCreateQueryOperator`.

.. exampleinclude:: /../../google/tests/system/google/marketing_platform/example_bid_manager.py
    :language: python
    :dedent: 4
    :start-after: [START howto_google_bid_manager_create_query_operator]
    :end-before: [END howto_google_bid_manager_create_query_operator]

Use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.bid_manager.GoogleBidManagerCreateQueryOperator`
parameters which allow you to dynamically determine values. You can provide body definition using ``.json`` file
as this operator supports this template extension.
The result is saved to :ref:`XCom <concepts:xcom>`, which allows the result to be used by other operators.

.. _howto/operator:GoogleBidManagerRunQueryOperator:

Run Query
^^^^^^^^^

To run a query using Bid Manager, use
:class:`~airflow.providers.google.marketing_platform.operators.bid_manager.GoogleBidManagerRunQueryOperator`.

.. exampleinclude:: /../../google/tests/system/google/marketing_platform/example_bid_manager.py
    :language: python
    :dedent: 4
    :start-after: [START howto_google_bid_manager_run_query_report_operator]
    :end-before: [END howto_google_bid_manager_run_query_report_operator]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.bid_manager.GoogleBidManagerRunQueryOperator`
parameters which allow you to dynamically determine values.
The result is saved to :ref:`XCom <concepts:xcom>`, which allows the result to be used by other operators.

.. _howto/operator:GoogleBidManagerDeleteQueryOperator:

Deleting a Query
^^^^^^^^^^^^^^^^

To delete a query using Bid Manager, use
:class:`~airflow.providers.google.marketing_platform.operators.bid_manager.GoogleBidManagerDeleteQueryOperator`.

.. exampleinclude:: /../../google/tests/system/google/marketing_platform/example_bid_manager.py
    :language: python
    :dedent: 4
    :start-after: [START howto_google_bid_manager_delete_query_operator]
    :end-before: [END howto_google_bid_manager_delete_query_operator]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.bid_manager.GoogleBidManagerDeleteQueryOperator`
parameters which allow you to dynamically determine values.

.. _howto/operator:GoogleBidManagerRunQuerySensor:

Waiting for query
^^^^^^^^^^^^^^^^^

To wait for the report use
:class:`~airflow.providers.google.marketing_platform.sensors.bid_manager.GoogleBidManagerRunQuerySensor`.

.. exampleinclude:: /../../google/tests/system/google/marketing_platform/example_bid_manager.py
    :language: python
    :dedent: 4
    :start-after: [START howto_google_bid_manager_wait_run_query_sensor]
    :end-before: [END howto_google_bid_manager_wait_run_query_sensor]

Use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.sensors.bid_manager.GoogleBidManagerRunQuerySensor`
parameters which allow you to dynamically determine values.

.. _howto/operator:GoogleBidManagerDownloadReportOperator:

Downloading a report
^^^^^^^^^^^^^^^^^^^^

To download a report to GCS bucket use
:class:`~airflow.providers.google.marketing_platform.operators.bid_manager.GoogleBidManagerDownloadReportOperator`.

.. exampleinclude:: /../../google/tests/system/google/marketing_platform/example_bid_manager.py
    :language: python
    :dedent: 4
    :start-after: [START howto_google_bid_manager_get_report_operator]
    :end-before: [END howto_google_bid_manager_get_report_operator]

Use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.bid_manager.GoogleBidManagerDownloadReportOperator`
parameters which allow you to dynamically determine values.

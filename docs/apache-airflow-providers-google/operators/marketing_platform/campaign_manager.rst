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

Google Campaign Manager Operators
=================================

Google Campaign Manager operators allow you to insert, run, get or delete
reports. For more information about the Campaign Manager API check
`official documentation <https://developers.google.com/doubleclick-advertisers/v3.3/reports>`__.


.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:GoogleCampaignManagerDeleteReportOperator:

Deleting a report
^^^^^^^^^^^^^^^^^

To delete Campaign Manager report you can use the
:class:`~airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerDeleteReportOperator`.
It deletes a report by its unique ID.

.. exampleinclude:: /../../airflow/providers/google/marketing_platform/example_dags/example_campaign_manager.py
    :language: python
    :dedent: 4
    :start-after: [START howto_campaign_manager_delete_report_operator]
    :end-before: [END howto_campaign_manager_delete_report_operator]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerDeleteReportOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:GoogleCampaignManagerDownloadReportOperator:

Downloading a report
^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerDownloadReportOperator`.
allows you to download a Campaign Manager to Google Cloud Storage bucket.

.. exampleinclude:: /../../airflow/providers/google/marketing_platform/example_dags/example_campaign_manager.py
    :language: python
    :dedent: 4
    :start-after: [START howto_campaign_manager_get_report_operator]
    :end-before: [END howto_campaign_manager_get_report_operator]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerDownloadReportOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:GoogleCampaignManagerReportSensor:

Waiting for a report
^^^^^^^^^^^^^^^^^^^^

Report are generated asynchronously. To wait for report to be ready for downloading
you can use :class:`~airflow.providers.google.marketing_platform.sensors.campaign_manager.GoogleCampaignManagerReportSensor`.

.. exampleinclude:: /../../airflow/providers/google/marketing_platform/example_dags/example_campaign_manager.py
    :language: python
    :dedent: 4
    :start-after: [START howto_campaign_manager_wait_for_operation]
    :end-before: [END howto_campaign_manager_wait_for_operation]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.sensors.campaign_manager.GoogleCampaignManagerReportSensor`
parameters which allows you to dynamically determine values.

.. _howto/operator:GoogleCampaignManagerInsertReportOperator:

Inserting a new report
^^^^^^^^^^^^^^^^^^^^^^

To insert a Campaign Manager report you can use the
:class:`~airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerInsertReportOperator`.
Running this operator creates a new report.

.. exampleinclude:: /../../airflow/providers/google/marketing_platform/example_dags/example_campaign_manager.py
    :language: python
    :dedent: 4
    :start-after: [START howto_campaign_manager_insert_report_operator]
    :end-before: [END howto_campaign_manager_insert_report_operator]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerInsertReportOperator`
parameters which allows you to dynamically determine values. You can provide report definition using
``.json`` file as this operator supports this template extension.
The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

.. _howto/operator:GoogleCampaignManagerRunReportOperator:

Running a report
^^^^^^^^^^^^^^^^

To run Campaign Manager report you can use the
:class:`~airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerRunReportOperator`.

.. exampleinclude:: /../../airflow/providers/google/marketing_platform/example_dags/example_campaign_manager.py
    :language: python
    :dedent: 4
    :start-after: [START howto_campaign_manager_run_report_operator]
    :end-before: [END howto_campaign_manager_run_report_operator]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerRunReportOperator`
parameters which allows you to dynamically determine values.
The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

.. _howto/operator:GoogleCampaignManagerBatchInsertConversionsOperator:

Inserting a conversions
^^^^^^^^^^^^^^^^^^^^^^^

To insert Campaign Manager conversions you can use the
:class:`~airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerBatchInsertConversionsOperator`.

.. exampleinclude:: /../../airflow/providers/google/marketing_platform/example_dags/example_campaign_manager.py
    :language: python
    :dedent: 4
    :start-after: [START howto_campaign_manager_insert_conversions]
    :end-before: [END howto_campaign_manager_insert_conversions]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerBatchInsertConversionsOperator`
parameters which allows you to dynamically determine values.
The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

.. _howto/operator:GoogleCampaignManagerBatchUpdateConversionsOperator:

Updating a conversions
^^^^^^^^^^^^^^^^^^^^^^

To update Campaign Manager conversions you can use the
:class:`~airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerBatchUpdateConversionsOperator`.

.. exampleinclude:: /../../airflow/providers/google/marketing_platform/example_dags/example_campaign_manager.py
    :language: python
    :dedent: 4
    :start-after: [START howto_campaign_manager_update_conversions]
    :end-before: [END howto_campaign_manager_update_conversions]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerBatchUpdateConversionsOperator`
parameters which allows you to dynamically determine values.
The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

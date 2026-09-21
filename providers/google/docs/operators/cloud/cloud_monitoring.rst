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



Google Cloud Monitoring Operators
==================================

.. note::
    The ``Stackdriver*`` classes in the ``stackdriver`` modules are deprecated aliases for the
    ``CloudMonitoring*`` classes documented below, and will be removed after March 31, 2027.

Prerequisite Tasks
------------------

.. include:: /operators/_partials/prerequisite_tasks.rst


.. _howto/operator:CloudMonitoringListAlertPoliciesOperator:

CloudMonitoringListAlertPoliciesOperator
-----------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.cloud_monitoring.CloudMonitoringListAlertPoliciesOperator`
to fetch all the Alert Policies identified by given filter.

Using the operator
""""""""""""""""""

You can use this operator with or without project id to fetch all the alert policies.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_monitoring/example_cloud_monitoring.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_cloud_monitoring_list_alert_policy]
    :end-before: [END howto_operator_gcp_cloud_monitoring_list_alert_policy]

.. _howto/operator:CloudMonitoringEnableAlertPoliciesOperator:

CloudMonitoringEnableAlertPoliciesOperator
--------------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.cloud_monitoring.CloudMonitoringEnableAlertPoliciesOperator`
to enable Alert Policies identified by given filter.

Using the operator
""""""""""""""""""

You can use this operator with or without project id to enable alert policies.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_monitoring/example_cloud_monitoring.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_cloud_monitoring_enable_alert_policy]
    :end-before: [END howto_operator_gcp_cloud_monitoring_enable_alert_policy]

.. _howto/operator:CloudMonitoringDisableAlertPoliciesOperator:

CloudMonitoringDisableAlertPoliciesOperator
---------------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.cloud_monitoring.CloudMonitoringDisableAlertPoliciesOperator`
to disable Alert Policies identified by given filter.

Using the operator
""""""""""""""""""

You can use this operator with or without project id to disable alert policies.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_monitoring/example_cloud_monitoring.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_cloud_monitoring_disable_alert_policy]
    :end-before: [END howto_operator_gcp_cloud_monitoring_disable_alert_policy]

.. _howto/operator:CloudMonitoringUpsertAlertOperator:

CloudMonitoringUpsertAlertOperator
------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.cloud_monitoring.CloudMonitoringUpsertAlertOperator`
to upsert Alert Policies identified by given filter JSON string. If the alert with the given name already
exists, then the operator updates the existing policy otherwise creates a new one.

Using the operator
""""""""""""""""""

You can use this operator with or without project id to create or update alert policies.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_monitoring/example_cloud_monitoring.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_cloud_monitoring_upsert_alert_policy]
    :end-before: [END howto_operator_gcp_cloud_monitoring_upsert_alert_policy]

.. _howto/operator:CloudMonitoringDeleteAlertOperator:

CloudMonitoringDeleteAlertOperator
------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.cloud_monitoring.CloudMonitoringDeleteAlertOperator`
to delete an Alert Policy identified by given name.

Using the operator
""""""""""""""""""

The name of the alert to be deleted should be given in the format projects/<PROJECT_NAME>/alertPolicies/<ALERT_NAME>

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_monitoring/example_cloud_monitoring.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_cloud_monitoring_delete_alert_policy]
    :end-before: [END howto_operator_gcp_cloud_monitoring_delete_alert_policy]

.. _howto/operator:CloudMonitoringListNotificationChannelsOperator:

CloudMonitoringListNotificationChannelsOperator
--------------------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.cloud_monitoring.CloudMonitoringListNotificationChannelsOperator`
to fetch all the Notification Channels identified by given filter.

Using the operator
""""""""""""""""""

You can use this operator with or without project id to fetch all the notification channels.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_monitoring/example_cloud_monitoring.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_cloud_monitoring_list_notification_channel]
    :end-before: [END howto_operator_gcp_cloud_monitoring_list_notification_channel]

.. _howto/operator:CloudMonitoringEnableNotificationChannelsOperator:

CloudMonitoringEnableNotificationChannelsOperator
-----------------------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.cloud_monitoring.CloudMonitoringEnableNotificationChannelsOperator`
to enable Notification Channels identified by given filter.

Using the operator
""""""""""""""""""

You can use this operator with or without project id to enable notification channels.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_monitoring/example_cloud_monitoring.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_cloud_monitoring_enable_notification_channel]
    :end-before: [END howto_operator_gcp_cloud_monitoring_enable_notification_channel]

.. _howto/operator:CloudMonitoringDisableNotificationChannelsOperator:

CloudMonitoringDisableNotificationChannelsOperator
------------------------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.cloud_monitoring.CloudMonitoringDisableNotificationChannelsOperator`
to disable Notification Channels identified by given filter.

Using the operator
""""""""""""""""""

You can use this operator with or without project id to disable notification channels.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_monitoring/example_cloud_monitoring.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_cloud_monitoring_disable_notification_channel]
    :end-before: [END howto_operator_gcp_cloud_monitoring_disable_notification_channel]

.. _howto/operator:CloudMonitoringUpsertNotificationChannelOperator:

CloudMonitoringUpsertNotificationChannelOperator
----------------------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.cloud_monitoring.CloudMonitoringUpsertNotificationChannelOperator`
to upsert Notification Channels identified by given channel JSON string. If the channel with the given name already
exists, then the operator updates the existing channel otherwise creates a new one.

Using the operator
""""""""""""""""""

You can use this operator with or without project id to create or update notification channels.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_monitoring/example_cloud_monitoring.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_cloud_monitoring_upsert_notification_channel]
    :end-before: [END howto_operator_gcp_cloud_monitoring_upsert_notification_channel]

.. _howto/operator:CloudMonitoringDeleteNotificationChannelOperator:

CloudMonitoringDeleteNotificationChannelOperator
----------------------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.cloud_monitoring.CloudMonitoringDeleteNotificationChannelOperator`
to delete a Notification Channel identified by given name.

Using the operator
""""""""""""""""""

The name of the notification channel to be deleted should be given in the format projects/<PROJECT_NAME>/notificationChannels/<CHANNEL_NAME>

You can use this operator with or without project id to delete a notification channel.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_monitoring/example_cloud_monitoring.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_cloud_monitoring_delete_notification_channel]
    :end-before: [END howto_operator_gcp_cloud_monitoring_delete_notification_channel]

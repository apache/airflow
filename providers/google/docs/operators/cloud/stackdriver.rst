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

Prerequisite Tasks
------------------

.. include:: /operators/_partials/prerequisite_tasks.rst


.. _howto/operator:StackdriverListAlertPoliciesOperator:
.. _howto/operator:CloudMonitoringListAlertPoliciesOperator:

CloudMonitoringListAlertPoliciesOperator
----------------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.monitoring.CloudMonitoringListAlertPoliciesOperator`
to fetch all the Alert Policies identified by given filter.

Using the operator
""""""""""""""""""

You can use this operator with or without project id to fetch all the alert policies.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../google/tests/system/google/cloud/stackdriver/example_stackdriver.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_cloud_monitoring_list_alert_policy]
    :end-before: [END howto_operator_gcp_cloud_monitoring_list_alert_policy]

.. _howto/operator:StackdriverEnableAlertPoliciesOperator:
.. _howto/operator:CloudMonitoringEnableAlertPoliciesOperator:

CloudMonitoringEnableAlertPoliciesOperator
----------------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.monitoring.CloudMonitoringEnableAlertPoliciesOperator`
to enable Alert Policies identified by given filter.

Using the operator
""""""""""""""""""

You can use this operator with or without project id to fetch all the alert policies.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../google/tests/system/google/cloud/stackdriver/example_stackdriver.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_cloud_monitoring_enable_alert_policy]
    :end-before: [END howto_operator_gcp_cloud_monitoring_enable_alert_policy]

.. _howto/operator:StackdriverDisableAlertPoliciesOperator:
.. _howto/operator:CloudMonitoringDisableAlertPoliciesOperator:

CloudMonitoringDisableAlertPoliciesOperator
----------------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.monitoring.CloudMonitoringDisableAlertPoliciesOperator`
to disable Alert Policies identified by given filter.

Using the operator
""""""""""""""""""

You can use this operator with or without project id to fetch all the alert policies.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../google/tests/system/google/cloud/stackdriver/example_stackdriver.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_cloud_monitoring_disable_alert_policy]
    :end-before: [END howto_operator_gcp_cloud_monitoring_disable_alert_policy]

.. _howto/operator:StackdriverUpsertAlertOperator:
.. _howto/operator:CloudMonitoringUpsertAlertOperator:

CloudMonitoringUpsertAlertOperator
----------------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.monitoring.CloudMonitoringUpsertAlertOperator`
to upsert Alert Policies identified by given filter JSON string. If the alert with the give name already
exists, then the operator updates the existing policy otherwise creates a new one.

Using the operator
""""""""""""""""""

You can use this operator with or without project id to fetch all the alert policies.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../google/tests/system/google/cloud/stackdriver/example_stackdriver.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_cloud_monitoring_upsert_alert_policy]
    :end-before: [END howto_operator_gcp_cloud_monitoring_upsert_alert_policy]

.. _howto/operator:StackdriverDeleteAlertOperator:
.. _howto/operator:CloudMonitoringDeleteAlertOperator:

CloudMonitoringDeleteAlertOperator
----------------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.monitoring.CloudMonitoringDeleteAlertOperator`
to delete an Alert Policy identified by given name.

Using the operator
""""""""""""""""""

The name of the alert to be deleted should be given in the format projects/<PROJECT_NAME>/alertPolicies/<ALERT_NAME>

.. exampleinclude:: /../../google/tests/system/google/cloud/stackdriver/example_stackdriver.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_cloud_monitoring_delete_alert_policy]
    :end-before: [END howto_operator_gcp_cloud_monitoring_delete_alert_policy]

.. _howto/operator:StackdriverListNotificationChannelsOperator:
.. _howto/operator:CloudMonitoringListNotificationChannelsOperator:

CloudMonitoringListNotificationChannelsOperator
----------------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.monitoring.CloudMonitoringListNotificationChannelsOperator`
to fetch all the Notification Channels identified by given filter.

Using the operator
""""""""""""""""""

You can use this operator with or without project id to fetch all the alert policies.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../google/tests/system/google/cloud/stackdriver/example_stackdriver.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_cloud_monitoring_list_notification_channel]
    :end-before: [END howto_operator_gcp_cloud_monitoring_list_notification_channel]

.. _howto/operator:StackdriverEnableNotificationChannelsOperator:
.. _howto/operator:CloudMonitoringEnableNotificationChannelsOperator:

CloudMonitoringEnableNotificationChannelsOperator
----------------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.monitoring.CloudMonitoringEnableNotificationChannelsOperator`
to enable Notification Channels identified by given filter.

Using the operator
""""""""""""""""""

You can use this operator with or without project id to fetch all the alert policies.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../google/tests/system/google/cloud/stackdriver/example_stackdriver.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_cloud_monitoring_enable_notification_channel]
    :end-before: [END howto_operator_gcp_cloud_monitoring_enable_notification_channel]

.. _howto/operator:StackdriverDisableNotificationChannelsOperator:
.. _howto/operator:CloudMonitoringDisableNotificationChannelsOperator:

CloudMonitoringDisableNotificationChannelsOperator
----------------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.monitoring.CloudMonitoringDisableNotificationChannelsOperator`
to disable Notification Channels identified by given filter.

Using the operator
""""""""""""""""""

You can use this operator with or without project id to fetch all the alert policies.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../google/tests/system/google/cloud/stackdriver/example_stackdriver.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_cloud_monitoring_upsert_notification_channel]
    :end-before: [END howto_operator_gcp_cloud_monitoring_upsert_notification_channel]

.. _howto/operator:StackdriverUpsertNotificationChannelOperator:
.. _howto/operator:CloudMonitoringUpsertNotificationChannelOperator:

CloudMonitoringUpsertNotificationChannelOperator
----------------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.monitoring.CloudMonitoringUpsertNotificationChannelOperator`
to upsert Notification Channels identified by given channel JSON string. If the channel with the give name already
exists, then the operator updates the existing channel otherwise creates a new one.

Using the operator
""""""""""""""""""

You can use this operator with or without project id to fetch all the alert policies.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../google/tests/system/google/cloud/stackdriver/example_stackdriver.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_cloud_monitoring_disable_notification_channel]
    :end-before: [END howto_operator_gcp_cloud_monitoring_disable_notification_channel]

.. _howto/operator:StackdriverDeleteNotificationChannelOperator:
.. _howto/operator:CloudMonitoringDeleteNotificationChannelOperator:

CloudMonitoringDeleteNotificationChannelOperator
----------------------------------------------

The name of the alert to be deleted should be given in the format projects/<PROJECT_NAME>/notificationChannels/<CHANNEL_NAME>

Using the operator
""""""""""""""""""

You can use this operator with or without project id to fetch all the alert policies.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../google/tests/system/google/cloud/stackdriver/example_stackdriver.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_cloud_monitoring_delete_notification_channel]
    :end-before: [END howto_operator_gcp_cloud_monitoring_delete_notification_channel]

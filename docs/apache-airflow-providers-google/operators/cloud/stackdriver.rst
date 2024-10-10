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



Google Cloud Stackdriver Operators
==================================

Prerequisite Tasks
------------------

.. include:: /operators/_partials/prerequisite_tasks.rst


.. _howto/operator:StackdriverListAlertPoliciesOperator:

StackdriverListAlertPoliciesOperator
------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.stackdriver.StackdriverListAlertPoliciesOperator`
to fetch all the Alert Policies identified by given filter.

Using the operator
""""""""""""""""""

You can use this operator with or without project id to fetch all the alert policies.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../providers/tests/system/google/cloud/stackdriver/example_stackdriver.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_stackdriver_list_alert_policy]
    :end-before: [END howto_operator_gcp_stackdriver_list_alert_policy]

.. _howto/operator:StackdriverEnableAlertPoliciesOperator:

StackdriverEnableAlertPoliciesOperator
--------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.stackdriver.StackdriverEnableAlertPoliciesOperator`
to enable Alert Policies identified by given filter.

Using the operator
""""""""""""""""""

You can use this operator with or without project id to fetch all the alert policies.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../providers/tests/system/google/cloud/stackdriver/example_stackdriver.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_stackdriver_enable_alert_policy]
    :end-before: [END howto_operator_gcp_stackdriver_enable_alert_policy]

.. _howto/operator:StackdriverDisableAlertPoliciesOperator:

StackdriverDisableAlertPoliciesOperator
---------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.stackdriver.StackdriverDisableAlertPoliciesOperator`
to disable Alert Policies identified by given filter.

Using the operator
""""""""""""""""""

You can use this operator with or without project id to fetch all the alert policies.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../providers/tests/system/google/cloud/stackdriver/example_stackdriver.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_stackdriver_disable_alert_policy]
    :end-before: [END howto_operator_gcp_stackdriver_disable_alert_policy]

.. _howto/operator:StackdriverUpsertAlertOperator:

StackdriverUpsertAlertOperator
------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.stackdriver.StackdriverUpsertAlertOperator`
to upsert Alert Policies identified by given filter JSON string. If the alert with the give name already
exists, then the operator updates the existing policy otherwise creates a new one.

Using the operator
""""""""""""""""""

You can use this operator with or without project id to fetch all the alert policies.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../providers/tests/system/google/cloud/stackdriver/example_stackdriver.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_stackdriver_upsert_alert_policy]
    :end-before: [END howto_operator_gcp_stackdriver_upsert_alert_policy]

.. _howto/operator:StackdriverDeleteAlertOperator:

StackdriverDeleteAlertOperator
------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.stackdriver.StackdriverDeleteAlertOperator`
to delete an Alert Policy identified by given name.

Using the operator
""""""""""""""""""

The name of the alert to be deleted should be given in the format projects/<PROJECT_NAME>/alertPolicies/<ALERT_NAME>

.. exampleinclude:: /../../providers/tests/system/google/cloud/stackdriver/example_stackdriver.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_stackdriver_delete_alert_policy]
    :end-before: [END howto_operator_gcp_stackdriver_delete_alert_policy]

.. _howto/operator:StackdriverListNotificationChannelsOperator:

StackdriverListNotificationChannelsOperator
-------------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.stackdriver.StackdriverListNotificationChannelsOperator`
to fetch all the Notification Channels identified by given filter.

Using the operator
""""""""""""""""""

You can use this operator with or without project id to fetch all the alert policies.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../providers/tests/system/google/cloud/stackdriver/example_stackdriver.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_stackdriver_list_notification_channel]
    :end-before: [END howto_operator_gcp_stackdriver_list_notification_channel]

.. _howto/operator:StackdriverEnableNotificationChannelsOperator:

StackdriverEnableNotificationChannelsOperator
---------------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.stackdriver.StackdriverEnableNotificationChannelsOperator`
to enable Notification Channels identified by given filter.

Using the operator
""""""""""""""""""

You can use this operator with or without project id to fetch all the alert policies.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../providers/tests/system/google/cloud/stackdriver/example_stackdriver.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_stackdriver_enable_notification_channel]
    :end-before: [END howto_operator_gcp_stackdriver_enable_notification_channel]

.. _howto/operator:StackdriverDisableNotificationChannelsOperator:

StackdriverDisableNotificationChannelsOperator
----------------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.stackdriver.StackdriverDisableNotificationChannelsOperator`
to disable Notification Channels identified by given filter.

Using the operator
""""""""""""""""""

You can use this operator with or without project id to fetch all the alert policies.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../providers/tests/system/google/cloud/stackdriver/example_stackdriver.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_stackdriver_disable_notification_channel]
    :end-before: [END howto_operator_gcp_stackdriver_disable_notification_channel]

.. _howto/operator:StackdriverUpsertNotificationChannelOperator:

StackdriverUpsertNotificationChannelOperator
--------------------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.stackdriver.StackdriverUpsertNotificationChannelOperator`
to upsert Notification Channels identified by given channel JSON string. If the channel with the give name already
exists, then the operator updates the existing channel otherwise creates a new one.

Using the operator
""""""""""""""""""

You can use this operator with or without project id to fetch all the alert policies.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../providers/tests/system/google/cloud/stackdriver/example_stackdriver.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_stackdriver_disable_notification_channel]
    :end-before: [END howto_operator_gcp_stackdriver_disable_notification_channel]

.. _howto/operator:StackdriverDeleteNotificationChannelOperator:

StackdriverDeleteNotificationChannelOperator
--------------------------------------------

The name of the alert to be deleted should be given in the format projects/<PROJECT_NAME>/notificationChannels/<CHANNEL_NAME>

Using the operator
""""""""""""""""""

You can use this operator with or without project id to fetch all the alert policies.
If project id is missing it will be retrieved from Google Cloud connection used.

.. exampleinclude:: /../../providers/tests/system/google/cloud/stackdriver/example_stackdriver.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_stackdriver_delete_notification_channel]
    :end-before: [END howto_operator_gcp_stackdriver_delete_notification_channel]

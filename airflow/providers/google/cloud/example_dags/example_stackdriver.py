#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Example Airflow DAG for Google Cloud Stackdriver service.
"""

import json

from airflow.models import DAG
from airflow.providers.google.cloud.operators.stackdriver import (
    StackdriverDeleteAlertOperator, StackdriverDeleteNotificationChannelOperator,
    StackdriverDisableAlertPoliciesOperator, StackdriverDisableNotificationChannelsOperator,
    StackdriverEnableAlertPoliciesOperator, StackdriverEnableNotificationChannelsOperator,
    StackdriverListAlertPoliciesOperator, StackdriverListNotificationChannelsOperator,
    StackdriverUpsertAlertOperator, StackdriverUpsertNotificationChannelOperator,
)
from airflow.utils.dates import days_ago

TEST_ALERT_POLICY_1 = {
    "combiner": "OR",
    "name": "projects/sd-project/alertPolicies/12345",
    "creationRecord": {
        "mutatedBy": "user123",
        "mutateTime": "2020-01-01T00:00:00.000000Z"
    },
    "enabled": True,
    "displayName": "test alert 1",
    "conditions": [
        {
            "conditionThreshold": {
                "comparison": "COMPARISON_GT",
                "aggregations": [
                    {
                        "alignmentPeriod": "60s",
                        "perSeriesAligner": "ALIGN_RATE"
                    }
                ]
            },
            "displayName": "Condition display",
            "name": "projects/sd-project/alertPolicies/123/conditions/456"
        }
    ]
}

TEST_ALERT_POLICY_2 = {
    "combiner": "OR",
    "name": "projects/sd-project/alertPolicies/6789",
    "creationRecord": {
        "mutatedBy": "user123",
        "mutateTime": "2020-01-01T00:00:00.000000Z"
    },
    "enabled": False,
    "displayName": "test alert 2",
    "conditions": [
        {
            "conditionThreshold": {
                "comparison": "COMPARISON_GT",
                "aggregations": [
                    {
                        "alignmentPeriod": "60s",
                        "perSeriesAligner": "ALIGN_RATE"
                    }
                ]
            },
            "displayName": "Condition display",
            "name": "projects/sd-project/alertPolicies/456/conditions/789"
        }
    ]
}

TEST_NOTIFICATION_CHANNEL_1 = {
    "displayName": "channel1",
    "enabled": True,
    "labels": {
        "auth_token": "top-secret",
        "channel_name": "#channel"
    },
    "name": "projects/sd-project/notificationChannels/12345",
    "type": "slack"
}

TEST_NOTIFICATION_CHANNEL_2 = {
    "displayName": "channel2",
    "enabled": False,
    "labels": {
        "auth_token": "top-secret",
        "channel_name": "#channel"
    },
    "name": "projects/sd-project/notificationChannels/6789",
    "type": "slack"
}

default_args = {"start_date": days_ago(1)}

with DAG(
    'example_stackdriver',
    default_args=default_args,
    tags=['example']
) as dag:
    # [START howto_operator_gcp_stackdriver_upsert_notification_channel]
    create_notification_channel = StackdriverUpsertNotificationChannelOperator(
        task_id='create-notification-channel',
        channels=json.dumps({"channels": [TEST_NOTIFICATION_CHANNEL_1, TEST_NOTIFICATION_CHANNEL_2]}),
    )
    # [END howto_operator_gcp_stackdriver_upsert_notification_channel]

    # [START howto_operator_gcp_stackdriver_enable_notification_channel]
    enable_notification_channel = StackdriverEnableNotificationChannelsOperator(
        task_id='enable-notification-channel',
        filter_='type="slack"'
    )
    # [END howto_operator_gcp_stackdriver_enable_notification_channel]

    # [START howto_operator_gcp_stackdriver_disable_notification_channel]
    disable_notification_channel = StackdriverDisableNotificationChannelsOperator(
        task_id='disable-notification-channel',
        filter_='displayName="channel1"'
    )
    # [END howto_operator_gcp_stackdriver_disable_notification_channel]

    # [START howto_operator_gcp_stackdriver_list_notification_channel]
    list_notification_channel = StackdriverListNotificationChannelsOperator(
        task_id='list-notification-channel',
        filter_='type="slack"'
    )
    # [END howto_operator_gcp_stackdriver_list_notification_channel]

    # [START howto_operator_gcp_stackdriver_upsert_alert_policy]
    create_alert_policy = StackdriverUpsertAlertOperator(
        task_id='create-alert-policies',
        alerts=json.dumps({"policies": [TEST_ALERT_POLICY_1, TEST_ALERT_POLICY_2]}),
    )
    # [END howto_operator_gcp_stackdriver_upsert_alert_policy]

    # [START howto_operator_gcp_stackdriver_enable_alert_policy]
    enable_alert_policy = StackdriverEnableAlertPoliciesOperator(
        task_id='enable-alert-policies',
        filter_='(displayName="test alert 1" OR displayName="test alert 2")',
    )
    # [END howto_operator_gcp_stackdriver_enable_alert_policy]

    # [START howto_operator_gcp_stackdriver_disable_alert_policy]
    disable_alert_policy = StackdriverDisableAlertPoliciesOperator(
        task_id='disable-alert-policies',
        filter_='displayName="test alert 1"',
    )
    # [END howto_operator_gcp_stackdriver_disable_alert_policy]

    # [START howto_operator_gcp_stackdriver_list_alert_policy]
    list_alert_policies = StackdriverListAlertPoliciesOperator(
        task_id='list-alert-policies',
    )
    # [END howto_operator_gcp_stackdriver_list_alert_policy]

    # [START howto_operator_gcp_stackdriver_delete_notification_channel]
    delete_notification_channel = StackdriverDeleteNotificationChannelOperator(
        task_id='delete-notification-channel',
        name='test-channel',
    )
    # [END howto_operator_gcp_stackdriver_delete_notification_channel]

    # [START howto_operator_gcp_stackdriver_delete_alert_policy]
    delete_alert_policy = StackdriverDeleteAlertOperator(
        task_id='delete-alert-polciy',
        name='test-alert',
    )
    # [END howto_operator_gcp_stackdriver_delete_alert_policy]

    create_notification_channel >> enable_notification_channel >> disable_notification_channel \
        >> list_notification_channel >> create_alert_policy >> enable_alert_policy >> disable_alert_policy \
        >> list_alert_policies >> delete_notification_channel >> delete_alert_policy

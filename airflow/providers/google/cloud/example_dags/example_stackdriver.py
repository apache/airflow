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
import os

from airflow import models
from airflow.providers.google.cloud.operators.stackdriver import (
    StackdriverDeleteAlertOperator,
    StackdriverDeleteNotificationChannelOperator,
    StackdriverDisableAlertPoliciesOperator,
    StackdriverDisableNotificationChannelsOperator,
    StackdriverEnableAlertPoliciesOperator,
    StackdriverEnableNotificationChannelsOperator,
    StackdriverListAlertPoliciesOperator,
    StackdriverListNotificationChannelsOperator,
    StackdriverUpsertAlertOperator,
    StackdriverUpsertNotificationChannelOperator,
)
from airflow.utils.dates import days_ago

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")

TEST_ALERT_POLICY_1 = {
    "combiner": "OR",
    "enabled": True,
    "display_name": "test alert 1",
    "conditions": [
        {
            "condition_threshold": {
                "filter": (
                    'metric.label.state="blocked" AND '
                    'metric.type="agent.googleapis.com/processes/count_by_state" '
                    'AND resource.type="gce_instance"'
                ),
                "comparison": "COMPARISON_GT",
                "threshold_value": 100,
                "duration": {'seconds': 900},
                "trigger": {"percent": 0},
                "aggregations": [
                    {
                        "alignment_period": {'seconds': 60},
                        "per_series_aligner": "ALIGN_MEAN",
                        "cross_series_reducer": "REDUCE_MEAN",
                        "group_by_fields": ["project", "resource.label.instance_id", "resource.label.zone"],
                    }
                ],
            },
            "display_name": "test_alert_policy_1",
        }
    ],
}

TEST_ALERT_POLICY_2 = {
    "combiner": "OR",
    "enabled": False,
    "display_name": "test alert 2",
    "conditions": [
        {
            "condition_threshold": {
                "filter": (
                    'metric.label.state="blocked" AND '
                    'metric.type="agent.googleapis.com/processes/count_by_state" AND '
                    'resource.type="gce_instance"'
                ),
                "comparison": "COMPARISON_GT",
                "threshold_value": 100,
                "duration": {'seconds': 900},
                "trigger": {"percent": 0},
                "aggregations": [
                    {
                        "alignment_period": {'seconds': 60},
                        "per_series_aligner": "ALIGN_MEAN",
                        "cross_series_reducer": "REDUCE_MEAN",
                        "group_by_fields": ["project", "resource.label.instance_id", "resource.label.zone"],
                    }
                ],
            },
            "display_name": "test_alert_policy_2",
        }
    ],
}

TEST_NOTIFICATION_CHANNEL_1 = {
    "display_name": "channel1",
    "enabled": True,
    "labels": {"auth_token": "top-secret", "channel_name": "#channel"},
    "type_": "slack",
}

TEST_NOTIFICATION_CHANNEL_2 = {
    "display_name": "channel2",
    "enabled": False,
    "labels": {"auth_token": "top-secret", "channel_name": "#channel"},
    "type_": "slack",
}

with models.DAG(
    'example_stackdriver',
    schedule_interval=None,  # Override to match your needs
    start_date=days_ago(1),
    tags=['example'],
) as dag:
    # [START howto_operator_gcp_stackdriver_upsert_notification_channel]
    create_notification_channel = StackdriverUpsertNotificationChannelOperator(
        task_id='create-notification-channel',
        channels=json.dumps({"channels": [TEST_NOTIFICATION_CHANNEL_1, TEST_NOTIFICATION_CHANNEL_2]}),
    )
    # [END howto_operator_gcp_stackdriver_upsert_notification_channel]

    # [START howto_operator_gcp_stackdriver_enable_notification_channel]
    enable_notification_channel = StackdriverEnableNotificationChannelsOperator(
        task_id='enable-notification-channel', filter_='type="slack"'
    )
    # [END howto_operator_gcp_stackdriver_enable_notification_channel]

    # [START howto_operator_gcp_stackdriver_disable_notification_channel]
    disable_notification_channel = StackdriverDisableNotificationChannelsOperator(
        task_id='disable-notification-channel', filter_='displayName="channel1"'
    )
    # [END howto_operator_gcp_stackdriver_disable_notification_channel]

    # [START howto_operator_gcp_stackdriver_list_notification_channel]
    list_notification_channel = StackdriverListNotificationChannelsOperator(
        task_id='list-notification-channel', filter_='type="slack"'
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
        name="{{ task_instance.xcom_pull('list-notification-channel')[0]['name'] }}",
    )
    # [END howto_operator_gcp_stackdriver_delete_notification_channel]

    delete_notification_channel_2 = StackdriverDeleteNotificationChannelOperator(
        task_id='delete-notification-channel-2',
        name="{{ task_instance.xcom_pull('list-notification-channel')[1]['name'] }}",
    )

    # [START howto_operator_gcp_stackdriver_delete_alert_policy]
    delete_alert_policy = StackdriverDeleteAlertOperator(
        task_id='delete-alert-policy',
        name="{{ task_instance.xcom_pull('list-alert-policies')[0]['name'] }}",
    )
    # [END howto_operator_gcp_stackdriver_delete_alert_policy]

    delete_alert_policy_2 = StackdriverDeleteAlertOperator(
        task_id='delete-alert-policy-2',
        name="{{ task_instance.xcom_pull('list-alert-policies')[1]['name'] }}",
    )

    create_notification_channel >> enable_notification_channel >> disable_notification_channel
    disable_notification_channel >> list_notification_channel >> create_alert_policy
    create_alert_policy >> enable_alert_policy >> disable_alert_policy >> list_alert_policies
    list_alert_policies >> delete_notification_channel >> delete_notification_channel_2
    delete_notification_channel_2 >> delete_alert_policy >> delete_alert_policy_2

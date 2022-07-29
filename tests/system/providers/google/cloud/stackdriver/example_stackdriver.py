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
from datetime import datetime

from airflow import models
from airflow.models.baseoperator import chain
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
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")
DAG_ID = "stackdriver"

ALERT_1_NAME = f"alert_{ENV_ID}_1"
ALERT_2_NAME = f"alert_{ENV_ID}_2"
CHANNEL_1_NAME = f"channel_{ENV_ID}_1"
CHANNEL_2_NAME = f"channel_{ENV_ID}_2"

TEST_ALERT_POLICY_1 = {
    "combiner": "OR",
    "enabled": True,
    "display_name": ALERT_1_NAME,
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
            "display_name": f"{ALERT_1_NAME}_policy_1",
        }
    ],
}

TEST_ALERT_POLICY_2 = {
    "combiner": "OR",
    "enabled": False,
    "display_name": ALERT_2_NAME,
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
            "display_name": f"{ALERT_2_NAME}_policy_2",
        }
    ],
}

TEST_NOTIFICATION_CHANNEL_1 = {
    "display_name": CHANNEL_1_NAME,
    "enabled": True,
    "labels": {"auth_token": "top-secret", "channel_name": "#channel"},
    "type_": "slack",
}

TEST_NOTIFICATION_CHANNEL_2 = {
    "display_name": CHANNEL_2_NAME,
    "enabled": False,
    "labels": {"auth_token": "top-secret", "channel_name": "#channel"},
    "type_": "slack",
}

with models.DAG(
    dag_id=DAG_ID,
    schedule='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example', "stackdriver"],
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
        task_id='disable-notification-channel', filter_=f'displayName="{CHANNEL_1_NAME}"'
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
        filter_=f'(displayName="{ALERT_1_NAME}" OR displayName="{ALERT_2_NAME}")',
    )
    # [END howto_operator_gcp_stackdriver_enable_alert_policy]

    # [START howto_operator_gcp_stackdriver_disable_alert_policy]
    disable_alert_policy = StackdriverDisableAlertPoliciesOperator(
        task_id='disable-alert-policies',
        filter_=f'displayName="{ALERT_1_NAME}"',
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
    delete_notification_channel.trigger_rule = TriggerRule.ALL_DONE

    delete_notification_channel_2 = StackdriverDeleteNotificationChannelOperator(
        task_id='delete-notification-channel-2',
        name="{{ task_instance.xcom_pull('list-notification-channel')[1]['name'] }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # [START howto_operator_gcp_stackdriver_delete_alert_policy]
    delete_alert_policy = StackdriverDeleteAlertOperator(
        task_id='delete-alert-policy',
        name="{{ task_instance.xcom_pull('list-alert-policies')[0]['name'] }}",
    )
    # [END howto_operator_gcp_stackdriver_delete_alert_policy]
    delete_alert_policy.trigger_rule = TriggerRule.ALL_DONE

    delete_alert_policy_2 = StackdriverDeleteAlertOperator(
        task_id='delete-alert-policy-2',
        name="{{ task_instance.xcom_pull('list-alert-policies')[1]['name'] }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        create_notification_channel,
        enable_notification_channel,
        disable_notification_channel,
        list_notification_channel,
        create_alert_policy,
        enable_alert_policy,
        disable_alert_policy,
        list_alert_policies,
        delete_notification_channel,
        delete_notification_channel_2,
        delete_alert_policy,
        delete_alert_policy_2,
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)

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

import json
import unittest

from unittest import mock
from google.api_core.gapic_v1.method import DEFAULT
from google.cloud import monitoring_v3
from google.protobuf.json_format import ParseDict

from airflow.providers.google.cloud.hooks import stackdriver

PROJECT_ID = "sd-project"
CREDENTIALS = "sd-credentials"
TEST_FILTER = "filter"
TEST_ALERT_POLICY_1 = {
    "combiner": "OR",
    "name": "projects/sd-project/alertPolicies/12345",
    "creationRecord": {"mutatedBy": "user123", "mutateTime": "2020-01-01T00:00:00.000000Z"},
    "enabled": True,
    "displayName": "test display",
    "conditions": [
        {
            "conditionThreshold": {
                "comparison": "COMPARISON_GT",
                "aggregations": [{"alignmentPeriod": "60s", "perSeriesAligner": "ALIGN_RATE"}],
            },
            "displayName": "Condition display",
            "name": "projects/sd-project/alertPolicies/123/conditions/456",
        }
    ],
}

TEST_ALERT_POLICY_2 = {
    "combiner": "OR",
    "name": "projects/sd-project/alertPolicies/6789",
    "creationRecord": {"mutatedBy": "user123", "mutateTime": "2020-01-01T00:00:00.000000Z"},
    "enabled": False,
    "displayName": "test display",
    "conditions": [
        {
            "conditionThreshold": {
                "comparison": "COMPARISON_GT",
                "aggregations": [{"alignmentPeriod": "60s", "perSeriesAligner": "ALIGN_RATE"}],
            },
            "displayName": "Condition display",
            "name": "projects/sd-project/alertPolicies/456/conditions/789",
        }
    ],
}

TEST_NOTIFICATION_CHANNEL_1 = {
    "displayName": "sd",
    "enabled": True,
    "labels": {"auth_token": "top-secret", "channel_name": "#channel"},
    "name": "projects/sd-project/notificationChannels/12345",
    "type": "slack",
}

TEST_NOTIFICATION_CHANNEL_2 = {
    "displayName": "sd",
    "enabled": False,
    "labels": {"auth_token": "top-secret", "channel_name": "#channel"},
    "name": "projects/sd-project/notificationChannels/6789",
    "type": "slack",
}


class TestStackdriverHookMethods(unittest.TestCase):
    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id',
        return_value=(CREDENTIALS, PROJECT_ID),
    )
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_policy_client')
    def test_stackdriver_list_alert_policies(self, mock_policy_client, mock_get_creds_and_project_id):
        method = mock_policy_client.return_value.list_alert_policies
        hook = stackdriver.StackdriverHook()
        hook.list_alert_policies(
            filter_=TEST_FILTER,
            project_id=PROJECT_ID,
        )
        method.assert_called_once_with(
            name=f'projects/{PROJECT_ID}',
            filter_=TEST_FILTER,
            retry=DEFAULT,
            timeout=DEFAULT,
            order_by=None,
            page_size=None,
            metadata=None,
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id',
        return_value=(CREDENTIALS, PROJECT_ID),
    )
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_policy_client')
    def test_stackdriver_enable_alert_policy(self, mock_policy_client, mock_get_creds_and_project_id):
        hook = stackdriver.StackdriverHook()

        alert_policy_enabled = ParseDict(TEST_ALERT_POLICY_1, monitoring_v3.types.alert_pb2.AlertPolicy())
        alert_policy_disabled = ParseDict(TEST_ALERT_POLICY_2, monitoring_v3.types.alert_pb2.AlertPolicy())

        alert_policies = [alert_policy_enabled, alert_policy_disabled]

        mock_policy_client.return_value.list_alert_policies.return_value = alert_policies
        hook.enable_alert_policies(
            filter_=TEST_FILTER,
            project_id=PROJECT_ID,
        )
        mock_policy_client.return_value.list_alert_policies.assert_called_once_with(
            name=f'projects/{PROJECT_ID}',
            filter_=TEST_FILTER,
            retry=DEFAULT,
            timeout=DEFAULT,
            order_by=None,
            page_size=None,
            metadata=None,
        )
        mask = monitoring_v3.types.field_mask_pb2.FieldMask()
        alert_policy_disabled.enabled.value = True  # pylint: disable=no-member
        mask.paths.append('enabled')  # pylint: disable=no-member
        mock_policy_client.return_value.update_alert_policy.assert_called_once_with(
            alert_policy=alert_policy_disabled,
            update_mask=mask,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None,
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id',
        return_value=(CREDENTIALS, PROJECT_ID),
    )
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_policy_client')
    def test_stackdriver_disable_alert_policy(self, mock_policy_client, mock_get_creds_and_project_id):
        hook = stackdriver.StackdriverHook()
        alert_policy_enabled = ParseDict(TEST_ALERT_POLICY_1, monitoring_v3.types.alert_pb2.AlertPolicy())
        alert_policy_disabled = ParseDict(TEST_ALERT_POLICY_2, monitoring_v3.types.alert_pb2.AlertPolicy())

        mock_policy_client.return_value.list_alert_policies.return_value = [
            alert_policy_enabled,
            alert_policy_disabled,
        ]
        hook.disable_alert_policies(
            filter_=TEST_FILTER,
            project_id=PROJECT_ID,
        )
        mock_policy_client.return_value.list_alert_policies.assert_called_once_with(
            name=f'projects/{PROJECT_ID}',
            filter_=TEST_FILTER,
            retry=DEFAULT,
            timeout=DEFAULT,
            order_by=None,
            page_size=None,
            metadata=None,
        )
        mask = monitoring_v3.types.field_mask_pb2.FieldMask()
        alert_policy_enabled.enabled.value = False  # pylint: disable=no-member
        mask.paths.append('enabled')  # pylint: disable=no-member
        mock_policy_client.return_value.update_alert_policy.assert_called_once_with(
            alert_policy=alert_policy_enabled,
            update_mask=mask,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None,
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id',
        return_value=(CREDENTIALS, PROJECT_ID),
    )
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_policy_client')
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_channel_client')
    def test_stackdriver_upsert_alert_policy(
        self, mock_channel_client, mock_policy_client, mock_get_creds_and_project_id
    ):
        hook = stackdriver.StackdriverHook()
        existing_alert_policy = ParseDict(TEST_ALERT_POLICY_1, monitoring_v3.types.alert_pb2.AlertPolicy())
        alert_policy_to_create = ParseDict(TEST_ALERT_POLICY_2, monitoring_v3.types.alert_pb2.AlertPolicy())

        mock_policy_client.return_value.list_alert_policies.return_value = [existing_alert_policy]
        mock_channel_client.return_value.list_notification_channels.return_value = []

        hook.upsert_alert(
            alerts=json.dumps({"policies": [TEST_ALERT_POLICY_1, TEST_ALERT_POLICY_2], "channels": []}),
            project_id=PROJECT_ID,
        )
        mock_channel_client.return_value.list_notification_channels.assert_called_once_with(
            name=f'projects/{PROJECT_ID}',
            filter_=None,
            retry=DEFAULT,
            timeout=DEFAULT,
            order_by=None,
            page_size=None,
            metadata=None,
        )
        mock_policy_client.return_value.list_alert_policies.assert_called_once_with(
            name=f'projects/{PROJECT_ID}',
            filter_=None,
            retry=DEFAULT,
            timeout=DEFAULT,
            order_by=None,
            page_size=None,
            metadata=None,
        )
        alert_policy_to_create.ClearField('name')
        alert_policy_to_create.ClearField('creation_record')
        alert_policy_to_create.ClearField('mutation_record')
        alert_policy_to_create.conditions[0].ClearField('name')  # pylint: disable=no-member
        mock_policy_client.return_value.create_alert_policy.assert_called_once_with(
            name=f'projects/{PROJECT_ID}',
            alert_policy=alert_policy_to_create,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None,
        )
        existing_alert_policy.ClearField('creation_record')
        existing_alert_policy.ClearField('mutation_record')
        mock_policy_client.return_value.update_alert_policy.assert_called_once_with(
            alert_policy=existing_alert_policy, retry=DEFAULT, timeout=DEFAULT, metadata=None
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id',
        return_value=(CREDENTIALS, PROJECT_ID),
    )
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_policy_client')
    def test_stackdriver_delete_alert_policy(self, mock_policy_client, mock_get_creds_and_project_id):
        hook = stackdriver.StackdriverHook()
        hook.delete_alert_policy(
            name='test-alert',
        )
        mock_policy_client.return_value.delete_alert_policy.assert_called_once_with(
            name='test-alert',
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None,
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id',
        return_value=(CREDENTIALS, PROJECT_ID),
    )
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_channel_client')
    def test_stackdriver_list_notification_channel(self, mock_channel_client, mock_get_creds_and_project_id):
        hook = stackdriver.StackdriverHook()
        hook.list_notification_channels(
            filter_=TEST_FILTER,
            project_id=PROJECT_ID,
        )
        mock_channel_client.return_value.list_notification_channels.assert_called_once_with(
            name=f'projects/{PROJECT_ID}',
            filter_=TEST_FILTER,
            order_by=None,
            page_size=None,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None,
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id',
        return_value=(CREDENTIALS, PROJECT_ID),
    )
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_channel_client')
    def test_stackdriver_enable_notification_channel(
        self, mock_channel_client, mock_get_creds_and_project_id
    ):
        hook = stackdriver.StackdriverHook()
        notification_channel_enabled = ParseDict(
            TEST_NOTIFICATION_CHANNEL_1, monitoring_v3.types.notification_pb2.NotificationChannel()
        )
        notification_channel_disabled = ParseDict(
            TEST_NOTIFICATION_CHANNEL_2, monitoring_v3.types.notification_pb2.NotificationChannel()
        )
        mock_channel_client.return_value.list_notification_channels.return_value = [
            notification_channel_enabled,
            notification_channel_disabled,
        ]

        hook.enable_notification_channels(
            filter_=TEST_FILTER,
            project_id=PROJECT_ID,
        )

        notification_channel_disabled.enabled.value = True  # pylint: disable=no-member
        mask = monitoring_v3.types.field_mask_pb2.FieldMask()
        mask.paths.append('enabled')  # pylint: disable=no-member
        mock_channel_client.return_value.update_notification_channel.assert_called_once_with(
            notification_channel=notification_channel_disabled,
            update_mask=mask,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None,
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id',
        return_value=(CREDENTIALS, PROJECT_ID),
    )
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_channel_client')
    def test_stackdriver_disable_notification_channel(
        self, mock_channel_client, mock_get_creds_and_project_id
    ):
        hook = stackdriver.StackdriverHook()
        notification_channel_enabled = ParseDict(
            TEST_NOTIFICATION_CHANNEL_1, monitoring_v3.types.notification_pb2.NotificationChannel()
        )
        notification_channel_disabled = ParseDict(
            TEST_NOTIFICATION_CHANNEL_2, monitoring_v3.types.notification_pb2.NotificationChannel()
        )
        mock_channel_client.return_value.list_notification_channels.return_value = [
            notification_channel_enabled,
            notification_channel_disabled,
        ]

        hook.disable_notification_channels(
            filter_=TEST_FILTER,
            project_id=PROJECT_ID,
        )

        notification_channel_enabled.enabled.value = False  # pylint: disable=no-member
        mask = monitoring_v3.types.field_mask_pb2.FieldMask()
        mask.paths.append('enabled')  # pylint: disable=no-member
        mock_channel_client.return_value.update_notification_channel.assert_called_once_with(
            notification_channel=notification_channel_enabled,
            update_mask=mask,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None,
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id',
        return_value=(CREDENTIALS, PROJECT_ID),
    )
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_channel_client')
    def test_stackdriver_upsert_channel(self, mock_channel_client, mock_get_creds_and_project_id):
        hook = stackdriver.StackdriverHook()
        existing_notification_channel = ParseDict(
            TEST_NOTIFICATION_CHANNEL_1, monitoring_v3.types.notification_pb2.NotificationChannel()
        )
        notification_channel_to_be_created = ParseDict(
            TEST_NOTIFICATION_CHANNEL_2, monitoring_v3.types.notification_pb2.NotificationChannel()
        )
        mock_channel_client.return_value.list_notification_channels.return_value = [
            existing_notification_channel
        ]
        hook.upsert_channel(
            channels=json.dumps({"channels": [TEST_NOTIFICATION_CHANNEL_1, TEST_NOTIFICATION_CHANNEL_2]}),
            project_id=PROJECT_ID,
        )
        mock_channel_client.return_value.list_notification_channels.assert_called_once_with(
            name=f'projects/{PROJECT_ID}',
            filter_=None,
            order_by=None,
            page_size=None,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None,
        )
        mock_channel_client.return_value.update_notification_channel.assert_called_once_with(
            notification_channel=existing_notification_channel, retry=DEFAULT, timeout=DEFAULT, metadata=None
        )
        notification_channel_to_be_created.ClearField('name')
        mock_channel_client.return_value.create_notification_channel.assert_called_once_with(
            name=f'projects/{PROJECT_ID}',
            notification_channel=notification_channel_to_be_created,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None,
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id',
        return_value=(CREDENTIALS, PROJECT_ID),
    )
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_channel_client')
    def test_stackdriver_delete_notification_channel(
        self, mock_channel_client, mock_get_creds_and_project_id
    ):
        hook = stackdriver.StackdriverHook()
        hook.delete_notification_channel(
            name='test-channel',
        )
        mock_channel_client.return_value.delete_notification_channel.assert_called_once_with(
            name='test-channel', retry=DEFAULT, timeout=DEFAULT, metadata=None
        )

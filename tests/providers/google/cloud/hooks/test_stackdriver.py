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

import unittest

import mock
from google.api_core.gapic_v1.method import DEFAULT
from google.cloud import monitoring_v3
from google.protobuf.json_format import Parse

from airflow.providers.google.cloud.hooks import stackdriver

PROJECT_ID = "sd-project"
CREDENTIALS = "sd-credentials"
TEST_FILTER = "filter"
TEST_ALERT_POLICY_ENABLED = """
{
  "combiner": "OR",
  "name": "projects/sd-project/alertPolicies/123",
  "creationRecord": {
    "mutatedBy": "user123",
    "mutateTime": "2020-01-01T00:00:00.000000Z"
  },
  "enabled": true,
  "displayName": "test display",
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
"""

TEST_ALERT_POLICY_DISABLED = """
{
  "combiner": "OR",
  "name": "projects/sd-project/alertPolicies/123",
  "creationRecord": {
    "mutatedBy": "user123",
    "mutateTime": "2020-01-01T00:00:00.000000Z"
  },
  "enabled": false,
  "displayName": "test display",
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
"""


class TestStackdriverHookMethods(unittest.TestCase):
    @mock.patch(
        'airflow.providers.google.cloud.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=(CREDENTIALS, PROJECT_ID)
    )
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_policy_client')
    def test_stackdriver_list_alert_policies(self, mock_policy_client, mock_get_creds_and_project_id):
        method = mock_policy_client.return_value.list_alert_policies
        hook = stackdriver.StackdriverHook()
        hook.list_alert_policies(
            filter_=TEST_FILTER
        )
        method.assert_called_once_with(
            name='projects/{project}'.format(project=PROJECT_ID),
            filter_=TEST_FILTER,
            retry=DEFAULT,
            timeout=DEFAULT,
            order_by=None,
            page_size=None,
            metadata=None
        )

    @mock.patch(
        'airflow.providers.google.cloud.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=(CREDENTIALS, PROJECT_ID)
    )
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_policy_client')
    def test_stackdriver_enable_alert_policy(self, mock_policy_client, mock_get_creds_and_project_id):
        hook = stackdriver.StackdriverHook()
        hook.enable_alert_policies(
            filter_=TEST_FILTER,
        )

        alert_policy1 = Parse(TEST_ALERT_POLICY_ENABLED,
                              monitoring_v3.types.alert_pb2.AlertPolicy())
        alert_policy2 = Parse(TEST_ALERT_POLICY_DISABLED,
                              monitoring_v3.types.alert_pb2.AlertPolicy())

        alert_policies = iter([alert_policy1, alert_policy2])

        mock_policy_client.return_value.list_alert_policies \
            .return_value = alert_policies
        mock_policy_client.return_value.list_alert_policies.assert_called_once_with(
            name='projects/{project}'.format(project=PROJECT_ID),
            filter_=TEST_FILTER,
            retry=DEFAULT,
            timeout=DEFAULT,
            order_by=None,
            page_size=None,
            metadata=None,
        )
        mask = monitoring_v3.types.field_mask_pb2.FieldMask()
        alert_policy2.enabled.value = True
        mask.paths.append('enabled')
        mock_policy_client.return_value.update_alert_policy.assert_called_once_with(
            alert_policy=alert_policy2,
            update_mask=mask,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None,
        )

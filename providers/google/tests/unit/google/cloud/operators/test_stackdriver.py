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
from __future__ import annotations

import pytest
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.monitoring_v3 import AlertPolicy, NotificationChannel

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.hooks import stackdriver as stackdriver_hook
from airflow.providers.google.cloud.links import stackdriver as stackdriver_links
from airflow.providers.google.cloud.operators import cloud_monitoring, stackdriver
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID

TEST_TASK_ID = "test-cloud-monitoring-operator"


@pytest.mark.parametrize(
    ("old_class", "new_class", "extra_kwargs"),
    [
        (
            stackdriver.StackdriverListAlertPoliciesOperator,
            cloud_monitoring.CloudMonitoringListAlertPoliciesOperator,
            {},
        ),
        (
            stackdriver.StackdriverEnableAlertPoliciesOperator,
            cloud_monitoring.CloudMonitoringEnableAlertPoliciesOperator,
            {},
        ),
        (
            stackdriver.StackdriverDisableAlertPoliciesOperator,
            cloud_monitoring.CloudMonitoringDisableAlertPoliciesOperator,
            {},
        ),
        (
            stackdriver.StackdriverUpsertAlertOperator,
            cloud_monitoring.CloudMonitoringUpsertAlertOperator,
            {"alerts": "{}"},
        ),
        (
            stackdriver.StackdriverDeleteAlertOperator,
            cloud_monitoring.CloudMonitoringDeleteAlertOperator,
            {"name": "test-alert"},
        ),
        (
            stackdriver.StackdriverListNotificationChannelsOperator,
            cloud_monitoring.CloudMonitoringListNotificationChannelsOperator,
            {},
        ),
        (
            stackdriver.StackdriverEnableNotificationChannelsOperator,
            cloud_monitoring.CloudMonitoringEnableNotificationChannelsOperator,
            {},
        ),
        (
            stackdriver.StackdriverDisableNotificationChannelsOperator,
            cloud_monitoring.CloudMonitoringDisableNotificationChannelsOperator,
            {},
        ),
        (
            stackdriver.StackdriverUpsertNotificationChannelOperator,
            cloud_monitoring.CloudMonitoringUpsertNotificationChannelOperator,
            {"channels": "{}"},
        ),
        (
            stackdriver.StackdriverDeleteNotificationChannelOperator,
            cloud_monitoring.CloudMonitoringDeleteNotificationChannelOperator,
            {"name": "test-channel"},
        ),
    ],
)
def test_deprecated_operator_warns_and_subclasses_new_operator(old_class, new_class, extra_kwargs):
    with pytest.warns(AirflowProviderDeprecationWarning, match="CloudMonitoring"):
        obj = old_class(task_id=TEST_TASK_ID, **extra_kwargs)
    assert isinstance(obj, new_class)


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("StackdriverHook", stackdriver_hook.StackdriverHook),
        ("StackdriverNotificationsLink", stackdriver_links.StackdriverNotificationsLink),
        ("StackdriverPoliciesLink", stackdriver_links.StackdriverPoliciesLink),
        ("GoogleCloudBaseOperator", GoogleCloudBaseOperator),
        ("PROVIDE_PROJECT_ID", PROVIDE_PROJECT_ID),
        ("AlertPolicy", AlertPolicy),
        ("NotificationChannel", NotificationChannel),
        ("DEFAULT", DEFAULT),
        ("_MethodDefault", _MethodDefault),
    ],
)
def test_names_re_exported_before_the_rename_are_still_importable(name, expected):
    assert getattr(stackdriver, name) is expected

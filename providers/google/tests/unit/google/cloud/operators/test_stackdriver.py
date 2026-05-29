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

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.links.monitoring import (
    CloudMonitoringNotificationsLink,
    CloudMonitoringPoliciesLink,
)
from airflow.providers.google.cloud.links.stackdriver import (
    StackdriverNotificationsLink,
    StackdriverPoliciesLink,
)
from airflow.providers.google.cloud.operators.monitoring import (
    CloudMonitoringDeleteAlertOperator,
    CloudMonitoringDeleteNotificationChannelOperator,
    CloudMonitoringDisableAlertPoliciesOperator,
    CloudMonitoringDisableNotificationChannelsOperator,
    CloudMonitoringEnableAlertPoliciesOperator,
    CloudMonitoringEnableNotificationChannelsOperator,
    CloudMonitoringListAlertPoliciesOperator,
    CloudMonitoringListNotificationChannelsOperator,
    CloudMonitoringUpsertAlertOperator,
    CloudMonitoringUpsertNotificationChannelOperator,
)
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


@pytest.mark.parametrize(
    ("deprecated_class", "replacement_class", "kwargs"),
    [
        (StackdriverListAlertPoliciesOperator, CloudMonitoringListAlertPoliciesOperator, {}),
        (StackdriverEnableAlertPoliciesOperator, CloudMonitoringEnableAlertPoliciesOperator, {}),
        (StackdriverDisableAlertPoliciesOperator, CloudMonitoringDisableAlertPoliciesOperator, {}),
        (StackdriverUpsertAlertOperator, CloudMonitoringUpsertAlertOperator, {"alerts": "{}"}),
        (StackdriverDeleteAlertOperator, CloudMonitoringDeleteAlertOperator, {"name": "policy"}),
        (StackdriverListNotificationChannelsOperator, CloudMonitoringListNotificationChannelsOperator, {}),
        (
            StackdriverEnableNotificationChannelsOperator,
            CloudMonitoringEnableNotificationChannelsOperator,
            {},
        ),
        (
            StackdriverDisableNotificationChannelsOperator,
            CloudMonitoringDisableNotificationChannelsOperator,
            {},
        ),
        (
            StackdriverUpsertNotificationChannelOperator,
            CloudMonitoringUpsertNotificationChannelOperator,
            {"channels": "{}"},
        ),
        (
            StackdriverDeleteNotificationChannelOperator,
            CloudMonitoringDeleteNotificationChannelOperator,
            {"name": "channel"},
        ),
    ],
)
def test_stackdriver_operators_warn_and_wrap_new_monitoring_classes(
    deprecated_class, replacement_class, kwargs
):
    with pytest.warns(AirflowProviderDeprecationWarning, match=replacement_class.__name__):
        operator = deprecated_class(task_id="test-task", **kwargs)

    assert isinstance(operator, replacement_class)


@pytest.mark.parametrize(
    ("deprecated_class", "replacement_class", "expected_key"),
    [
        (StackdriverNotificationsLink, CloudMonitoringNotificationsLink, "stackdriver_notifications"),
        (StackdriverPoliciesLink, CloudMonitoringPoliciesLink, "stackdriver_policies"),
    ],
)
def test_stackdriver_links_warn_and_wrap_new_monitoring_links(
    deprecated_class, replacement_class, expected_key
):
    with pytest.warns(AirflowProviderDeprecationWarning, match=replacement_class.__name__):
        link = deprecated_class()

    assert isinstance(link, replacement_class)
    assert link.key == expected_key

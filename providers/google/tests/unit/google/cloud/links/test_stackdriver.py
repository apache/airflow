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
from airflow.providers.google.cloud.links import cloud_monitoring, stackdriver


@pytest.mark.parametrize(
    ("old_class", "new_class"),
    [
        (stackdriver.StackdriverNotificationsLink, cloud_monitoring.CloudMonitoringNotificationsLink),
        (stackdriver.StackdriverPoliciesLink, cloud_monitoring.CloudMonitoringPoliciesLink),
    ],
)
def test_deprecated_link_warns_and_subclasses_new_link(old_class, new_class):
    with pytest.warns(AirflowProviderDeprecationWarning, match="CloudMonitoring"):
        obj = old_class()
    assert isinstance(obj, new_class)


@pytest.mark.parametrize(
    ("old_constant", "new_constant"),
    [
        (stackdriver.STACKDRIVER_BASE_LINK, cloud_monitoring.CLOUD_MONITORING_BASE_LINK),
        (
            stackdriver.STACKDRIVER_NOTIFICATIONS_LINK,
            cloud_monitoring.CLOUD_MONITORING_NOTIFICATIONS_LINK,
        ),
        (stackdriver.STACKDRIVER_POLICIES_LINK, cloud_monitoring.CLOUD_MONITORING_POLICIES_LINK),
    ],
)
def test_deprecated_link_constants_are_importable(old_constant, new_constant):
    assert old_constant == new_constant

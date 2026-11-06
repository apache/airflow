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

from airflow.providers.google.cloud.links.cloud_monitoring import (
    CloudMonitoringNotificationsLink,
    CloudMonitoringPoliciesLink,
)


@pytest.mark.parametrize(
    ("link_class", "expected_name", "expected_key"),
    [
        (
            CloudMonitoringNotificationsLink,
            "Cloud Monitoring Notifications",
            "stackdriver_notifications",
        ),
        (CloudMonitoringPoliciesLink, "Cloud Monitoring Policies", "stackdriver_policies"),
    ],
)
def test_link_keeps_serialized_identity(link_class, expected_name, expected_key):
    """Serialized Dags and stored XComs key extra links on these, so the rename must not change them."""
    assert link_class.name == expected_name
    assert link_class.key == expected_key

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
"""Deprecated aliases for :mod:`airflow.providers.google.cloud.links.cloud_monitoring`."""

from __future__ import annotations

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.links.cloud_monitoring import (
    CLOUD_MONITORING_BASE_LINK,
    CLOUD_MONITORING_NOTIFICATIONS_LINK,
    CLOUD_MONITORING_POLICIES_LINK,
    CloudMonitoringNotificationsLink,
    CloudMonitoringPoliciesLink,
)
from airflow.providers.google.common.deprecated import deprecated

STACKDRIVER_BASE_LINK = CLOUD_MONITORING_BASE_LINK
STACKDRIVER_NOTIFICATIONS_LINK = CLOUD_MONITORING_NOTIFICATIONS_LINK
STACKDRIVER_POLICIES_LINK = CLOUD_MONITORING_POLICIES_LINK


@deprecated(
    planned_removal_date="March 31, 2027",
    use_instead="airflow.providers.google.cloud.links.cloud_monitoring.CloudMonitoringNotificationsLink",
    category=AirflowProviderDeprecationWarning,
)
class StackdriverNotificationsLink(CloudMonitoringNotificationsLink):
    """Deprecated. Use :class:`CloudMonitoringNotificationsLink`."""


@deprecated(
    planned_removal_date="March 31, 2027",
    use_instead="airflow.providers.google.cloud.links.cloud_monitoring.CloudMonitoringPoliciesLink",
    category=AirflowProviderDeprecationWarning,
)
class StackdriverPoliciesLink(CloudMonitoringPoliciesLink):
    """Deprecated. Use :class:`CloudMonitoringPoliciesLink`."""

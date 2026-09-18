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
"""Deprecated aliases for :mod:`airflow.providers.google.cloud.operators.cloud_monitoring`."""

from __future__ import annotations

from google.api_core.gapic_v1.method import DEFAULT as DEFAULT, _MethodDefault as _MethodDefault
from google.cloud.monitoring_v3 import (
    AlertPolicy as AlertPolicy,
    NotificationChannel as NotificationChannel,
)

from airflow.exceptions import AirflowProviderDeprecationWarning

# Re-exported for backwards compatibility: these names were importable from this
# module before the rename.
from airflow.providers.google.cloud.hooks.stackdriver import StackdriverHook as StackdriverHook
from airflow.providers.google.cloud.links.stackdriver import (
    StackdriverNotificationsLink as StackdriverNotificationsLink,
    StackdriverPoliciesLink as StackdriverPoliciesLink,
)
from airflow.providers.google.cloud.operators.cloud_base import (
    GoogleCloudBaseOperator as GoogleCloudBaseOperator,
)
from airflow.providers.google.cloud.operators.cloud_monitoring import (
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
from airflow.providers.google.common.deprecated import deprecated
from airflow.providers.google.common.hooks.base_google import (
    PROVIDE_PROJECT_ID as PROVIDE_PROJECT_ID,
)


@deprecated(
    planned_removal_date="March 31, 2027",
    use_instead="airflow.providers.google.cloud.operators.cloud_monitoring.CloudMonitoringListAlertPoliciesOperator",
    category=AirflowProviderDeprecationWarning,
)
class StackdriverListAlertPoliciesOperator(CloudMonitoringListAlertPoliciesOperator):
    """Deprecated. Use :class:`CloudMonitoringListAlertPoliciesOperator`."""


@deprecated(
    planned_removal_date="March 31, 2027",
    use_instead="airflow.providers.google.cloud.operators.cloud_monitoring.CloudMonitoringEnableAlertPoliciesOperator",
    category=AirflowProviderDeprecationWarning,
)
class StackdriverEnableAlertPoliciesOperator(CloudMonitoringEnableAlertPoliciesOperator):
    """Deprecated. Use :class:`CloudMonitoringEnableAlertPoliciesOperator`."""


@deprecated(
    planned_removal_date="March 31, 2027",
    use_instead="airflow.providers.google.cloud.operators.cloud_monitoring.CloudMonitoringDisableAlertPoliciesOperator",
    category=AirflowProviderDeprecationWarning,
)
class StackdriverDisableAlertPoliciesOperator(CloudMonitoringDisableAlertPoliciesOperator):
    """Deprecated. Use :class:`CloudMonitoringDisableAlertPoliciesOperator`."""


@deprecated(
    planned_removal_date="March 31, 2027",
    use_instead="airflow.providers.google.cloud.operators.cloud_monitoring.CloudMonitoringUpsertAlertOperator",
    category=AirflowProviderDeprecationWarning,
)
class StackdriverUpsertAlertOperator(CloudMonitoringUpsertAlertOperator):
    """Deprecated. Use :class:`CloudMonitoringUpsertAlertOperator`."""


@deprecated(
    planned_removal_date="March 31, 2027",
    use_instead="airflow.providers.google.cloud.operators.cloud_monitoring.CloudMonitoringDeleteAlertOperator",
    category=AirflowProviderDeprecationWarning,
)
class StackdriverDeleteAlertOperator(CloudMonitoringDeleteAlertOperator):
    """Deprecated. Use :class:`CloudMonitoringDeleteAlertOperator`."""


@deprecated(
    planned_removal_date="March 31, 2027",
    use_instead="airflow.providers.google.cloud.operators.cloud_monitoring.CloudMonitoringListNotificationChannelsOperator",
    category=AirflowProviderDeprecationWarning,
)
class StackdriverListNotificationChannelsOperator(CloudMonitoringListNotificationChannelsOperator):
    """Deprecated. Use :class:`CloudMonitoringListNotificationChannelsOperator`."""


@deprecated(
    planned_removal_date="March 31, 2027",
    use_instead="airflow.providers.google.cloud.operators.cloud_monitoring.CloudMonitoringEnableNotificationChannelsOperator",
    category=AirflowProviderDeprecationWarning,
)
class StackdriverEnableNotificationChannelsOperator(CloudMonitoringEnableNotificationChannelsOperator):
    """Deprecated. Use :class:`CloudMonitoringEnableNotificationChannelsOperator`."""


@deprecated(
    planned_removal_date="March 31, 2027",
    use_instead="airflow.providers.google.cloud.operators.cloud_monitoring.CloudMonitoringDisableNotificationChannelsOperator",
    category=AirflowProviderDeprecationWarning,
)
class StackdriverDisableNotificationChannelsOperator(CloudMonitoringDisableNotificationChannelsOperator):
    """Deprecated. Use :class:`CloudMonitoringDisableNotificationChannelsOperator`."""


@deprecated(
    planned_removal_date="March 31, 2027",
    use_instead="airflow.providers.google.cloud.operators.cloud_monitoring.CloudMonitoringUpsertNotificationChannelOperator",
    category=AirflowProviderDeprecationWarning,
)
class StackdriverUpsertNotificationChannelOperator(CloudMonitoringUpsertNotificationChannelOperator):
    """Deprecated. Use :class:`CloudMonitoringUpsertNotificationChannelOperator`."""


@deprecated(
    planned_removal_date="March 31, 2027",
    use_instead="airflow.providers.google.cloud.operators.cloud_monitoring.CloudMonitoringDeleteNotificationChannelOperator",
    category=AirflowProviderDeprecationWarning,
)
class StackdriverDeleteNotificationChannelOperator(CloudMonitoringDeleteNotificationChannelOperator):
    """Deprecated. Use :class:`CloudMonitoringDeleteNotificationChannelOperator`."""

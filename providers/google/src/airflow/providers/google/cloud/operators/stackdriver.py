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

import warnings

from airflow.exceptions import AirflowProviderDeprecationWarning
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
from airflow.providers.google.common.deprecated import deprecated

STACKDRIVER_DEPRECATION_DATE = "December 01, 2026"
STACKDRIVER_DEPRECATION_REASON = (
    "Stackdriver operator names are deprecated in favor of Cloud Monitoring operator names."
)


class _StackdriverPoliciesLinkMixin:
    @property
    def operator_extra_links(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", AirflowProviderDeprecationWarning)
            return (StackdriverPoliciesLink(),)


class _StackdriverNotificationsLinkMixin:
    @property
    def operator_extra_links(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", AirflowProviderDeprecationWarning)
            return (StackdriverNotificationsLink(),)


@deprecated(
    planned_removal_date=STACKDRIVER_DEPRECATION_DATE,
    use_instead="airflow.providers.google.cloud.operators.monitoring.CloudMonitoringListAlertPoliciesOperator",
    reason=STACKDRIVER_DEPRECATION_REASON,
    category=AirflowProviderDeprecationWarning,
)
class StackdriverListAlertPoliciesOperator(
    _StackdriverPoliciesLinkMixin, CloudMonitoringListAlertPoliciesOperator
):
    """Deprecated wrapper for :class:`CloudMonitoringListAlertPoliciesOperator`."""


@deprecated(
    planned_removal_date=STACKDRIVER_DEPRECATION_DATE,
    use_instead="airflow.providers.google.cloud.operators.monitoring.CloudMonitoringEnableAlertPoliciesOperator",
    reason=STACKDRIVER_DEPRECATION_REASON,
    category=AirflowProviderDeprecationWarning,
)
class StackdriverEnableAlertPoliciesOperator(
    _StackdriverPoliciesLinkMixin, CloudMonitoringEnableAlertPoliciesOperator
):
    """Deprecated wrapper for :class:`CloudMonitoringEnableAlertPoliciesOperator`."""


@deprecated(
    planned_removal_date=STACKDRIVER_DEPRECATION_DATE,
    use_instead="airflow.providers.google.cloud.operators.monitoring.CloudMonitoringDisableAlertPoliciesOperator",
    reason=STACKDRIVER_DEPRECATION_REASON,
    category=AirflowProviderDeprecationWarning,
)
class StackdriverDisableAlertPoliciesOperator(
    _StackdriverPoliciesLinkMixin, CloudMonitoringDisableAlertPoliciesOperator
):
    """Deprecated wrapper for :class:`CloudMonitoringDisableAlertPoliciesOperator`."""


@deprecated(
    planned_removal_date=STACKDRIVER_DEPRECATION_DATE,
    use_instead="airflow.providers.google.cloud.operators.monitoring.CloudMonitoringUpsertAlertOperator",
    reason=STACKDRIVER_DEPRECATION_REASON,
    category=AirflowProviderDeprecationWarning,
)
class StackdriverUpsertAlertOperator(_StackdriverPoliciesLinkMixin, CloudMonitoringUpsertAlertOperator):
    """Deprecated wrapper for :class:`CloudMonitoringUpsertAlertOperator`."""


@deprecated(
    planned_removal_date=STACKDRIVER_DEPRECATION_DATE,
    use_instead="airflow.providers.google.cloud.operators.monitoring.CloudMonitoringDeleteAlertOperator",
    reason=STACKDRIVER_DEPRECATION_REASON,
    category=AirflowProviderDeprecationWarning,
)
class StackdriverDeleteAlertOperator(CloudMonitoringDeleteAlertOperator):
    """Deprecated wrapper for :class:`CloudMonitoringDeleteAlertOperator`."""


@deprecated(
    planned_removal_date=STACKDRIVER_DEPRECATION_DATE,
    use_instead="airflow.providers.google.cloud.operators.monitoring.CloudMonitoringListNotificationChannelsOperator",
    reason=STACKDRIVER_DEPRECATION_REASON,
    category=AirflowProviderDeprecationWarning,
)
class StackdriverListNotificationChannelsOperator(
    _StackdriverNotificationsLinkMixin, CloudMonitoringListNotificationChannelsOperator
):
    """Deprecated wrapper for :class:`CloudMonitoringListNotificationChannelsOperator`."""


@deprecated(
    planned_removal_date=STACKDRIVER_DEPRECATION_DATE,
    use_instead="airflow.providers.google.cloud.operators.monitoring.CloudMonitoringEnableNotificationChannelsOperator",
    reason=STACKDRIVER_DEPRECATION_REASON,
    category=AirflowProviderDeprecationWarning,
)
class StackdriverEnableNotificationChannelsOperator(
    _StackdriverNotificationsLinkMixin, CloudMonitoringEnableNotificationChannelsOperator
):
    """Deprecated wrapper for :class:`CloudMonitoringEnableNotificationChannelsOperator`."""


@deprecated(
    planned_removal_date=STACKDRIVER_DEPRECATION_DATE,
    use_instead="airflow.providers.google.cloud.operators.monitoring.CloudMonitoringDisableNotificationChannelsOperator",
    reason=STACKDRIVER_DEPRECATION_REASON,
    category=AirflowProviderDeprecationWarning,
)
class StackdriverDisableNotificationChannelsOperator(
    _StackdriverNotificationsLinkMixin, CloudMonitoringDisableNotificationChannelsOperator
):
    """Deprecated wrapper for :class:`CloudMonitoringDisableNotificationChannelsOperator`."""


@deprecated(
    planned_removal_date=STACKDRIVER_DEPRECATION_DATE,
    use_instead="airflow.providers.google.cloud.operators.monitoring.CloudMonitoringUpsertNotificationChannelOperator",
    reason=STACKDRIVER_DEPRECATION_REASON,
    category=AirflowProviderDeprecationWarning,
)
class StackdriverUpsertNotificationChannelOperator(
    _StackdriverNotificationsLinkMixin, CloudMonitoringUpsertNotificationChannelOperator
):
    """Deprecated wrapper for :class:`CloudMonitoringUpsertNotificationChannelOperator`."""


@deprecated(
    planned_removal_date=STACKDRIVER_DEPRECATION_DATE,
    use_instead="airflow.providers.google.cloud.operators.monitoring.CloudMonitoringDeleteNotificationChannelOperator",
    reason=STACKDRIVER_DEPRECATION_REASON,
    category=AirflowProviderDeprecationWarning,
)
class StackdriverDeleteNotificationChannelOperator(CloudMonitoringDeleteNotificationChannelOperator):
    """Deprecated wrapper for :class:`CloudMonitoringDeleteNotificationChannelOperator`."""

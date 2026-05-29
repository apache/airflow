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
"""Deprecated Google Stackdriver logging wrappers."""

from __future__ import annotations

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.log.cloud_logging_task_handler import (
    CloudLoggingRemoteLogIO,
    CloudLoggingTaskHandler,
)
from airflow.providers.google.common.deprecated import deprecated

STACKDRIVER_DEPRECATION_DATE = "December 01, 2026"


@deprecated(
    planned_removal_date=STACKDRIVER_DEPRECATION_DATE,
    use_instead="airflow.providers.google.cloud.log.cloud_logging_task_handler.CloudLoggingRemoteLogIO",
    reason="Stackdriver log IO names are deprecated in favor of Cloud Logging names.",
    category=AirflowProviderDeprecationWarning,
)
class StackdriverRemoteLogIO(CloudLoggingRemoteLogIO):
    """Deprecated wrapper for :class:`CloudLoggingRemoteLogIO`."""


@deprecated(
    planned_removal_date=STACKDRIVER_DEPRECATION_DATE,
    use_instead="airflow.providers.google.cloud.log.cloud_logging_task_handler.CloudLoggingTaskHandler",
    reason="Stackdriver task handler names are deprecated in favor of Cloud Logging names.",
    category=AirflowProviderDeprecationWarning,
)
class StackdriverTaskHandler(CloudLoggingTaskHandler):
    """Deprecated wrapper for :class:`CloudLoggingTaskHandler`."""

    LOG_NAME = "Google Stackdriver"

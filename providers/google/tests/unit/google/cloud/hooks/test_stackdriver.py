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

from unittest import mock

import pytest

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models import Connection
from airflow.providers.google.cloud.hooks.cloud_monitoring import CloudMonitoringHook
from airflow.providers.google.cloud.hooks.stackdriver import StackdriverHook


@mock.patch(
    "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_connection",
    return_value=Connection(conn_id="google_cloud_default"),
)
def test_deprecated_hook_warns_and_subclasses_new_hook(mock_get_connection):
    with pytest.warns(AirflowProviderDeprecationWarning, match="CloudMonitoring"):
        hook = StackdriverHook()
    assert isinstance(hook, CloudMonitoringHook)

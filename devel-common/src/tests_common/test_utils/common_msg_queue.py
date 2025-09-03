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

from tests_common.test_utils.version_compat import get_base_airflow_version_tuple


@pytest.fixture
def collect_queue_param_deprecation_warning():
    """Collect deprecation warnings for queue parameter."""
    with pytest.warns(
        AirflowProviderDeprecationWarning,
        match="The `queue` parameter is deprecated and will be removed in future versions. Use the `scheme` parameter instead and pass configuration as keyword arguments to `MessageQueueTrigger`.",
    ):
        yield


mark_common_msg_queue_test = pytest.mark.skipif(
    get_base_airflow_version_tuple() < (3, 0, 1), reason="CommonMessageQueueTrigger Requires Airflow 3.0.1+"
)

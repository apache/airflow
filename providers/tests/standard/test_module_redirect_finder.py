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

import importlib

import pytest

from tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Valid for Airflow 3.0+ only")
class TestModuleRedirectFinder:
    @pytest.mark.parametrize("old_module, new_module", [
        ("airflow.operators.python", "airflow.providers.standard.operators.python"),
        ("airflow.operators.bash", "airflow.providers.standard.operators.bash"),
        ("airflow.operators.datetime", "airflow.providers.standard.operators.datetime"),
        ("airflow.operators.weekday", "airflow.providers.standard.operators.weekday"),
        ("airflow.sensors.bash", "airflow.providers.standard.sensors.bash"),
        ("airflow.sensors.date_time", "airflow.providers.standard.sensors.date_time"),
        ("airflow.sensors.python", "airflow.providers.standard.sensors.python"),
        ("airflow.sensors.time", "airflow.providers.standard.sensors.time"),
        ("airflow.sensors.time_delta", "airflow.providers.standard.sensors.time_delta"),
        ("airflow.sensors.weekday", "airflow.providers.standard.sensors.weekday"),
        ("airflow.hooks.filesystem", "airflow.providers.standard.hooks.filesystem"),
        ("airflow.hooks.package_index", "airflow.providers.standard.hooks.package_index"),
        ("airflow.hooks.subprocess", "airflow.providers.standard.hooks.subprocess"),
        ("airflow.utils.python_virtualenv", "airflow.providers.standard.utils.python_virtualenv"),
    ])
    def test_module_redirect_finder_for_old_location(self, old_module, new_module):
        try:
            assert importlib.import_module(old_module).__name__ == new_module
        except ImportError:
            pytest.fail(f"Failed to import module, incorrect reference: {old_module} -> {new_module}")

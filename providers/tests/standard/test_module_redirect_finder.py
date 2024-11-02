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

pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Valid for Airflow 3.0+ only")


class TestModuleRedirectFinder:
    def test_module_redirect_finder_for_old_location(self):
        assert (
            importlib.import_module("airflow.operators.python").__name__
            == "airflow.providers.standard.operators.python"
        )

        assert (
            importlib.import_module("airflow.operators.bash").__name__
            == "airflow.providers.standard.operators.bash"
        )

        assert (
            importlib.import_module("airflow.operators.datetime").__name__
            == "airflow.providers.standard.operators.datetime"
        )

        assert (
            importlib.import_module("airflow.operators.weekday").__name__
            == "airflow.providers.standard.operators.weekday"
        )

        assert (
            importlib.import_module("airflow.sensors.bash").__name__
            == "airflow.providers.standard.sensors.bash"
        )

        assert (
            importlib.import_module("airflow.sensors.date_time").__name__
            == "airflow.providers.standard.sensors.date_time"
        )

        assert (
            importlib.import_module("airflow.sensors.python").__name__
            == "airflow.providers.standard.sensors.python"
        )

        assert (
            importlib.import_module("airflow.sensors.time").__name__
            == "airflow.providers.standard.sensors.time"
        )

        assert (
            importlib.import_module("airflow.sensors.time_delta").__name__
            == "airflow.providers.standard.sensors.time_delta"
        )

        assert (
            importlib.import_module("airflow.sensors.weekday").__name__
            == "airflow.providers.standard.sensors.weekday"
        )

        assert (
            importlib.import_module("airflow.hooks.filesystem").__name__
            == "airflow.providers.standard.hooks.filesystem"
        )

        assert (
            importlib.import_module("airflow.hooks.package_index").__name__
            == "airflow.providers.standard.hooks.package_index"
        )

        assert (
            importlib.import_module("airflow.hooks.subprocess").__name__
            == "airflow.providers.standard.hooks.subprocess"
        )

        assert (
            importlib.import_module("airflow.utils.python_virtualenv").__name__
            == "airflow.providers.standard.utils.python_virtualenv"
        )

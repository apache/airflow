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
from openfeature import api

from airflow.providers.common.compat.sdk import AirflowNotFoundException
from airflow.providers.openfeature.hooks import openfeature as openfeature_module
from airflow.providers.openfeature.hooks.openfeature import OpenFeatureHook
from airflow.providers.openfeature.providers.fractional import BoolFlag, FractionalProvider
from airflow.providers.openfeature.sensors.feature_flag import FeatureFlagSensor


@pytest.fixture(autouse=True)
def _no_connection(monkeypatch):
    openfeature_module._REGISTERED_CONNECTIONS.clear()
    monkeypatch.setattr(
        OpenFeatureHook, "get_connection", mock.Mock(side_effect=AirflowNotFoundException("missing"))
    )
    yield
    openfeature_module._REGISTERED_CONNECTIONS.clear()


class TestFeatureFlagSensor:
    def test_poke_true_when_flag_enabled(self):
        api.set_provider(FractionalProvider(bool_flags={"f": BoolFlag(100)}))
        sensor = FeatureFlagSensor(task_id="s", flag_key="f", entity="e")
        assert sensor.poke({}) is True

    def test_poke_false_when_flag_disabled(self):
        api.set_provider(FractionalProvider(bool_flags={"f": BoolFlag(0)}))
        sensor = FeatureFlagSensor(task_id="s", flag_key="f", entity="e")
        assert sensor.poke({}) is False

    def test_poke_respects_expected_false(self):
        api.set_provider(FractionalProvider(bool_flags={"f": BoolFlag(0)}))
        sensor = FeatureFlagSensor(task_id="s", flag_key="f", entity="e", expected=False)
        assert sensor.poke({}) is True

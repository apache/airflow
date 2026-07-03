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

from datetime import datetime

import pytest

from airflow.api_fastapi.core_api.datamodels.trigger import TriggerResponse


class _Trigger:
    """Stand-in for the ``Trigger`` ORM object a ``TriggerResponse`` is built from."""

    id = 1
    classpath = "airflow.providers.standard.triggers.temporal.DateTimeTrigger"
    created_date = datetime(2024, 1, 1)
    queue = None
    triggerer_id = None

    def __init__(self, kwargs):
        self.kwargs = kwargs


class TestTriggerResponse:
    @pytest.mark.parametrize(
        "kwargs",
        [
            pytest.param({"api_key": "super-secret", "polling_interval": 30}, id="sensitive-values"),
            pytest.param({}, id="already-empty"),
        ],
    )
    def test_kwargs_are_always_empty(self, kwargs):
        """Trigger kwargs may hold credentials, so the API always returns them empty as ``"{}"``."""
        response = TriggerResponse.model_validate(_Trigger(kwargs), from_attributes=True)

        # Read through the serialized output (the API representation) rather than the attribute,
        # which would emit the field's deprecation warning.
        dumped = response.model_dump()
        assert dumped["kwargs"] == "{}"
        # The schema must remain a string for backwards compatibility.
        assert isinstance(dumped["kwargs"], str)

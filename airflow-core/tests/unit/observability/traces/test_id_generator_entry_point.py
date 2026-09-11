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

from importlib.metadata import entry_points

import pytest
from opentelemetry.sdk.trace.id_generator import IdGenerator

from airflow._shared.observability.traces import OverrideableRandomIdGenerator


class TestIdGeneratorEntryPoint:
    """
    ``OTEL_PYTHON_ID_GENERATOR=airflow`` must resolve without importing a private module.

    The OpenTelemetry SDK configurator looks the value up in the ``opentelemetry_id_generator``
    entry point group and rejects anything that is not an ``IdGenerator`` subclass.
    """

    @pytest.fixture
    def entry_point(self):
        return next(iter(entry_points(group="opentelemetry_id_generator", name="airflow")))

    def test_resolves_to_airflows_id_generator(self, entry_point):
        assert entry_point.load() is OverrideableRandomIdGenerator

    def test_is_accepted_by_the_sdk_configurator(self, entry_point):
        assert issubclass(entry_point.load(), IdGenerator)

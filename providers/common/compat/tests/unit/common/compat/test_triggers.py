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

import airflow.triggers.base
from airflow.providers.common.compat import triggers


class TestBaseEventTrigger:
    def test_falls_back_to_base_trigger_without_base_event_trigger(self, monkeypatch):
        """On Airflow 2.x, where the class does not exist, the alias resolves to BaseTrigger."""
        # Already absent when the tests run against Airflow 2.x, which is the case being simulated.
        monkeypatch.delattr(airflow.triggers.base, "BaseEventTrigger", raising=False)

        assert triggers.BaseEventTrigger is airflow.triggers.base.BaseTrigger

    def test_unknown_attribute_raises(self):
        with pytest.raises(AttributeError, match="module has no attribute 'NotATrigger'"):
            triggers.NotATrigger

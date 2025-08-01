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

import pytest

from airflow.utils.trigger_rule import TriggerRule


class TestTriggerRule:
    def test_valid_trigger_rules(self):
        assert TriggerRule.is_valid(TriggerRule.ALL_SUCCESS)
        assert TriggerRule.is_valid(TriggerRule.ALL_FAILED)
        assert TriggerRule.is_valid(TriggerRule.ALL_DONE)
        assert TriggerRule.is_valid(TriggerRule.ALL_SKIPPED)
        assert TriggerRule.is_valid(TriggerRule.ONE_SUCCESS)
        assert TriggerRule.is_valid(TriggerRule.ONE_FAILED)
        assert TriggerRule.is_valid(TriggerRule.ONE_DONE)
        assert TriggerRule.is_valid(TriggerRule.NONE_FAILED)
        assert TriggerRule.is_valid(TriggerRule.NONE_SKIPPED)
        assert TriggerRule.is_valid(TriggerRule.ALWAYS)
        assert TriggerRule.is_valid(TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
        assert TriggerRule.is_valid(TriggerRule.ALL_DONE_SETUP_SUCCESS)
        assert TriggerRule.is_valid(TriggerRule.ALL_DONE_MIN_ONE_SUCCESS)
        assert len(TriggerRule.all_triggers()) == 13

        with pytest.raises(ValueError):
            TriggerRule("NOT_EXIST_TRIGGER_RULE")

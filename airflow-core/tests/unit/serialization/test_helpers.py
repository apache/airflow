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

from airflow.sdk.definitions._internal.types import SET_DURING_EXECUTION
from airflow.serialization.definitions.notset import NOTSET
from airflow.serialization.helpers import serialize_template_field


def test_serialize_template_field_with_very_small_max_length(monkeypatch):
    """Test that truncation message is prioritized even for very small max_length."""
    monkeypatch.setenv("AIRFLOW__CORE__MAX_TEMPLATED_FIELD_LENGTH", "1")

    result = serialize_template_field("This is a long string", "test")

    # The truncation message should be shown even if it exceeds max_length
    # This ensures users always see why content is truncated
    assert result
    assert "Truncated. You can change this behaviour" in result


def test_serialize_template_field_with_notset():
    """NOTSET must serialize deterministically via serialize(), not str() fallback."""
    result = serialize_template_field(NOTSET, "logical_date")
    assert result == "NOTSET"


def test_serialize_template_field_with_set_during_execution():
    """SetDuringExecution must use its own serialize() override."""
    result = serialize_template_field(SET_DURING_EXECUTION, "logical_date")
    assert result == "DYNAMIC (set during execution)"


def test_argnotset_repr_and_str():
    """repr/str should return the stable serialized sentinel string."""
    assert repr(NOTSET) == "NOTSET"
    assert str(NOTSET) == "NOTSET"
    assert repr(SET_DURING_EXECUTION) == "DYNAMIC (set during execution)"
    assert str(SET_DURING_EXECUTION) == "DYNAMIC (set during execution)"

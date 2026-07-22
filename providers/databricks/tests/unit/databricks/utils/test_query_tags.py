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

from airflow.providers.databricks.utils.query_tags import (
    build_query_tags,
    dict_to_query_tag_list,
    get_airflow_query_tags,
)


def _make_context():
    ti = mock.MagicMock(spec=["dag_id", "task_id", "run_id", "try_number", "map_index"])
    ti.dag_id = "test_dag"
    ti.task_id = "test_task"
    ti.run_id = "test_run"
    ti.try_number = 1
    ti.map_index = -1
    return {"ti": ti}


class TestGetAirflowQueryTags:
    def test_returns_empty_dict_without_task_instance(self):
        assert get_airflow_query_tags({}) == {}

    def test_returns_stringified_context_metadata(self):
        result = get_airflow_query_tags(_make_context())
        assert result == {
            "airflow_dag_id": "test_dag",
            "airflow_task_id": "test_task",
            "airflow_run_id": "test_run",
            "airflow_try_number": "1",
            "airflow_map_index": "-1",
        }

    def test_none_valued_attributes_become_none(self):
        ti = mock.MagicMock(spec=["dag_id", "task_id", "run_id", "try_number", "map_index"])
        ti.dag_id = "test_dag"
        ti.task_id = None
        ti.run_id = None
        ti.try_number = None
        ti.map_index = None
        result = get_airflow_query_tags({"ti": ti})
        assert result["airflow_dag_id"] == "test_dag"
        assert result["airflow_task_id"] is None
        assert result["airflow_run_id"] is None


class TestBuildQueryTags:
    def test_none_context_returns_user_tags_only(self):
        assert build_query_tags(None, {"custom": "value"}, include_airflow_query_tags=True) == {
            "custom": "value"
        }

    def test_none_context_and_no_user_tags_returns_none(self):
        assert build_query_tags(None, {}, include_airflow_query_tags=True) is None

    def test_disabled_airflow_tags_returns_user_tags_only(self):
        result = build_query_tags(_make_context(), {"custom": "value"}, include_airflow_query_tags=False)
        assert result == {"custom": "value"}

    def test_disabled_airflow_tags_and_no_user_tags_returns_none(self):
        assert build_query_tags(_make_context(), {}, include_airflow_query_tags=False) is None

    def test_merges_airflow_and_user_tags(self):
        result = build_query_tags(_make_context(), {"custom": "value"}, include_airflow_query_tags=True)
        assert result is not None
        assert result["airflow_dag_id"] == "test_dag"
        assert result["custom"] == "value"

    def test_user_tags_override_airflow_tags_on_collision(self):
        result = build_query_tags(
            _make_context(), {"airflow_dag_id": "overridden"}, include_airflow_query_tags=True
        )
        assert result is not None
        assert result["airflow_dag_id"] == "overridden"


class TestDictToQueryTagList:
    def test_converts_dict_to_key_value_list(self):
        assert dict_to_query_tag_list({"a": "1", "b": "2"}) == [
            {"key": "a", "value": "1"},
            {"key": "b", "value": "2"},
        ]

    def test_omits_none_values(self):
        assert dict_to_query_tag_list({"a": "1", "b": None}) == [{"key": "a", "value": "1"}]

    def test_empty_dict_returns_empty_list(self):
        assert dict_to_query_tag_list({}) == []

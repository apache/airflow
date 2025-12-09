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

from typing import Any

import pytest

from airflow._shared.observability.metrics import dual_stats_manager


class TestDualStatsManager:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            pytest.param(
                1,
                True,
                id="number",
            ),
            pytest.param(
                False,
                True,
                id="boolean",
            ),
            pytest.param(
                None,
                False,
                id="None",
            ),
            pytest.param(
                {},
                False,
                id="empty_dict",
            ),
        ],
    )
    def test_value_is_provided(self, value: Any, expected: bool):
        result = dual_stats_manager._value_is_provided(value)
        assert result == expected

    @pytest.mark.parametrize(
        ("count", "rate", "delta", "tags", "expected_args_dict"),
        [
            pytest.param(
                1,
                1,
                False,
                {},
                {"count": 1, "rate": 1, "delta": False},
                id="all_params_empty_tags",
            ),
            pytest.param(
                1,
                1,
                False,
                {"test": True},
                {"count": 1, "rate": 1, "delta": False, "tags": {"test": True}},
                id="provide_tags",
            ),
            pytest.param(
                None,
                1,
                False,
                {},
                {"rate": 1, "delta": False},
                id="no_count",
            ),
            pytest.param(
                1,
                None,
                False,
                {},
                {"count": 1, "delta": False},
                id="no_rate",
            ),
            pytest.param(
                1,
                1,
                None,
                {},
                {"count": 1, "rate": 1},
                id="no_delta",
            ),
            pytest.param(
                1,
                1,
                False,
                None,
                {"count": 1, "rate": 1, "delta": False},
                id="no_tags",
            ),
        ],
    )
    def test_get_dict_with_defined_args(
        self,
        count: int | None,
        rate: int | None,
        delta: bool | None,
        tags: dict[str, Any] | None,
        expected_args_dict: dict[str, Any],
    ):
        args_dict = dual_stats_manager._get_dict_with_defined_args(count, rate, delta, tags)
        assert sorted(args_dict) == sorted(expected_args_dict)

    @pytest.mark.parametrize(
        ("args_dict", "tags", "extra_tags", "expected_args_dict"),
        [
            pytest.param(
                {"count": 1},
                {"test": True},
                {},
                {"count": 1, "tags": {"test": True}},
                id="no_extra_tags",
            ),
            pytest.param(
                {},
                {"test": True},
                {},
                {"tags": {"test": True}},
                id="no_args_no_extra_but_tags",
            ),
            pytest.param(
                {},
                {"test": True},
                {"test_extra": True},
                {"tags": {"test": True, "test_extra": True}},
                id="no_args_but_tags_and_extra",
            ),
            pytest.param(
                {"count": 1},
                {"test": True},
                {},
                {"count": 1, "tags": {"test": True}},
                id="no_args_no_tags_but_extra_tags",
            ),
            pytest.param(
                {"count": 1},
                {"test": True},
                {"test_extra": True},
                {"count": 1, "tags": {"test": True, "test_extra": True}},
                id="all_params_provided",
            ),
            pytest.param(
                {"count": 1, "rate": 3},
                {"test1": True, "test2": False},
                {"test_extra1": True, "test_extra2": False, "test_extra3": True},
                {
                    "count": 1,
                    "rate": 3,
                    "tags": {
                        "test1": True,
                        "test2": False,
                        "test_extra1": True,
                        "test_extra2": False,
                        "test_extra3": True,
                    },
                },
                id="multiple_params",
            ),
        ],
    )
    def test_get_args_dict_with_extra_tags_if_set(
        self,
        args_dict: dict[str, Any] | None,
        tags: dict[str, Any] | None,
        extra_tags: dict[str, Any] | None,
        expected_args_dict: dict[str, Any],
    ):
        dict_full = dual_stats_manager._get_args_dict_with_extra_tags_if_set(args_dict, tags, extra_tags)
        assert sorted(dict_full) == sorted(expected_args_dict)

    @pytest.mark.parametrize(
        ("tags", "extra_tags", "expected_tags_dict"),
        [
            pytest.param(
                {"test": True},
                {"test_extra": True},
                {"test": True, "test_extra": True},
                id="all_params_provided",
            ),
            pytest.param(
                {},
                {},
                {},
                id="no_params_provided",
            ),
            pytest.param(
                {"test": True},
                {},
                {"test": True},
                id="only_tags",
            ),
            pytest.param(
                {},
                {"test_extra": True},
                {"test_extra": True},
                id="only_extra",
            ),
            pytest.param(
                {"test1": True, "test2": False},
                {"test_extra1": True, "test_extra2": False, "test_extra3": True},
                {
                    "test1": True,
                    "test2": False,
                    "test_extra1": True,
                    "test_extra2": False,
                    "test_extra3": True,
                },
                id="multiple_params",
            ),
        ],
    )
    def test_get_tags_with_extra(
        self,
        tags: dict[str, Any] | None,
        extra_tags: dict[str, Any] | None,
        expected_tags_dict: dict[str, Any],
    ):
        tags_full = dual_stats_manager._get_tags_with_extra(tags, extra_tags)
        assert sorted(tags_full) == sorted(expected_tags_dict)

    @pytest.mark.parametrize(
        ("stat", "variables", "expected_legacy_stat", "raises_value_error", "expected_error_msg"),
        [
            pytest.param(
                "operator_failures",
                {"operator_name": "exec1"},
                "operator_failures_exec1",
                False,
                "",
                id="no_errors",
            ),
            pytest.param(
                "operator_failures",
                {},
                "operator_failures_exec1",
                True,
                "Missing required variables for metric",
                id="missing_params",
            ),
            pytest.param(
                "missing_metric",
                {},
                "",
                True,
                "Add the metric to the YAML file before using it.",
                id="missing_metric",
            ),
        ],
    )
    def test_get_legacy_stat_from_registry(
        self,
        stat: str,
        variables: dict[str, Any],
        expected_legacy_stat: str,
        raises_value_error: bool,
        expected_error_msg: str,
    ):
        from airflow._shared.observability.metrics.dual_stats_manager import DualStatsManager

        manager = DualStatsManager()

        if raises_value_error:
            with pytest.raises(
                ValueError,
                match=expected_error_msg,
            ):
                manager.get_legacy_stat(stat, variables)
        else:
            legacy_stat = manager.get_legacy_stat(stat, variables)
            assert legacy_stat == expected_legacy_stat

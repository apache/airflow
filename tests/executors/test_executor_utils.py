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

from airflow.executors.executor_loader import ExecutorName


class TestExecutorName:
    def test_executor_name_repr_alias(self):
        executor_name = ExecutorName(module_path="foo.bar", alias="my_alias")
        assert str(executor_name) == "my_alias:foo.bar"

    def test_executor_name_repr_no_alias(self):
        executor_name = ExecutorName(module_path="foo.bar")
        assert str(executor_name) == "foo.bar"

    @pytest.mark.parametrize(
        ("name_args_1", "name_args_2", "expected_result"),
        [
            (["foo.bar", "my_alias"], ["foo.bar", "my_alias"], True),
            (["foo.bar"], ["foo.bar"], True),
            (["foo.bar"], ["foo.bar", "my_alias"], False),
        ],
    )
    def test_executor_name_eq(self, name_args_1, name_args_2, expected_result):
        executor_name_1 = ExecutorName(*name_args_1)
        executor_name_2 = ExecutorName(*name_args_2)
        eq_result = executor_name_1 == executor_name_2
        assert eq_result == expected_result

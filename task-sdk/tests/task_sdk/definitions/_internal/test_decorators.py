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

from textwrap import dedent

import pytest

from airflow.sdk.definitions._internal.decorators import remove_task_decorator


class TestExternalPythonDecorator:
    @pytest.mark.parametrize(
        ("decorators", "expected_decorators"),
        [
            (["@task.external_python"], []),
            (["@task.external_python()"], []),
            (['@task.external_python(serializer="dill")'], []),
            (["@foo", "@task.external_python", "@bar"], ["@foo", "@bar"]),
            (["@foo", "@task.external_python()", "@bar"], ["@foo", "@bar"]),
        ],
        ids=["without_parens", "parens", "with_args", "nested_without_parens", "nested_with_parens"],
    )
    def test_remove_task_decorator(self, decorators: list[str], expected_decorators: list[str]):
        concated_decorators = "\n".join(decorators)
        expected_decorator = "\n".join(expected_decorators)
        SCRIPT = dedent(
            """
        def f():
            import funcsigs
        """
        )
        py_source = concated_decorators + SCRIPT
        expected_source = expected_decorator + SCRIPT if expected_decorator else SCRIPT.lstrip()

        res = remove_task_decorator(python_source=py_source, task_decorator_name="@task.external_python")
        assert res == expected_source

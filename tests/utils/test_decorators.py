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

from typing import TYPE_CHECKING

from airflow.decorators import task

if TYPE_CHECKING:
    from airflow.decorators.base import Task


class TestDecoratorSource:
    def parse_python_source(self, task: Task) -> str:
        return task().operator.get_python_source()

    def test_branch_external_python(self):
        @task.branch_virtualenv()
        def f():
            return ["some_task"]

        assert self.parse_python_source(f) == 'def f():\n    return ["some_task"]\n'

    def test_branch_virtualenv(self):
        @task.external_python(python="python3")
        def f():
            return "hello world"

        assert self.parse_python_source(f) == 'def f():\n    return "hello world"\n'

    def test_virtualenv(self):
        @task.virtualenv()
        def f():
            return "hello world"

        assert self.parse_python_source(f) == 'def f():\n    return "hello world"\n'

    def test_skip_if(self):
        @task.skip_if(lambda context: True)
        @task.virtualenv()
        def f():
            return "hello world"

        assert self.parse_python_source(f) == 'def f():\n    return "hello world"\n'

    def test_run_if(self):
        @task.run_if(lambda context: True)
        @task.virtualenv()
        def f():
            return "hello world"

        assert self.parse_python_source(f) == 'def f():\n    return "hello world"\n'

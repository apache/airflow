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

import unittest

from airflow.utils.decorators import remove_task_decorator


class TestExternalPythonDecorator(unittest.TestCase):
    def test_remove_task_decorator(self):
        py_source = "@task.external_python(use_dill=True)\ndef f():\nimport funcsigs"
        res = remove_task_decorator(python_source=py_source, task_decorator_name="@task.external_python")
        assert res == "def f():\nimport funcsigs"

    def test_remove_decorator_no_parens(self):

        py_source = "@task.external_python\ndef f():\nimport funcsigs"
        res = remove_task_decorator(python_source=py_source, task_decorator_name="@task.external_python")
        assert res == "def f():\nimport funcsigs"

    def test_remove_decorator_nested(self):

        py_source = "@foo\n@task.external_python\n@bar\ndef f():\nimport funcsigs"
        res = remove_task_decorator(python_source=py_source, task_decorator_name="@task.external_python")
        assert res == "@foo\n@bar\ndef f():\nimport funcsigs"

        py_source = "@foo\n@task.external_python()\n@bar\ndef f():\nimport funcsigs"
        res = remove_task_decorator(python_source=py_source, task_decorator_name="@task.external_python")
        assert res == "@foo\n@bar\ndef f():\nimport funcsigs"

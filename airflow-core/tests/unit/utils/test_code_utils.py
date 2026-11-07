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

import functools

from airflow.utils.code_utils import get_python_source


def _sample_function():
    return 1


class _CallableObject:
    def __call__(self):
        return 2


class TestGetPythonSource:
    def test_partial_of_function(self):
        assert "def _sample_function" in get_python_source(functools.partial(_sample_function))

    def test_partial_of_builtin_does_not_raise(self):
        assert "No source code available" in get_python_source(functools.partial(len))

    def test_partial_of_callable_object(self):
        assert "def __call__" in get_python_source(functools.partial(_CallableObject()))

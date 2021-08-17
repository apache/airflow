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

import jinja2
import jinja2.exceptions
import pytest

import airflow.templates


@pytest.fixture
def env():
    return airflow.templates.SandboxedEnvironment(undefined=jinja2.StrictUndefined, cache_size=0)


def test_protected_access(env):
    class Test:
        _protected = 123

    assert env.from_string(r'{{ obj._protected }}').render(obj=Test) == "123"


def test_private_access(env):
    with pytest.raises(jinja2.exceptions.SecurityError):
        env.from_string(r'{{ func.__code__ }}').render(func=test_private_access)

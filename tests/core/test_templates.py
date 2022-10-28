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

import jinja2
import jinja2.exceptions
import pendulum
import pytest

import airflow.templates


@pytest.fixture
def env():
    return airflow.templates.SandboxedEnvironment(undefined=jinja2.StrictUndefined, cache_size=0)


def test_protected_access(env):
    class Test:
        _protected = 123

    assert env.from_string(r"{{ obj._protected }}").render(obj=Test) == "123"


def test_private_access(env):
    with pytest.raises(jinja2.exceptions.SecurityError):
        env.from_string(r"{{ func.__code__ }}").render(func=test_private_access)


@pytest.mark.parametrize(
    ["name", "expected"],
    (
        ("ds", "2012-07-24"),
        ("ds_nodash", "20120724"),
        ("ts", "2012-07-24T03:04:52+00:00"),
        ("ts_nodash", "20120724T030452"),
        ("ts_nodash_with_tz", "20120724T030452+0000"),
    ),
)
def test_filters(env, name, expected):
    when = pendulum.datetime(2012, 7, 24, 3, 4, 52, tz="UTC")
    result = env.from_string("{{ date |" + name + " }}").render(date=when)
    assert result == expected

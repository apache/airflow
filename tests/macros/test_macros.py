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

import lazy_object_proxy
import pendulum
import pytest

from airflow import macros
from airflow.utils import timezone


@pytest.mark.parametrize(
    "ds, days, expected",
    [
        ("2015-01-01", 5, "2015-01-06"),
        ("2015-01-02", 0, "2015-01-02"),
        ("2015-01-06", -5, "2015-01-01"),
        (lazy_object_proxy.Proxy(lambda: "2015-01-01"), 5, "2015-01-06"),
        (lazy_object_proxy.Proxy(lambda: "2015-01-02"), 0, "2015-01-02"),
        (lazy_object_proxy.Proxy(lambda: "2015-01-06"), -5, "2015-01-01"),
    ],
)
def test_ds_add(ds, days, expected):
    result = macros.ds_add(ds, days)
    assert result == expected


@pytest.mark.parametrize(
    "ds, input_format, output_format, expected",
    [
        ("2015-01-02", "%Y-%m-%d", "%m-%d-%y", "01-02-15"),
        ("2015-01-02", "%Y-%m-%d", "%Y-%m-%d", "2015-01-02"),
        ("1/5/2015", "%m/%d/%Y", "%m-%d-%y", "01-05-15"),
        ("1/5/2015", "%m/%d/%Y", "%Y-%m-%d", "2015-01-05"),
        (lazy_object_proxy.Proxy(lambda: "2015-01-02"), "%Y-%m-%d", "%m-%d-%y", "01-02-15"),
        (lazy_object_proxy.Proxy(lambda: "2015-01-02"), "%Y-%m-%d", "%Y-%m-%d", "2015-01-02"),
        (lazy_object_proxy.Proxy(lambda: "1/5/2015"), "%m/%d/%Y", "%m-%d-%y", "01-05-15"),
        (lazy_object_proxy.Proxy(lambda: "1/5/2015"), "%m/%d/%Y", "%Y-%m-%d", "2015-01-05"),
    ],
)
def test_ds_format(ds, input_format, output_format, expected):
    result = macros.ds_format(ds, input_format, output_format)
    assert result == expected


@pytest.mark.parametrize(
    "dt, since, expected",
    [
        (
            timezone.datetime(2017, 1, 2),
            None,
            pendulum.instance(timezone.datetime(2017, 1, 2)).diff_for_humans(),
        ),
        (timezone.datetime(2017, 1, 2), timezone.datetime(2017, 1, 3), "1 day before"),
        (timezone.datetime(2017, 1, 2), timezone.datetime(2017, 1, 1), "1 day after"),
        (
            lazy_object_proxy.Proxy(lambda: timezone.datetime(2017, 1, 2)),
            None,
            pendulum.instance(timezone.datetime(2017, 1, 2)).diff_for_humans(),
        ),
        (
            lazy_object_proxy.Proxy(lambda: timezone.datetime(2017, 1, 2)),
            timezone.datetime(2017, 1, 3),
            "1 day before",
        ),
        (
            lazy_object_proxy.Proxy(lambda: timezone.datetime(2017, 1, 2)),
            timezone.datetime(2017, 1, 1),
            "1 day after",
        ),
    ],
)
def test_datetime_diff_for_humans(dt, since, expected):
    result = macros.datetime_diff_for_humans(dt, since)
    assert result == expected


@pytest.mark.parametrize(
    "input_value, expected",
    [
        ('{"field1":"value1", "field2":4, "field3":true}', {"field1": "value1", "field2": 4, "field3": True}),
        (
            '{"field1": [ 1, 2, 3, 4, 5 ], "field2" : {"mini1" : 1, "mini2" : "2"}}',
            {"field1": [1, 2, 3, 4, 5], "field2": {"mini1": 1, "mini2": "2"}},
        ),
    ],
)
def test_json_loads(input_value, expected):
    result = macros.json.loads(input_value)
    assert result == expected


@pytest.mark.parametrize(
    "input_value, expected",
    [
        ('{"field1":"value1", "field2":4, "field3":true}', {"field1": "value1", "field2": 4, "field3": True}),
        ("field1: value1\nfield2: value2", {"field1": "value1", "field2": "value2"}),
        (
            'field1: [ 1, 2, 3, 4, 5 ]\nfield2: {"mini1" : 1, "mini2" : "2"}',
            {"field1": [1, 2, 3, 4, 5], "field2": {"mini1": 1, "mini2": "2"}},
        ),
    ],
)
def test_yaml_loads(input_value, expected):
    result = macros.yaml.safe_load(input_value)
    assert result == expected

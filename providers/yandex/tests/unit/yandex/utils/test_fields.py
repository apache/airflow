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

from airflow.providers.yandex.utils.fields import get_field_from_extras


def test_get_field_from_extras():
    field_name = "some-field"
    default = None
    expected = "some-value"
    extras = {
        field_name: expected,
    }

    res = get_field_from_extras(
        extras=extras,
        field_name=field_name,
        default=default,
    )

    assert res == expected


def test_get_field_from_extras_not_found():
    field_name = "some-field"
    default = "default"
    expected = default
    extras = {}

    res = get_field_from_extras(
        extras=extras,
        field_name=field_name,
        default=default,
    )

    assert res == expected


def test_get_field_from_extras_prefixed_in_extra():
    field_name = "some-field"
    default = None
    expected = "some-value"
    extras = {
        f"extra__yandexcloud__{field_name}": expected,
    }

    res = get_field_from_extras(
        extras=extras,
        field_name=field_name,
        default=default,
    )

    assert res == expected


def test_get_field_from_extras_field_name_with_extra_raise_exception():
    field_name = "extra__yandexcloud__field"
    default = None
    extras = {}

    with pytest.raises(ValueError, match="extra__yandexcloud__field"):
        get_field_from_extras(
            extras=extras,
            field_name=field_name,
            default=default,
        )

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

from unittest import mock

import pendulum
import pytest

from airflow.api_connexion.exceptions import BadRequest
from airflow.api_connexion.parameters import (
    check_limit,
    format_datetime,
    format_parameters,
    validate_istimezone,
)
from airflow.utils import timezone
from tests_common.test_utils.config import conf_vars


class TestValidateIsTimezone:
    def setup_method(self) -> None:
        from datetime import datetime

        self.naive = datetime.now()
        self.timezoned = datetime.now(tz=timezone.utc)

    def test_gives_400_for_naive(self):
        with pytest.raises(BadRequest):
            validate_istimezone(self.naive)

    def test_timezone_passes(self):
        assert validate_istimezone(self.timezoned) is None


class TestDateTimeParser:
    def setup_method(self) -> None:
        self.default_time = "2020-06-13T22:44:00+00:00"
        self.default_time_2 = "2020-06-13T22:44:00Z"

    def test_works_with_datestring_ending_00_00(self):
        datetime = format_datetime(self.default_time)
        datetime2 = timezone.parse(self.default_time)
        assert datetime == datetime2
        assert datetime.isoformat() == self.default_time

    def test_works_with_datestring_ending_with_zed(self):
        datetime = format_datetime(self.default_time_2)
        datetime2 = timezone.parse(self.default_time_2)
        assert datetime == datetime2
        assert datetime.isoformat() == self.default_time  # python uses +00:00 instead of Z

    def test_raises_400_for_invalid_arg(self):
        invalid_datetime = "2020-06-13T22:44:00P"
        with pytest.raises(BadRequest):
            format_datetime(invalid_datetime)


class TestMaximumPagelimit:
    @conf_vars({("api", "maximum_page_limit"): "320"})
    def test_maximum_limit_return_val(self):
        limit = check_limit(300)
        assert limit == 300

    @conf_vars({("api", "maximum_page_limit"): "320"})
    def test_maximum_limit_returns_configured_if_limit_above_conf(self):
        limit = check_limit(350)
        assert limit == 320

    @conf_vars({("api", "maximum_page_limit"): "1000"})
    def test_limit_returns_set_max_if_give_limit_is_exceeded(self):
        limit = check_limit(1500)
        assert limit == 1000

    @conf_vars({("api", "fallback_page_limit"): "100"})
    def test_limit_of_zero_returns_default(self):
        limit = check_limit(0)
        assert limit == 100

    @conf_vars({("api", "maximum_page_limit"): "1500"})
    def test_negative_limit_raises(self):
        with pytest.raises(BadRequest):
            check_limit(-1)


class TestFormatParameters:
    def test_should_works_with_datetime_formatter(self):
        decorator = format_parameters({"param_a": format_datetime})
        endpoint = mock.MagicMock()
        decorated_endpoint = decorator(endpoint)

        decorated_endpoint(param_a="2020-01-01T0:0:00+00:00")

        endpoint.assert_called_once_with(param_a=pendulum.datetime(2020, 1, 1, 0, tz="UTC"))

    def test_should_propagate_exceptions(self):
        decorator = format_parameters({"param_a": format_datetime})
        endpoint = mock.MagicMock()
        decorated_endpoint = decorator(endpoint)
        with pytest.raises(BadRequest):
            decorated_endpoint(param_a="XXXXX")

    @conf_vars({("api", "maximum_page_limit"): "100"})
    def test_should_work_with_limit(self):
        decorator = format_parameters({"limit": check_limit})
        endpoint = mock.MagicMock()
        decorated_endpoint = decorator(endpoint)
        decorated_endpoint(limit=89)
        endpoint.assert_called_once_with(limit=89)

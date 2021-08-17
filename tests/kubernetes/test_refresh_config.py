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

from unittest import TestCase

import pytest
from pendulum.parsing import ParserError

from airflow.kubernetes.refresh_config import _parse_timestamp


class TestRefreshKubeConfigLoader(TestCase):
    def test_parse_timestamp_should_convert_z_timezone_to_unix_timestamp(self):
        ts = _parse_timestamp("2020-01-13T13:42:20Z")
        assert 1578922940 == ts

    def test_parse_timestamp_should_convert_regular_timezone_to_unix_timestamp(self):
        ts = _parse_timestamp("2020-01-13T13:42:20+0600")
        assert 1578922940 == ts

    def test_parse_timestamp_should_throw_exception(self):
        with pytest.raises(ParserError):
            _parse_timestamp("foobar")

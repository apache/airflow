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

import unittest

from airflow.api_connexion.exceptions import BadRequest
from airflow.api_connexion.utils import conn_parse_datetime
from airflow.utils import timezone


class TestDateTimeParser(unittest.TestCase):

    def setUp(self) -> None:
        self.default_time = '2020-06-13T22:44:00+00:00'
        self.default_time_2 = '2020-06-13T22:44:00Z'

    def test_works_with_datestring_ending_00_00(self):
        datetime = conn_parse_datetime(self.default_time)
        datetime2 = timezone.parse(self.default_time)
        assert datetime == datetime2
        assert datetime.isoformat() == self.default_time

    def test_works_with_datestring_ending_with_zed(self):
        datetime = conn_parse_datetime(self.default_time_2)
        datetime2 = timezone.parse(self.default_time_2)
        assert datetime == datetime2
        assert datetime.isoformat() == self.default_time  # python uses +00:00 instead of Z

    def test_raises_400_for_invalid_arg(self):
        invalid_datetime = '2020-06-13T22:44:00P'
        with self.assertRaises(BadRequest):
            conn_parse_datetime(invalid_datetime)

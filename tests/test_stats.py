# -*- coding: utf-8 -*-
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

import unittest

from airflow.stats import SafeStatsdLogger, AllowListValidator
from unittest.mock import Mock


class TestStats(unittest.TestCase):

    def setUp(self):
        self.statsd_client = Mock()
        self.stats = SafeStatsdLogger(self.statsd_client)

    def test_increment_counter_with_valid_name(self):
        self.stats.incr('test_stats_run')
        self.statsd_client.incr.assert_called_once_with('test_stats_run', 1, 1)

    def test_stat_name_must_be_a_string(self):
        self.stats.incr(list())
        self.statsd_client.assert_not_called()

    def test_stat_name_must_not_exceed_max_length(self):
        self.stats.incr('X' * 300)
        self.statsd_client.assert_not_called()

    def test_stat_name_must_only_include_whitelisted_characters(self):
        self.stats.incr('test/$tats')
        self.statsd_client.assert_not_called()


class TestStatsWithAllowList(unittest.TestCase):

    def setUp(self):
        self.statsd_client = Mock()
        self.stats = SafeStatsdLogger(self.statsd_client, AllowListValidator("stats_one, stats_two"))

    def test_increment_counter_with_allowed_key(self):
        self.stats.incr('stats_one')
        self.statsd_client.incr.assert_called_once_with('stats_one', 1, 1)

    def test_increment_counter_with_allowed_prefix(self):
        self.stats.incr('stats_two.bla')
        self.statsd_client.incr.assert_called_once_with('stats_two.bla', 1, 1)

    def test_not_increment_counter_if_not_allowed(self):
        self.stats.incr('stats_three')
        self.statsd_client.assert_not_called()

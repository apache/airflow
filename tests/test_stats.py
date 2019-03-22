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

from airflow.exceptions import InvalidStatsNameException
from airflow.stats import stat_name_default_handler


class TestStats(unittest.TestCase):

    def test_stat_name_default_handler_success(self):
        stat_name = 'task_run'
        stat_name_ = stat_name_default_handler(stat_name)
        self.assertEqual(stat_name, stat_name_)

    def test_stat_name_default_handler_not_string(self):
        try:
            stat_name_default_handler(list())
        except InvalidStatsNameException:
            return
        self.fail()

    def test_stat_name_default_handler_exceed_max_length(self):
        try:
            stat_name_default_handler('123456', 3)
        except InvalidStatsNameException:
            return
        self.fail()

    def test_stat_name_default_handler_invalid_character(self):
        try:
            stat_name_default_handler(':123456')
        except InvalidStatsNameException:
            return
        self.fail()

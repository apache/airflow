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

from airflow.utils.trigger_rule import TriggerRule


class TestTriggerRule(unittest.TestCase):

    def test_valid_trigger_rules(self):
        self.assertTrue(TriggerRule.is_valid(TriggerRule.ALL_SUCCESS))
        self.assertTrue(TriggerRule.is_valid(TriggerRule.ALL_FAILED))
        self.assertTrue(TriggerRule.is_valid(TriggerRule.ALL_DONE))
        self.assertTrue(TriggerRule.is_valid(TriggerRule.ONE_SUCCESS))
        self.assertTrue(TriggerRule.is_valid(TriggerRule.ONE_FAILED))
        self.assertTrue(TriggerRule.is_valid(TriggerRule.NONE_FAILED))
        self.assertTrue(TriggerRule.is_valid(TriggerRule.NONE_SKIPPED))
        self.assertTrue(TriggerRule.is_valid(TriggerRule.DUMMY))
        self.assertEqual(len(TriggerRule.all_triggers()), 8)

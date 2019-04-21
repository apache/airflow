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
        self.assertTrue(TriggerRule.ALL_SUCCESS in TriggerRule)
        self.assertTrue(TriggerRule.ALL_FAILED in TriggerRule)
        self.assertTrue(TriggerRule.ALL_DONE in TriggerRule)
        self.assertTrue(TriggerRule.ONE_SUCCESS in TriggerRule)
        self.assertTrue(TriggerRule.ONE_FAILED in TriggerRule)
        self.assertTrue(TriggerRule.NONE_FAILED in TriggerRule)
        self.assertTrue(TriggerRule.NONE_SKIPPED in TriggerRule)
        self.assertTrue(TriggerRule.DUMMY in TriggerRule)
        self.assertEqual(len(TriggerRule.all_triggers()), 8)

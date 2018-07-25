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
from datetime import datetime

from airflow.models import BaseOperator, TaskInstance
from airflow.utils.trigger_rule import TriggerRule
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep
from airflow.utils.state import State


class TriggerRuleDepTest(unittest.TestCase):

    def _get_task_instance(self, trigger_rule=TriggerRule.ALL_SUCCESS,
                           state=None, upstream_task_ids=None):
        task = BaseOperator(task_id='test_task', trigger_rule=trigger_rule,
                            start_date=datetime(2015, 1, 1))
        if upstream_task_ids:
            task._upstream_task_ids.update(upstream_task_ids)
        return TaskInstance(task=task, state=state, execution_date=None)

    def test_no_upstream_tasks(self):
        """
        If the TI has no upstream TIs then there is nothing to check and the dep is passed
        """
        ti = self._get_task_instance(TriggerRule.ALL_DONE, State.UP_FOR_RETRY)
        self.assertTrue(TriggerRuleDep().is_met(ti=ti))

    def test_dummy_tr(self):
        """
        The dummy trigger rule should always pass this dep
        """
        ti = self._get_task_instance(TriggerRule.DUMMY, State.UP_FOR_RETRY)
        self.assertTrue(TriggerRuleDep().is_met(ti=ti))

    def test_one_success_tr_success(self):
        """
        One-success trigger rule success
        """
        ti = self._get_task_instance(TriggerRule.ONE_SUCCESS, State.UP_FOR_RETRY)
        dep_statuses = tuple(TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=1,
            skipped=2,
            failed=2,
            upstream_failed=2,
            done=2,
            flag_upstream_failed=False,
            session="Fake Session"))
        self.assertEqual(len(dep_statuses), 0)

    def test_one_success_tr_failure(self):
        """
        One-success trigger rule failure
        """
        ti = self._get_task_instance(TriggerRule.ONE_SUCCESS)
        dep_statuses = tuple(TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=0,
            skipped=2,
            failed=2,
            upstream_failed=2,
            done=2,
            flag_upstream_failed=False,
            session="Fake Session"))
        self.assertEqual(len(dep_statuses), 1)
        self.assertFalse(dep_statuses[0].passed)

    def test_one_failure_tr_failure(self):
        """
        One-failure trigger rule failure
        """
        ti = self._get_task_instance(TriggerRule.ONE_FAILED)
        dep_statuses = tuple(TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=2,
            skipped=0,
            failed=0,
            upstream_failed=0,
            done=2,
            flag_upstream_failed=False,
            session="Fake Session"))
        self.assertEqual(len(dep_statuses), 1)
        self.assertFalse(dep_statuses[0].passed)

    def test_one_failure_tr_success(self):
        """
        One-failure trigger rule success
        """
        ti = self._get_task_instance(TriggerRule.ONE_FAILED)
        dep_statuses = tuple(TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=0,
            skipped=2,
            failed=2,
            upstream_failed=0,
            done=2,
            flag_upstream_failed=False,
            session="Fake Session"))
        self.assertEqual(len(dep_statuses), 0)

        dep_statuses = tuple(TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=0,
            skipped=2,
            failed=0,
            upstream_failed=2,
            done=2,
            flag_upstream_failed=False,
            session="Fake Session"))
        self.assertEqual(len(dep_statuses), 0)

    def test_all_success_tr_success(self):
        """
        All-success trigger rule success
        """
        ti = self._get_task_instance(TriggerRule.ALL_SUCCESS,
                                     upstream_task_ids=["FakeTaskID"])
        dep_statuses = tuple(TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=1,
            skipped=0,
            failed=0,
            upstream_failed=0,
            done=1,
            flag_upstream_failed=False,
            session="Fake Session"))
        self.assertEqual(len(dep_statuses), 0)

    def test_all_success_tr_failure(self):
        """
        All-success trigger rule failure
        """
        ti = self._get_task_instance(TriggerRule.ALL_SUCCESS,
                                     upstream_task_ids=["FakeTaskID",
                                                        "OtherFakeTaskID"])
        dep_statuses = tuple(TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=1,
            skipped=0,
            failed=1,
            upstream_failed=0,
            done=2,
            flag_upstream_failed=False,
            session="Fake Session"))
        self.assertEqual(len(dep_statuses), 1)
        self.assertFalse(dep_statuses[0].passed)

    def test_all_failed_tr_success(self):
        """
        All-failed trigger rule success
        """
        ti = self._get_task_instance(TriggerRule.ALL_FAILED,
                                     upstream_task_ids=["FakeTaskID",
                                                        "OtherFakeTaskID"])
        dep_statuses = tuple(TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=0,
            skipped=0,
            failed=2,
            upstream_failed=0,
            done=2,
            flag_upstream_failed=False,
            session="Fake Session"))
        self.assertEqual(len(dep_statuses), 0)

    def test_all_failed_tr_failure(self):
        """
        All-failed trigger rule failure
        """
        ti = self._get_task_instance(TriggerRule.ALL_FAILED,
                                     upstream_task_ids=["FakeTaskID",
                                                        "OtherFakeTaskID"])
        dep_statuses = tuple(TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=2,
            skipped=0,
            failed=0,
            upstream_failed=0,
            done=2,
            flag_upstream_failed=False,
            session="Fake Session"))
        self.assertEqual(len(dep_statuses), 1)
        self.assertFalse(dep_statuses[0].passed)

    def test_all_done_tr_success(self):
        """
        All-done trigger rule success
        """
        ti = self._get_task_instance(TriggerRule.ALL_DONE,
                                     upstream_task_ids=["FakeTaskID",
                                                        "OtherFakeTaskID"])
        dep_statuses = tuple(TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=2,
            skipped=0,
            failed=0,
            upstream_failed=0,
            done=2,
            flag_upstream_failed=False,
            session="Fake Session"))
        self.assertEqual(len(dep_statuses), 0)

    def test_all_done_tr_failure(self):
        """
        All-done trigger rule failure
        """
        ti = self._get_task_instance(TriggerRule.ALL_DONE,
                                     upstream_task_ids=["FakeTaskID",
                                                        "OtherFakeTaskID"])
        dep_statuses = tuple(TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=1,
            skipped=0,
            failed=0,
            upstream_failed=0,
            done=1,
            flag_upstream_failed=False,
            session="Fake Session"))
        self.assertEqual(len(dep_statuses), 1)
        self.assertFalse(dep_statuses[0].passed)

    def test_unknown_tr(self):
        """
        Unknown trigger rules should cause this dep to fail
        """
        ti = self._get_task_instance()
        ti.task.trigger_rule = "Unknown Trigger Rule"
        dep_statuses = tuple(TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=1,
            skipped=0,
            failed=0,
            upstream_failed=0,
            done=1,
            flag_upstream_failed=False,
            session="Fake Session"))

        self.assertEqual(len(dep_statuses), 1)
        self.assertFalse(dep_statuses[0].passed)

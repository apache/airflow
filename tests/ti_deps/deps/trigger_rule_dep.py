# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from airflow.utils.trigger_rule import TriggerRule
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep
from airflow.utils.state import State
from fake_models import FakeTask, FakeTI


class TriggerRuleDepTest(unittest.TestCase):

    def test_no_upstream_tasks(self):
        """
        If the TI has no upstream TIs then there is nothing to check and the dep is passed
        """
        task = FakeTask(
            trigger_rule=TriggerRule.ALL_DONE,
            upstream_list=[])
        ti = FakeTI(
            task=task,
            state=State.UP_FOR_RETRY)

        self.assertTrue(TriggerRuleDep().is_met(ti=ti, dep_context=None))

    def test_dummy_tr(self):
        """
        The dummy trigger rule should always pass this dep
        """
        task = FakeTask(
            trigger_rule=TriggerRule.DUMMY,
            upstream_list=[])
        ti = FakeTI(
            task=task,
            state=State.UP_FOR_RETRY)

        self.assertTrue(TriggerRuleDep().is_met(ti=ti, dep_context=None))

    def test_one_success_tr_success(self):
        """
        One-success trigger rule success
        """
        task = FakeTask(
            trigger_rule=TriggerRule.ONE_SUCCESS,
            upstream_task_ids=[])
        ti = FakeTI(task=task)

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
        task = FakeTask(
            trigger_rule=TriggerRule.ONE_SUCCESS,
            upstream_task_ids=[])
        ti = FakeTI(task=task)

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
        task = FakeTask(
            trigger_rule=TriggerRule.ONE_FAILED,
            upstream_task_ids=[])
        ti = FakeTI(task=task)

        dep_statuses = tuple(TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=2,
            skipped=0,
            failed=0,
            upstream_failed=0,
            done=2,
            flag_upstream_failed=False,
            session="Fake Session"))

    def test_one_failure_tr_success(self):
        """
        One-failure trigger rule success
        """
        task = FakeTask(
            trigger_rule=TriggerRule.ONE_FAILED,
            upstream_task_ids=[])
        ti = FakeTI(task=task)

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
        task = FakeTask(
            trigger_rule=TriggerRule.ALL_SUCCESS,
            upstream_task_ids=["FakeTaskID"])
        ti = FakeTI(task=task)

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
        task = FakeTask(
            trigger_rule=TriggerRule.ALL_SUCCESS,
            upstream_task_ids=["FakeTaskID", "OtherFakeTaskID"])
        ti = FakeTI(task=task)

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
        task = FakeTask(
            trigger_rule=TriggerRule.ALL_FAILED,
            upstream_task_ids=["FakeTaskID", "OtherFakeTaskID"])
        ti = FakeTI(task=task)

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
        task = FakeTask(
            trigger_rule=TriggerRule.ALL_FAILED,
            upstream_task_ids=["FakeTaskID", "OtherFakeTaskID"])
        ti = FakeTI(task=task)

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
        task = FakeTask(
            trigger_rule=TriggerRule.ALL_DONE,
            upstream_task_ids=["FakeTaskID", "OtherFakeTaskID"])
        ti = FakeTI(task=task)

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
        task = FakeTask(
            trigger_rule=TriggerRule.ALL_DONE,
            upstream_task_ids=["FakeTaskID", "OtherFakeTaskID"])
        ti = FakeTI(task=task)

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
        task = FakeTask(
            trigger_rule="Unknown Trigger Rule",
            upstream_task_ids=[])
        ti = FakeTI(task=task)

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

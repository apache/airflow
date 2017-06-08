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
#

import unittest

from airflow import models
from airflow.api.common.experimental.mark_tasks import set_state, _create_dagruns
from airflow.settings import Session
from airflow.utils.dates import days_ago
from airflow.utils.state import State


DEV_NULL = "/dev/null"


class TestMarkTasks(unittest.TestCase):
    def setUp(self):
        self.dagbag = models.DagBag(include_examples=True)
        self.dag1 = self.dagbag.dags['test_example_bash_operator']
        self.dag2 = self.dagbag.dags['example_subdag_operator']

        self.execution_dates = [days_ago(2), days_ago(1)]

        drs = _create_dagruns(self.dag1, self.execution_dates,
                              state=State.RUNNING,
                              run_id_template="scheduled__{}")
        for dr in drs:
            dr.dag = self.dag1
            dr.verify_integrity()

        drs = _create_dagruns(self.dag2,
                              [self.dag2.default_args['start_date']],
                              state=State.RUNNING,
                              run_id_template="scheduled__{}")

        for dr in drs:
            dr.dag = self.dag2
            dr.verify_integrity()

        self.session = Session()

    def snapshot_state(self, dag, execution_dates):
        TI = models.TaskInstance
        tis = self.session.query(TI).filter(
            TI.dag_id==dag.dag_id,
            TI.execution_date.in_(execution_dates)
        ).all()

        self.session.expunge_all()

        return tis

    def verify_state(self, dag, task_ids, execution_dates, state, old_tis):
        TI = models.TaskInstance

        tis = self.session.query(TI).filter(
            TI.dag_id==dag.dag_id,
            TI.execution_date.in_(execution_dates)
        ).all()

        self.assertTrue(len(tis) > 0)

        for ti in tis:
            if ti.task_id in task_ids and ti.execution_date in execution_dates:
                self.assertEqual(ti.state, state)
            else:
                for old_ti in old_tis:
                    if (old_ti.task_id == ti.task_id
                            and old_ti.execution_date == ti.execution_date):
                            self.assertEqual(ti.state, old_ti.state)

    def test_mark_tasks_now(self):
        # set one task to success but do not commit
        snapshot = self.snapshot_state(self.dag1, self.execution_dates)
        task = self.dag1.get_task("runme_1")
        altered = set_state(task=task, execution_date=self.execution_dates[0],
                            upstream=False, downstream=False, future=False,
                            past=False, state=State.SUCCESS, commit=False)
        self.assertEqual(len(altered), 1)
        self.verify_state(self.dag1, [task.task_id], [self.execution_dates[0]],
                          None, snapshot)

        # set one and only one task to success
        altered = set_state(task=task, execution_date=self.execution_dates[0],
                            upstream=False, downstream=False, future=False,
                            past=False, state=State.SUCCESS, commit=True)
        self.assertEqual(len(altered), 1)
        self.verify_state(self.dag1, [task.task_id], [self.execution_dates[0]],
                          State.SUCCESS, snapshot)

        # set no tasks
        altered = set_state(task=task, execution_date=self.execution_dates[0],
                            upstream=False, downstream=False, future=False,
                            past=False, state=State.SUCCESS, commit=True)
        self.assertEqual(len(altered), 0)
        self.verify_state(self.dag1, [task.task_id], [self.execution_dates[0]],
                          State.SUCCESS, snapshot)

        # set task to other than success
        altered = set_state(task=task, execution_date=self.execution_dates[0],
                            upstream=False, downstream=False, future=False,
                            past=False, state=State.FAILED, commit=True)
        self.assertEqual(len(altered), 1)
        self.verify_state(self.dag1, [task.task_id], [self.execution_dates[0]],
                          State.FAILED, snapshot)

        # dont alter other tasks
        snapshot = self.snapshot_state(self.dag1, self.execution_dates)
        task = self.dag1.get_task("runme_0")
        altered = set_state(task=task, execution_date=self.execution_dates[0],
                            upstream=False, downstream=False, future=False,
                            past=False, state=State.SUCCESS, commit=True)
        self.assertEqual(len(altered), 1)
        self.verify_state(self.dag1, [task.task_id], [self.execution_dates[0]],
                          State.SUCCESS, snapshot)

    def test_mark_downstream(self):
        # test downstream
        snapshot = self.snapshot_state(self.dag1, self.execution_dates)
        task = self.dag1.get_task("runme_1")
        relatives = task.get_flat_relatives(upstream=False)
        task_ids = [t.task_id for t in relatives]
        task_ids.append(task.task_id)

        altered = set_state(task=task, execution_date=self.execution_dates[0],
                            upstream=False, downstream=True, future=False,
                            past=False, state=State.SUCCESS, commit=True)
        self.assertEqual(len(altered), 3)
        self.verify_state(self.dag1, task_ids, [self.execution_dates[0]],
                          State.SUCCESS, snapshot)

    def test_mark_upstream(self):
        # test upstream
        snapshot = self.snapshot_state(self.dag1, self.execution_dates)
        task = self.dag1.get_task("run_after_loop")
        relatives = task.get_flat_relatives(upstream=True)
        task_ids = [t.task_id for t in relatives]
        task_ids.append(task.task_id)

        altered = set_state(task=task, execution_date=self.execution_dates[0],
                            upstream=True, downstream=False, future=False,
                            past=False, state=State.SUCCESS, commit=True)
        self.assertEqual(len(altered), 4)
        self.verify_state(self.dag1, task_ids, [self.execution_dates[0]],
                          State.SUCCESS, snapshot)

    def test_mark_tasks_future(self):
        # set one task to success towards end of scheduled dag runs
        snapshot = self.snapshot_state(self.dag1, self.execution_dates)
        task = self.dag1.get_task("runme_1")
        altered = set_state(task=task, execution_date=self.execution_dates[0],
                            upstream=False, downstream=False, future=True,
                            past=False, state=State.SUCCESS, commit=True)
        self.assertEqual(len(altered), 2)
        self.verify_state(self.dag1, [task.task_id], self.execution_dates,
                          State.SUCCESS, snapshot)

    def test_mark_tasks_past(self):
        # set one task to success towards end of scheduled dag runs
        snapshot = self.snapshot_state(self.dag1, self.execution_dates)
        task = self.dag1.get_task("runme_1")
        altered = set_state(task=task, execution_date=self.execution_dates[1],
                            upstream=False, downstream=False, future=False,
                            past=True, state=State.SUCCESS, commit=True)
        self.assertEqual(len(altered), 2)
        self.verify_state(self.dag1, [task.task_id], self.execution_dates,
                          State.SUCCESS, snapshot)

    def test_mark_tasks_subdag(self):
        # set one task to success towards end of scheduled dag runs
        task = self.dag2.get_task("section-1")
        relatives = task.get_flat_relatives(upstream=False)
        task_ids = [t.task_id for t in relatives]
        task_ids.append(task.task_id)

        altered = set_state(task=task, execution_date=self.execution_dates[0],
                            upstream=False, downstream=True, future=False,
                            past=False, state=State.SUCCESS, commit=True)
        self.assertEqual(len(altered), 14)

        # cannot use snapshot here as that will require drilling down the
        # the sub dag tree essentially recreating the same code as in the
        # tested logic.
        self.verify_state(self.dag2, task_ids, [self.execution_dates[0]],
                          State.SUCCESS, [])

    def tearDown(self):
        self.dag1.clear()
        self.dag2.clear()

        # just to make sure we are fully cleaned up
        self.session.query(models.DagRun).delete()
        self.session.query(models.TaskInstance).delete()
        self.session.commit()

        self.session.close()

if __name__ == '__main__':
    unittest.main()

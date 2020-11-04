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

from airflow.lineage import AUTO
from airflow.lineage.entities import File
from airflow.models import DAG, TaskInstance as TI
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2016, 1, 1)


class TestLineage(unittest.TestCase):
    def test_lineage(self):
        dag = DAG(dag_id='test_prepare_lineage', start_date=DEFAULT_DATE)

        f1s = "/tmp/does_not_exist_1-{}"
        f2s = "/tmp/does_not_exist_2-{}"
        f3s = "/tmp/does_not_exist_3"
        file1 = File(f1s.format("{{ execution_date }}"))
        file2 = File(f2s.format("{{ execution_date }}"))
        file3 = File(f3s)

        with dag:
            op1 = DummyOperator(
                task_id='leave1',
                inlets=file1,
                outlets=[
                    file2,
                ],
            )
            op2 = DummyOperator(task_id='leave2')
            op3 = DummyOperator(task_id='upstream_level_1', inlets=AUTO, outlets=file3)
            op4 = DummyOperator(task_id='upstream_level_2')
            op5 = DummyOperator(task_id='upstream_level_3', inlets=["leave1", "upstream_level_1"])

            op1.set_downstream(op3)
            op2.set_downstream(op3)
            op3.set_downstream(op4)
            op4.set_downstream(op5)

        dag.clear()

        # execution_date is set in the context in order to avoid creating task instances
        ctx1 = {"ti": TI(task=op1, execution_date=DEFAULT_DATE), "execution_date": DEFAULT_DATE}
        ctx2 = {"ti": TI(task=op2, execution_date=DEFAULT_DATE), "execution_date": DEFAULT_DATE}
        ctx3 = {"ti": TI(task=op3, execution_date=DEFAULT_DATE), "execution_date": DEFAULT_DATE}
        ctx5 = {"ti": TI(task=op5, execution_date=DEFAULT_DATE), "execution_date": DEFAULT_DATE}

        # prepare with manual inlets and outlets
        op1.pre_execute(ctx1)

        self.assertEqual(len(op1.inlets), 1)
        self.assertEqual(op1.inlets[0].url, f1s.format(DEFAULT_DATE))

        self.assertEqual(len(op1.outlets), 1)
        self.assertEqual(op1.outlets[0].url, f2s.format(DEFAULT_DATE))

        # post process with no backend
        op1.post_execute(ctx1)

        op2.pre_execute(ctx2)
        self.assertEqual(len(op2.inlets), 0)
        op2.post_execute(ctx2)

        op3.pre_execute(ctx3)
        self.assertEqual(len(op3.inlets), 1)
        self.assertEqual(op3.inlets[0].url, f2s.format(DEFAULT_DATE))
        self.assertEqual(op3.outlets[0], file3)
        op3.post_execute(ctx3)

        # skip 4

        op5.pre_execute(ctx5)
        self.assertEqual(len(op5.inlets), 2)
        op5.post_execute(ctx5)

    def test_lineage_render(self):
        # tests inlets / outlets are rendered if they are added
        # after initalization
        dag = DAG(dag_id='test_lineage_render', start_date=DEFAULT_DATE)

        with dag:
            op1 = DummyOperator(task_id='task1')

        f1s = "/tmp/does_not_exist_1-{}"
        file1 = File(f1s.format("{{ execution_date }}"))

        op1.inlets.append(file1)
        op1.outlets.append(file1)

        # execution_date is set in the context in order to avoid creating task instances
        ctx1 = {"ti": TI(task=op1, execution_date=DEFAULT_DATE), "execution_date": DEFAULT_DATE}

        op1.pre_execute(ctx1)
        self.assertEqual(op1.inlets[0].url, f1s.format(DEFAULT_DATE))
        self.assertEqual(op1.outlets[0].url, f1s.format(DEFAULT_DATE))

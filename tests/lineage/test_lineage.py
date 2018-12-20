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

from airflow.lineage import apply_lineage, prepare_lineage
from airflow.lineage.datasets import File
from airflow.models import DAG, TaskInstance as TI
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2016, 1, 1)

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


class TestLineage(unittest.TestCase):

    @mock.patch("airflow.lineage._get_backend")
    def test_lineage(self, _get_backend):
        backend = mock.Mock()
        send_mock = mock.Mock()
        backend.send_lineage = send_mock

        _get_backend.return_value = backend

        dag = DAG(
            dag_id='test_prepare_lineage',
            start_date=DEFAULT_DATE
        )

        f1 = File("/tmp/does_not_exist_1")
        f2 = File("/tmp/does_not_exist_2")
        f3 = File("/tmp/does_not_exist_3")

        with dag:
            op1 = DummyOperator(task_id='leave1',
                                inlets={"datasets": [f1, ]},
                                outlets={"datasets": [f2, ]})
            op2 = DummyOperator(task_id='leave2')
            op3 = DummyOperator(task_id='upstream_level_1',
                                inlets={"auto": True},
                                outlets={"datasets": [f3, ]})
            op4 = DummyOperator(task_id='upstream_level_2')
            op5 = DummyOperator(task_id='upstream_level_3',
                                inlets={"task_ids": ["leave1", "upstream_level_1"]})

            op1.set_downstream(op3)
            op2.set_downstream(op3)
            op3.set_downstream(op4)
            op4.set_downstream(op5)

        ctx1 = {"ti": TI(task=op1, execution_date=DEFAULT_DATE)}
        ctx2 = {"ti": TI(task=op2, execution_date=DEFAULT_DATE)}
        ctx3 = {"ti": TI(task=op3, execution_date=DEFAULT_DATE)}
        ctx5 = {"ti": TI(task=op5, execution_date=DEFAULT_DATE)}

        func = mock.Mock()
        func.__name__ = 'foo'

        # prepare with manual inlets and outlets
        prep = prepare_lineage(func)
        prep(op1, ctx1)

        self.assertEqual(len(op1.inlets), 1)
        self.assertEqual(op1.inlets[0], f1)

        self.assertEqual(len(op1.outlets), 1)
        self.assertEqual(op1.outlets[0], f2)

        # post process with no backend
        post = apply_lineage(func)
        post(op1, ctx1)
        self.assertEqual(send_mock.call_count, 1)
        send_mock.reset_mock()

        prep(op2, ctx2)
        self.assertEqual(len(op2.inlets), 0)
        post(op2, ctx2)
        self.assertEqual(send_mock.call_count, 1)
        send_mock.reset_mock()

        prep(op3, ctx3)
        self.assertEqual(len(op3.inlets), 1)
        self.assertEqual(op3.inlets[0].qualified_name, f2.qualified_name)
        post(op3, ctx3)
        self.assertEqual(send_mock.call_count, 1)
        send_mock.reset_mock()

        # skip 4

        prep(op5, ctx5)
        self.assertEqual(len(op5.inlets), 2)
        post(op5, ctx5)
        self.assertEqual(send_mock.call_count, 1)
        send_mock.reset_mock()

    @mock.patch("airflow.lineage._get_backend")
    def test_lineage_auto_simple_skipping(self, _get_backend):
        # Tests the ability for the auto feature to skip non state affecting operators
        # DAG diagram:
        #  1--->2--->3--->4

        backend = mock.Mock()
        send_mock = mock.Mock()
        backend.send_lineage = send_mock

        _get_backend.return_value = backend

        dag = DAG(
            dag_id='test_prepare_lineage_auto_simple_skipping',
            start_date=DEFAULT_DATE
        )

        f1 = File("/tmp/does_not_exist_1")

        with dag:
            op1 = DummyOperator(task_id='leave1',
                                outlets={"datasets": [f1, ]})
            op2 = DummyOperator(task_id='upstream_level_1')
            op3 = DummyOperator(task_id='upstream_level_2')
            op4 = DummyOperator(task_id='upstream_level_3', inlets={"auto": True})

            op1.set_downstream(op2)
            op2.set_downstream(op3)
            op3.set_downstream(op4)

        ctx1 = {"ti": TI(task=op1, execution_date=DEFAULT_DATE)}
        ctx2 = {"ti": TI(task=op2, execution_date=DEFAULT_DATE)}
        ctx3 = {"ti": TI(task=op3, execution_date=DEFAULT_DATE)}
        ctx4 = {"ti": TI(task=op4, execution_date=DEFAULT_DATE)}

        func = mock.Mock()
        func.__name__ = 'foo'

        # prepare with manual inlets and outlets
        prep = prepare_lineage(func)
        prep(op1, ctx1)

        self.assertEqual(len(op1.inlets), 0)

        self.assertEqual(len(op1.outlets), 1)
        self.assertEqual(op1.outlets[0], f1)

        # post process with no backend
        post = apply_lineage(func)
        post(op1, ctx1)
        send_mock.reset_mock()

        prep(op2, ctx2)
        self.assertEqual(len(op2.inlets), 0)
        post(op2, ctx2)
        self.assertEqual(send_mock.call_count, 1)
        send_mock.reset_mock()

        prep(op3, ctx3)
        self.assertEqual(len(op3.inlets), 0)
        post(op3, ctx3)
        self.assertEqual(send_mock.call_count, 1)
        send_mock.reset_mock()

        prep(op4, ctx4)
        self.assertEqual(len(op4.inlets), 1)
        self.assertEqual(op4.inlets[0].name, f1.name)
        post(op4, ctx4)
        self.assertEqual(send_mock.call_count, 1)
        send_mock.reset_mock()

    @mock.patch("airflow.lineage._get_backend")
    def test_lineage_complicated_dag(self, _get_backend):
        # Tests the ability for the auto feature to skip non state affecting operators, while still
        # retrieving data from multiple outlet sources. Notice how if outlets are not specified,
        # that the auto feature continues to traverse down the dag until not input sources are found.

        # DAG diagram:
        # 1-----------+
        #             |
        #             ▼
        #             4 ----------+
        #             ▲           ▼
        #             |           5+-------->6
        # 2-----------+           ▲
        #                         |
        #                         |
        #                         |
        # 3-----------------------+

        backend = mock.Mock()
        send_mock = mock.Mock()
        backend.send_lineage = send_mock

        _get_backend.return_value = backend

        dag = DAG(
            dag_id='test_prepare_lineage_auto_complicated_dag',
            start_date=DEFAULT_DATE
        )

        f1 = File("/tmp/does_not_exist_1")
        f2 = File("/tmp/does_not_exist_2")
        f3 = File("/tmp/does_not_exist_3")

        with dag:
            op1 = DummyOperator(task_id='leave1',
                                outlets={"datasets": [f1, ]},
                                inlets={"auto": True})
            op2 = DummyOperator(task_id='leave2',
                                outlets={"datasets": [f2, ]})
            op3 = DummyOperator(task_id='leave3',
                                outlets={"datasets": [f3, ]})
            op4 = DummyOperator(task_id='upstream_level_1')
            op5 = DummyOperator(task_id='upstream_level_2', inlets={"auto": True})
            op6 = DummyOperator(task_id='upstream_level_3', inlets={"auto": True})

            op1.set_downstream(op4)
            op2.set_downstream(op4)
            op3.set_downstream(op5)
            op4.set_downstream(op5)
            op5.set_downstream(op6)

        ctx1 = {"ti": TI(task=op1, execution_date=DEFAULT_DATE)}
        ctx2 = {"ti": TI(task=op2, execution_date=DEFAULT_DATE)}
        ctx3 = {"ti": TI(task=op3, execution_date=DEFAULT_DATE)}
        ctx4 = {"ti": TI(task=op4, execution_date=DEFAULT_DATE)}
        ctx5 = {"ti": TI(task=op5, execution_date=DEFAULT_DATE)}
        ctx6 = {"ti": TI(task=op6, execution_date=DEFAULT_DATE)}

        func = mock.Mock()
        func.__name__ = 'foo'

        # prepare with manual inlets and outlets
        prep = prepare_lineage(func)
        prep(op1, ctx1)

        self.assertEqual(len(op1.outlets), 1)
        self.assertEqual(op1.outlets[0], f1)
        self.assertEqual(len(op1.inlets), 0)

        # post process with no backend
        post = apply_lineage(func)
        post(op1, ctx1)

        prep(op2, ctx2)
        self.assertEqual(len(op2.outlets), 1)
        post(op2, ctx2)

        prep(op3, ctx3)
        self.assertEqual(len(op3.outlets), 1)
        post(op3, ctx3)

        prep(op4, ctx4)
        self.assertEqual(len(op4.inlets), 0)
        post(op4, ctx4)

        prep(op5, ctx5)
        self.assertEqual(len(op5.inlets), 3)
        self.assertEqual({file.qualified_name for file in op5.inlets}, {'file:///tmp/does_not_exist_1',
                                                                        'file:///tmp/does_not_exist_2',
                                                                        'file:///tmp/does_not_exist_3'})
        post(op5, ctx5)

        prep(op6, ctx6)
        self.assertEqual(len(op6.inlets), 3)
        self.assertEqual({file.qualified_name for file in op6.inlets}, {'file:///tmp/does_not_exist_1',
                                                                        'file:///tmp/does_not_exist_2',
                                                                        'file:///tmp/does_not_exist_3'})
        post(op6, ctx6)

    @mock.patch("airflow.lineage._get_backend")
    def test_lineage_auto_branching(self, _get_backend):
        # Tests the ability for the auto feature to skip non state affecting operators
        # DAG diagram:
        #  1--->2---->4
        #       ▼     ▲
        #       3-----+
        backend = mock.Mock()
        send_mock = mock.Mock()
        backend.send_lineage = send_mock

        _get_backend.return_value = backend

        dag = DAG(
            dag_id='test_prepare_lineage_auto_branching',
            start_date=DEFAULT_DATE
        )

        f1 = File("/tmp/does_not_exist_1")

        with dag:
            op1 = DummyOperator(task_id='leave1')
            op2 = DummyOperator(task_id='branch_1', outlets={"datasets": [f1, ]})
            op3 = DummyOperator(task_id='branch_2')
            op4 = DummyOperator(task_id='upstream_level_2', inlets={"auto": True})

            op1.set_downstream(op2)
            op2.set_downstream(op3)
            op2.set_downstream(op4)
            op3.set_downstream(op4)

        ctx1 = {"ti": TI(task=op1, execution_date=DEFAULT_DATE)}
        ctx2 = {"ti": TI(task=op2, execution_date=DEFAULT_DATE)}
        ctx3 = {"ti": TI(task=op3, execution_date=DEFAULT_DATE)}
        ctx4 = {"ti": TI(task=op4, execution_date=DEFAULT_DATE)}

        func = mock.Mock()
        func.__name__ = 'foo'

        # prepare with manual inlets and outlets
        prep = prepare_lineage(func)
        prep(op1, ctx1)

        self.assertEqual(len(op1.inlets), 0)

        # post process with no backend
        post = apply_lineage(func)
        post(op1, ctx1)
        send_mock.reset_mock()

        prep(op2, ctx2)
        self.assertEqual(len(op2.inlets), 0)
        post(op2, ctx2)
        self.assertEqual(send_mock.call_count, 1)
        send_mock.reset_mock()

        prep(op3, ctx3)
        self.assertEqual(len(op3.inlets), 0)
        post(op3, ctx3)
        self.assertEqual(send_mock.call_count, 1)
        send_mock.reset_mock()

        prep(op4, ctx4)
        self.assertEqual(len(op4.inlets), 1)
        self.assertEqual(op4.inlets[0].name, f1.name)
        post(op4, ctx4)
        self.assertEqual(send_mock.call_count, 1)
        send_mock.reset_mock()

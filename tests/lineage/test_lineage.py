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
from tests.compat import mock

DEFAULT_DATE = timezone.datetime(2016, 1, 1)


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

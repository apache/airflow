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
from unittest import mock

from airflow.lineage import AUTO, apply_lineage, get_backend, prepare_lineage
from airflow.lineage.backend import LineageBackend
from airflow.lineage.entities import File
from airflow.models import TaskInstance as TI
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.utils.context import Context
from airflow.utils.types import DagRunType
from tests.test_utils.config import conf_vars

DEFAULT_DATE = timezone.datetime(2016, 1, 1)


class CustomLineageBackend(LineageBackend):
    def send_lineage(self, operator, inlets=None, outlets=None, context=None):
        pass


class TestLineage:
    def test_lineage(self, dag_maker):
        f1s = "/tmp/does_not_exist_1-{}"
        f2s = "/tmp/does_not_exist_2-{}"
        f3s = "/tmp/does_not_exist_3"
        file1 = File(f1s.format("{{ ds }}"))
        file2 = File(f2s.format("{{ ds }}"))
        file3 = File(f3s)

        with dag_maker(dag_id='test_prepare_lineage', start_date=DEFAULT_DATE) as dag:
            op1 = EmptyOperator(
                task_id='leave1',
                inlets=file1,
                outlets=[
                    file2,
                ],
            )
            op2 = EmptyOperator(task_id='leave2')
            op3 = EmptyOperator(task_id='upstream_level_1', inlets=AUTO, outlets=file3)
            op4 = EmptyOperator(task_id='upstream_level_2')
            op5 = EmptyOperator(task_id='upstream_level_3', inlets=["leave1", "upstream_level_1"])

            op1.set_downstream(op3)
            op2.set_downstream(op3)
            op3.set_downstream(op4)
            op4.set_downstream(op5)

        dag.clear()
        dag_run = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)

        ctx1 = Context({"ti": TI(task=op1, run_id=dag_run.run_id), "ds": DEFAULT_DATE})
        ctx2 = Context({"ti": TI(task=op2, run_id=dag_run.run_id), "ds": DEFAULT_DATE})
        ctx3 = Context({"ti": TI(task=op3, run_id=dag_run.run_id), "ds": DEFAULT_DATE})
        ctx5 = Context({"ti": TI(task=op5, run_id=dag_run.run_id), "ds": DEFAULT_DATE})

        # prepare with manual inlets and outlets
        op1.pre_execute(ctx1)

        assert len(op1.inlets) == 1
        assert op1.inlets[0].url == f1s.format(DEFAULT_DATE)

        assert len(op1.outlets) == 1
        assert op1.outlets[0].url == f2s.format(DEFAULT_DATE)

        # post process with no backend
        op1.post_execute(ctx1)

        op2.pre_execute(ctx2)
        assert len(op2.inlets) == 0
        op2.post_execute(ctx2)

        op3.pre_execute(ctx3)
        assert len(op3.inlets) == 1
        assert op3.inlets[0].url == f2s.format(DEFAULT_DATE)
        assert op3.outlets[0] == file3
        op3.post_execute(ctx3)

        # skip 4

        op5.pre_execute(ctx5)
        assert len(op5.inlets) == 2
        op5.post_execute(ctx5)

    def test_lineage_render(self, dag_maker):
        # tests inlets / outlets are rendered if they are added
        # after initialization
        with dag_maker(dag_id='test_lineage_render', start_date=DEFAULT_DATE):
            op1 = EmptyOperator(task_id='task1')
        dag_run = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)

        f1s = "/tmp/does_not_exist_1-{}"
        file1 = File(f1s.format("{{ ds }}"))

        op1.inlets.append(file1)
        op1.outlets.append(file1)

        # execution_date is set in the context in order to avoid creating task instances
        ctx1 = Context({"ti": TI(task=op1, run_id=dag_run.run_id), "ds": DEFAULT_DATE})

        op1.pre_execute(ctx1)
        assert op1.inlets[0].url == f1s.format(DEFAULT_DATE)
        assert op1.outlets[0].url == f1s.format(DEFAULT_DATE)

    @mock.patch("airflow.lineage.get_backend")
    def test_lineage_is_sent_to_backend(self, mock_get_backend, dag_maker):
        class TestBackend(LineageBackend):
            def send_lineage(self, operator, inlets=None, outlets=None, context=None):
                assert len(inlets) == 1
                assert len(outlets) == 1

        func = mock.Mock()
        func.__name__ = 'foo'

        mock_get_backend.return_value = TestBackend()

        with dag_maker(dag_id='test_lineage_is_sent_to_backend', start_date=DEFAULT_DATE):
            op1 = EmptyOperator(task_id='task1')
        dag_run = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)

        file1 = File("/tmp/some_file")

        op1.inlets.append(file1)
        op1.outlets.append(file1)

        (ti,) = dag_run.task_instances
        ctx1 = Context({"ti": ti, "ds": DEFAULT_DATE})

        prep = prepare_lineage(func)
        prep(op1, ctx1)
        post = apply_lineage(func)
        post(op1, ctx1)

    def test_empty_lineage_backend(self):
        backend = get_backend()
        assert backend is None

    @conf_vars({("lineage", "backend"): "tests.lineage.test_lineage.CustomLineageBackend"})
    def test_resolve_lineage_class(self):
        backend = get_backend()
        assert issubclass(backend.__class__, LineageBackend)
        assert isinstance(backend, CustomLineageBackend)

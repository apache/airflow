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
from __future__ import annotations

import datetime
import subprocess
import venv
from datetime import timedelta
from pathlib import Path
from subprocess import CalledProcessError
from tempfile import TemporaryDirectory

import pytest

from airflow.decorators import setup, task, teardown
from airflow.utils import timezone

pytestmark = pytest.mark.db_test


DEFAULT_DATE = timezone.datetime(2016, 1, 1)
END_DATE = timezone.datetime(2016, 1, 2)
INTERVAL = timedelta(hours=12)
FROZEN_NOW = timezone.datetime(2016, 1, 2, 12, 1, 1)

TI_CONTEXT_ENV_VARS = [
    "AIRFLOW_CTX_DAG_ID",
    "AIRFLOW_CTX_TASK_ID",
    "AIRFLOW_CTX_EXECUTION_DATE",
    "AIRFLOW_CTX_DAG_RUN_ID",
]


@pytest.fixture()
def venv_python():
    with TemporaryDirectory() as d:
        venv.create(d, with_pip=False)
        yield Path(d) / "bin" / "python"


@pytest.fixture()
def venv_python_with_dill():
    with TemporaryDirectory() as d:
        venv.create(d, with_pip=True)
        python_path = Path(d) / "bin" / "python"
        subprocess.call([python_path, "-m", "pip", "install", "dill"])
        yield python_path


class TestExternalPythonDecorator:
    def test_with_dill_works(self, dag_maker, venv_python_with_dill):
        @task.external_python(python=venv_python_with_dill, use_dill=True)
        def f():
            """Import dill to double-check it is installed ."""
            import dill  # noqa: F401

        with dag_maker():
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_no_dill_installed_raises_exception_when_use_dill(self, dag_maker, venv_python):
        @task.external_python(python=venv_python, use_dill=True)
        def f():
            pass

        with dag_maker():
            ret = f()

        with pytest.raises(CalledProcessError):
            ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_exception_raises_error(self, dag_maker, venv_python):
        @task.external_python(python=venv_python)
        def f():
            raise Exception

        with dag_maker():
            ret = f()

        with pytest.raises(CalledProcessError):
            ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_with_args(self, dag_maker, venv_python):
        @task.external_python(python=venv_python)
        def f(a, b, c=False, d=False):
            if a == 0 and b == 1 and c and not d:
                return True
            else:
                raise Exception

        with dag_maker():
            ret = f(0, 1, c=True)

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_return_none(self, dag_maker, venv_python):
        @task.external_python(python=venv_python)
        def f():
            return None

        with dag_maker():
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_nonimported_as_arg(self, dag_maker, venv_python):
        @task.external_python(python=venv_python)
        def f(_):
            return None

        with dag_maker():
            ret = f(datetime.datetime.utcnow())

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_marking_external_python_task_as_setup(self, dag_maker, venv_python):
        @setup
        @task.external_python(python=venv_python)
        def f():
            return 1

        with dag_maker() as dag:
            ret = f()

        assert len(dag.task_group.children) == 1
        setup_task = dag.task_group.children["f"]
        assert setup_task.is_setup
        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_marking_external_python_task_as_teardown(self, dag_maker, venv_python):
        @teardown
        @task.external_python(python=venv_python)
        def f():
            return 1

        with dag_maker() as dag:
            ret = f()

        assert len(dag.task_group.children) == 1
        teardown_task = dag.task_group.children["f"]
        assert teardown_task.is_teardown
        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    @pytest.mark.parametrize("on_failure_fail_dagrun", [True, False])
    def test_marking_external_python_task_as_teardown_with_on_failure_fail(
        self, dag_maker, on_failure_fail_dagrun, venv_python
    ):
        @teardown(on_failure_fail_dagrun=on_failure_fail_dagrun)
        @task.external_python(python=venv_python)
        def f():
            return 1

        with dag_maker() as dag:
            ret = f()

        assert len(dag.task_group.children) == 1
        teardown_task = dag.task_group.children["f"]
        assert teardown_task.is_teardown
        assert teardown_task.on_failure_fail_dagrun is on_failure_fail_dagrun
        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

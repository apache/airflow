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
from importlib.util import find_spec
from subprocess import CalledProcessError

import pytest

from tests_common.test_utils.taskinstance import run_task_instance
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import setup, task, teardown
else:
    from airflow.decorators import setup, task, teardown  # type: ignore[attr-defined,no-redef]

try:
    from airflow.utils import timezone  # type: ignore[attr-defined]
except AttributeError:
    from airflow.sdk import timezone

pytestmark = pytest.mark.db_test


DEFAULT_DATE = timezone.datetime(2016, 1, 1)
END_DATE = timezone.datetime(2016, 1, 2)
INTERVAL = timedelta(hours=12)
FROZEN_NOW = timezone.datetime(2016, 1, 2, 12, 1, 1)
DILL_INSTALLED = find_spec("dill") is not None
DILL_MARKER = pytest.mark.skipif(not DILL_INSTALLED, reason="`dill` is not installed")
CLOUDPICKLE_INSTALLED = find_spec("cloudpickle") is not None
CLOUDPICKLE_MARKER = pytest.mark.skipif(not CLOUDPICKLE_INSTALLED, reason="`cloudpickle` is not installed")

TI_CONTEXT_ENV_VARS = [
    "AIRFLOW_CTX_DAG_ID",
    "AIRFLOW_CTX_TASK_ID",
    "AIRFLOW_CTX_LOGICAL_DATE",
    "AIRFLOW_CTX_DAG_RUN_ID",
]


@pytest.fixture(scope="module")
def venv_python(tmp_path_factory):
    venv_dir = tmp_path_factory.mktemp("venv")
    venv.create(venv_dir, with_pip=False)
    return (venv_dir / "bin" / "python").resolve(strict=True).as_posix()


@pytest.fixture(scope="module")
def venv_python_with_cloudpickle_and_dill(tmp_path_factory):
    venv_dir = tmp_path_factory.mktemp("venv_serializers")
    venv.create(venv_dir, with_pip=True)
    python_path = (venv_dir / "bin" / "python").resolve(strict=True).as_posix()
    subprocess.check_call([python_path, "-m", "pip", "install", "cloudpickle", "dill"])
    return python_path


class TestExternalPythonDecorator:
    @pytest.mark.parametrize(
        "serializer",
        [
            pytest.param("dill", marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
        ],
    )
    def test_with_serializer_works(self, serializer, dag_maker, venv_python_with_cloudpickle_and_dill):
        @task.external_python(python=venv_python_with_cloudpickle_and_dill, serializer=serializer)
        def f():
            """Import cloudpickle/dill to double-check it is installed ."""
            import cloudpickle  # noqa: F401
            import dill  # noqa: F401

        with dag_maker(serialized=True):
            v = f()

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instances()[0]
        run_task_instance(ti, v.operator)

    @pytest.mark.parametrize(
        "serializer",
        [
            pytest.param("dill", marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
        ],
    )
    def test_with_templated_python_serializer(
        self, serializer, dag_maker, venv_python_with_cloudpickle_and_dill
    ):
        # add template that produces empty string when rendered
        templated_python_with_cloudpickle = venv_python_with_cloudpickle_and_dill + "{{ '' }}"

        @task.external_python(python=templated_python_with_cloudpickle, serializer=serializer)
        def f():
            """Import cloudpickle/dill to double-check it is installed ."""
            import cloudpickle  # noqa: F401
            import dill  # noqa: F401

        with dag_maker(serialized=True):
            v = f()
        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instances()[0]
        run_task_instance(ti, v.operator)

    @pytest.mark.parametrize(
        "serializer",
        [
            pytest.param("dill", marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
        ],
    )
    def test_no_advanced_serializer_installed(self, serializer, dag_maker, venv_python):
        @task.external_python(python=venv_python, serializer=serializer)
        def f():
            pass

        with dag_maker(serialized=True):
            v = f()
        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instances()[0]

        with pytest.raises(CalledProcessError):
            run_task_instance(ti, v.operator)

    def test_exception_raises_error(self, dag_maker, venv_python):
        @task.external_python(python=venv_python)
        def f():
            raise Exception

        with dag_maker(serialized=True):
            v = f()
        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instances()[0]

        with pytest.raises(CalledProcessError):
            run_task_instance(ti, v.operator)

    @pytest.mark.parametrize(
        "serializer",
        [
            pytest.param("pickle", id="pickle"),
            pytest.param("dill", marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
            pytest.param(None, id="default"),
        ],
    )
    def test_with_args(self, serializer, dag_maker, venv_python_with_cloudpickle_and_dill):
        @task.external_python(python=venv_python_with_cloudpickle_and_dill, serializer=serializer)
        def f(a, b, c=False, d=False):
            if a == 0 and b == 1 and c and not d:
                return True
            raise Exception

        with dag_maker(serialized=True):
            v = f(0, 1, c=True)
        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instances()[0]
        run_task_instance(ti, v.operator)

    @pytest.mark.parametrize(
        "serializer",
        [
            pytest.param("pickle", id="pickle"),
            pytest.param("dill", marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
            pytest.param(None, id="default"),
        ],
    )
    def test_return_none(self, serializer, dag_maker, venv_python_with_cloudpickle_and_dill):
        @task.external_python(python=venv_python_with_cloudpickle_and_dill, serializer=serializer)
        def f():
            return None

        with dag_maker(serialized=True):
            v = f()

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instances()[0]
        run_task_instance(ti, v.operator)

    @pytest.mark.parametrize(
        "serializer",
        [
            pytest.param("pickle", id="pickle"),
            pytest.param("dill", marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
            pytest.param(None, id="default"),
        ],
    )
    def test_nonimported_as_arg(self, serializer, dag_maker, venv_python_with_cloudpickle_and_dill):
        @task.external_python(python=venv_python_with_cloudpickle_and_dill, serializer=serializer)
        def f(_):
            return None

        with dag_maker(serialized=True):
            v = f(datetime.datetime.now(tz=datetime.timezone.utc))

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instances()[0]
        run_task_instance(ti, v.operator)

    @pytest.mark.parametrize(
        "serializer",
        [
            pytest.param("pickle", id="pickle"),
            pytest.param("dill", marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
            pytest.param(None, id="default"),
        ],
    )
    def test_marking_external_python_task_as_setup(
        self, serializer, dag_maker, venv_python_with_cloudpickle_and_dill
    ):
        @setup
        @task.external_python(python=venv_python_with_cloudpickle_and_dill, serializer=serializer)
        def f():
            return 1

        with dag_maker(serialized=True) as dag:
            v = f()
        dr = dag_maker.create_dagrun()

        assert len(dag.task_group.children) == 1
        setup_task = dag.task_group.children["f"]
        assert setup_task.is_setup
        ti = dr.get_task_instances()[0]
        run_task_instance(ti, v.operator)

    @pytest.mark.parametrize(
        "serializer",
        [
            pytest.param("pickle", id="pickle"),
            pytest.param("dill", marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
            pytest.param(None, id="default"),
        ],
    )
    def test_marking_external_python_task_as_teardown(
        self, serializer, dag_maker, venv_python_with_cloudpickle_and_dill
    ):
        @teardown
        @task.external_python(python=venv_python_with_cloudpickle_and_dill, serializer=serializer)
        def f():
            return 1

        with dag_maker(serialized=True) as dag:
            v = f()
        dr = dag_maker.create_dagrun()

        assert len(dag.task_group.children) == 1
        teardown_task = dag.task_group.children["f"]
        assert teardown_task.is_teardown
        ti = dr.get_task_instances()[0]
        run_task_instance(ti, v.operator)

    @pytest.mark.parametrize(
        "serializer",
        [
            pytest.param("pickle", id="pickle"),
            pytest.param("dill", marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
            pytest.param(None, id="default"),
        ],
    )
    @pytest.mark.parametrize("on_failure_fail_dagrun", [True, False])
    def test_marking_external_python_task_as_teardown_with_on_failure_fail(
        self, serializer, dag_maker, on_failure_fail_dagrun, venv_python_with_cloudpickle_and_dill
    ):
        @teardown(on_failure_fail_dagrun=on_failure_fail_dagrun)
        @task.external_python(python=venv_python_with_cloudpickle_and_dill, serializer=serializer)
        def f():
            return 1

        with dag_maker(serialized=True) as dag:
            v = f()
        dr = dag_maker.create_dagrun()

        assert len(dag.task_group.children) == 1
        teardown_task = dag.task_group.children["f"]
        assert teardown_task.is_teardown
        assert teardown_task.on_failure_fail_dagrun is on_failure_fail_dagrun
        ti = dr.get_task_instances()[0]
        run_task_instance(ti, v.operator)

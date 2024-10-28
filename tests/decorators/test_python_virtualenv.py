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
import sys
from importlib.util import find_spec
from subprocess import CalledProcessError
from typing import Any

import pytest

from airflow.decorators import setup, task, teardown
from airflow.exceptions import RemovedInAirflow3Warning
from airflow.utils import timezone
from airflow.utils.state import TaskInstanceState

pytestmark = pytest.mark.db_test

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
PYTHON_VERSION = f"{sys.version_info.major}{sys.version_info.minor}"
DILL_INSTALLED = find_spec("dill") is not None
DILL_MARKER = pytest.mark.skipif(not DILL_INSTALLED, reason="`dill` is not installed")
CLOUDPICKLE_INSTALLED = find_spec("cloudpickle") is not None
CLOUDPICKLE_MARKER = pytest.mark.skipif(not CLOUDPICKLE_INSTALLED, reason="`cloudpickle` is not installed")

_Invalid = Any


@pytest.mark.execution_timeout(120)
class TestPythonVirtualenvDecorator:
    @CLOUDPICKLE_MARKER
    def test_add_cloudpickle(self, dag_maker):
        @task.virtualenv(serializer="cloudpickle", system_site_packages=False)
        def f():
            """Ensure cloudpickle is correctly installed."""
            import cloudpickle  # noqa: F401

        with dag_maker(serialized=True):
            ret = f()
        dag_maker.create_dagrun()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    @DILL_MARKER
    def test_add_dill(self, dag_maker):
        @task.virtualenv(serializer="dill", system_site_packages=False)
        def f():
            """Ensure dill is correctly installed."""
            import dill  # noqa: F401

        with dag_maker(serialized=True):
            ret = f()
        dag_maker.create_dagrun()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    @DILL_MARKER
    def test_add_dill_use_dill(self, dag_maker):
        @task.virtualenv(use_dill=True, system_site_packages=False)
        def f():
            """Ensure dill is correctly installed."""
            import dill  # noqa: F401

        with pytest.warns(RemovedInAirflow3Warning, match="`use_dill` is deprecated and will be removed"):
            with dag_maker(serialized=True):
                ret = f()
            dag_maker.create_dagrun()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_no_requirements(self, dag_maker):
        """Tests that the python callable is invoked on task run."""

        @task.virtualenv()
        def f():
            pass

        with dag_maker(serialized=True):
            ret = f()
        dag_maker.create_dagrun()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    @pytest.mark.parametrize(
        "serializer",
        [
            pytest.param("dill", marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
        ],
    )
    def test_no_system_site_packages(self, serializer, dag_maker):
        @task.virtualenv(system_site_packages=False, python_version=PYTHON_VERSION, serializer=serializer)
        def f():
            try:
                import funcsigs  # noqa: F401
            except ImportError:
                return True
            raise Exception

        with dag_maker(serialized=True):
            ret = f()
        dag_maker.create_dagrun()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    @pytest.mark.parametrize(
        "serializer",
        [
            pytest.param("dill", marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
        ],
    )
    def test_system_site_packages(self, serializer, dag_maker):
        @task.virtualenv(
            system_site_packages=False,
            requirements=["funcsigs"],
            python_version=PYTHON_VERSION,
            serializer=serializer,
        )
        def f():
            import funcsigs  # noqa: F401

        with dag_maker(serialized=True):
            ret = f()
        dag_maker.create_dagrun()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    @pytest.mark.parametrize(
        "serializer",
        [
            pytest.param("pickle", id="pickle"),
            pytest.param("dill", marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
            pytest.param(None, id="default"),
        ],
    )
    def test_with_requirements_pinned(self, serializer, dag_maker):
        @task.virtualenv(
            system_site_packages=False,
            requirements=["funcsigs==0.4"],
            python_version=PYTHON_VERSION,
            serializer=serializer,
        )
        def f():
            import funcsigs

            if funcsigs.__version__ != "0.4":
                raise Exception

        with dag_maker(serialized=True):
            ret = f()
        dag_maker.create_dagrun()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    @pytest.mark.parametrize(
        "serializer",
        [
            pytest.param("pickle", id="pickle"),
            pytest.param("dill", marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
            pytest.param(None, id="default"),
        ],
    )
    def test_with_requirements_file(self, serializer, dag_maker, tmp_path):
        requirements_file = tmp_path / "requirements.txt"
        requirements_file.write_text("funcsigs==0.4\nattrs==23.1.0")

        @task.virtualenv(
            system_site_packages=False,
            requirements="requirements.txt",
            python_version=PYTHON_VERSION,
            serializer=serializer,
        )
        def f():
            import funcsigs

            if funcsigs.__version__ != "0.4":
                raise Exception

            import attrs

            if attrs.__version__ != "23.1.0":
                raise Exception

        with dag_maker(template_searchpath=tmp_path.as_posix(), serialized=True):
            ret = f()
        dag_maker.create_dagrun()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    @pytest.mark.parametrize(
        "serializer, extra_requirements",
        [
            pytest.param("pickle", [], id="pickle"),
            pytest.param("dill", ["dill"], marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", ["cloudpickle"], marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
            pytest.param(None, [], id="default"),
        ],
    )
    def test_unpinned_requirements(self, serializer, extra_requirements, dag_maker):
        @task.virtualenv(
            system_site_packages=False,
            requirements=["funcsigs", *extra_requirements],
            python_version=PYTHON_VERSION,
            serializer=serializer,
        )
        def f():
            import funcsigs  # noqa: F401

        with dag_maker(serialized=True):
            ret = f()
        dag_maker.create_dagrun()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    @pytest.mark.parametrize(
        "serializer",
        [
            pytest.param("pickle", id="pickle"),
            pytest.param("dill", marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
            pytest.param(None, id="default"),
        ],
    )
    def test_fail(self, serializer, dag_maker):
        @task.virtualenv()
        def f():
            raise Exception

        with dag_maker(serialized=True):
            ret = f()
        dag_maker.create_dagrun()

        with pytest.raises(CalledProcessError):
            ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    @pytest.mark.parametrize(
        "serializer, extra_requirements",
        [
            pytest.param("pickle", [], id="pickle"),
            pytest.param("dill", ["dill"], marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", ["cloudpickle"], marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
            pytest.param(None, [], id="default"),
        ],
    )
    def test_python_3(self, serializer, extra_requirements, dag_maker):
        @task.virtualenv(python_version="3", serializer=serializer, requirements=extra_requirements)
        def f():
            import sys

            print(sys.version)
            try:
                {}.iteritems()
            except AttributeError:
                return
            raise Exception

        with dag_maker(serialized=True):
            ret = f()
        dag_maker.create_dagrun()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    @pytest.mark.parametrize(
        "serializer, extra_requirements",
        [
            pytest.param("pickle", [], id="pickle"),
            pytest.param("dill", ["dill"], marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", ["cloudpickle"], marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
            pytest.param(None, [], id="default"),
        ],
    )
    def test_with_args(self, serializer, extra_requirements, dag_maker):
        @task.virtualenv(serializer=serializer, requirements=extra_requirements)
        def f(a, b, c=False, d=False):
            if a == 0 and b == 1 and c and not d:
                return True
            else:
                raise Exception

        with dag_maker(serialized=True):
            ret = f(0, 1, c=True)
        dag_maker.create_dagrun()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_return_none(self, dag_maker):
        @task.virtualenv
        def f():
            return None

        with dag_maker(serialized=True):
            ret = f()
        dag_maker.create_dagrun()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_nonimported_as_arg(self, dag_maker):
        @task.virtualenv
        def f(_):
            return None

        with dag_maker(serialized=True):
            ret = f(datetime.datetime.now(tz=datetime.timezone.utc))
        dag_maker.create_dagrun()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_marking_virtualenv_python_task_as_setup(self, dag_maker):
        @setup
        @task.virtualenv
        def f():
            return 1

        with dag_maker(serialized=True) as dag:
            ret = f()
        dag_maker.create_dagrun()

        assert len(dag.task_group.children) == 1
        setup_task = dag.task_group.children["f"]
        assert setup_task.is_setup
        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_marking_virtualenv_python_task_as_teardown(self, dag_maker):
        @teardown
        @task.virtualenv
        def f():
            return 1

        with dag_maker(serialized=True) as dag:
            ret = f()
        dag_maker.create_dagrun()

        assert len(dag.task_group.children) == 1
        teardown_task = dag.task_group.children["f"]
        assert teardown_task.is_teardown
        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    @pytest.mark.parametrize("on_failure_fail_dagrun", [True, False])
    def test_marking_virtualenv_python_task_as_teardown_with_on_failure_fail(
        self, dag_maker, on_failure_fail_dagrun
    ):
        @teardown(on_failure_fail_dagrun=on_failure_fail_dagrun)
        @task.virtualenv
        def f():
            return 1

        with dag_maker(serialized=True) as dag:
            ret = f()
        dag_maker.create_dagrun()

        assert len(dag.task_group.children) == 1
        teardown_task = dag.task_group.children["f"]
        assert teardown_task.is_teardown
        assert teardown_task.on_failure_fail_dagrun is on_failure_fail_dagrun
        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_invalid_annotation(self, dag_maker):
        import uuid

        unique_id = uuid.uuid4().hex
        value = {"unique_id": unique_id}

        # Functions that throw an error
        # if `from __future__ import annotations` is missing
        @task.virtualenv(multiple_outputs=False, do_xcom_push=True)
        def in_venv(value: dict[str, _Invalid]) -> _Invalid:
            assert isinstance(value, dict)
            return value["unique_id"]

        with dag_maker(serialized=True):
            ret = in_venv(value)

        dr = dag_maker.create_dagrun()
        ret.operator.run(start_date=dr.execution_date, end_date=dr.execution_date)
        ti = dr.get_task_instances()[0]

        assert ti.state == TaskInstanceState.SUCCESS

        xcom = ti.xcom_pull(task_ids=ti.task_id, key="return_value")
        assert isinstance(xcom, str)
        assert xcom == unique_id

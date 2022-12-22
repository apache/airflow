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
from subprocess import CalledProcessError

import pytest

from airflow.decorators import task
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
PYTHON_VERSION = sys.version_info[0]


class TestPythonVirtualenvDecorator:
    def test_add_dill(self, dag_maker):
        @task.virtualenv(use_dill=True, system_site_packages=False)
        def f():
            """Ensure dill is correctly installed."""
            import dill  # noqa: F401

        with dag_maker():
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_no_requirements(self, dag_maker):
        """Tests that the python callable is invoked on task run."""

        @task.virtualenv()
        def f():
            pass

        with dag_maker():
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_no_system_site_packages(self, dag_maker):
        @task.virtualenv(system_site_packages=False, python_version=PYTHON_VERSION, use_dill=True)
        def f():
            try:
                import funcsigs  # noqa: F401
            except ImportError:
                return True
            raise Exception

        with dag_maker():
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_system_site_packages(self, dag_maker):
        @task.virtualenv(
            system_site_packages=False,
            requirements=["funcsigs"],
            python_version=PYTHON_VERSION,
            use_dill=True,
        )
        def f():
            import funcsigs  # noqa: F401

        with dag_maker():
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_with_requirements_pinned(self, dag_maker):
        @task.virtualenv(
            system_site_packages=False,
            requirements=["funcsigs==0.4"],
            python_version=PYTHON_VERSION,
            use_dill=True,
        )
        def f():
            import funcsigs

            if funcsigs.__version__ != "0.4":
                raise Exception

        with dag_maker():
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_unpinned_requirements(self, dag_maker):
        @task.virtualenv(
            system_site_packages=False,
            requirements=["funcsigs", "dill"],
            python_version=PYTHON_VERSION,
            use_dill=True,
        )
        def f():
            import funcsigs  # noqa: F401

        with dag_maker():
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_fail(self, dag_maker):
        @task.virtualenv()
        def f():
            raise Exception

        with dag_maker():
            ret = f()

        with pytest.raises(CalledProcessError):
            ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_python_3(self, dag_maker):
        @task.virtualenv(python_version=3, use_dill=False, requirements=["dill"])
        def f():
            import sys

            print(sys.version)
            try:
                {}.iteritems()
            except AttributeError:
                return
            raise Exception

        with dag_maker():
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_with_args(self, dag_maker):
        @task.virtualenv
        def f(a, b, c=False, d=False):
            if a == 0 and b == 1 and c and not d:
                return True
            else:
                raise Exception

        with dag_maker():
            ret = f(0, 1, c=True)

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_return_none(self, dag_maker):
        @task.virtualenv
        def f():
            return None

        with dag_maker():
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_nonimported_as_arg(self, dag_maker):
        @task.virtualenv
        def f(_):
            return None

        with dag_maker():
            ret = f(datetime.datetime.utcnow())

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

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

import datetime

import funcsigs
import sys
import unittest

from subprocess import CalledProcessError

from airflow import DAG
from airflow.operators.python_operator import PythonVirtualenvOperator
from airflow.utils import timezone

from airflow.exceptions import AirflowException

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
END_DATE = timezone.datetime(2016, 1, 2)
INTERVAL = datetime.timedelta(hours=12)
FROZEN_NOW = timezone.datetime(2016, 1, 2, 12, 1, 1)


class TestPythonVirtualenvOperator(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.dag = DAG(
            'test_dag',
            default_args={
                'owner': 'airflow',
                'start_date': DEFAULT_DATE},
            schedule_interval=INTERVAL)
        self.addCleanup(self.dag.clear)

    def _run_as_operator(self, fn, python_version=sys.version_info[0], **kwargs):
        task = PythonVirtualenvOperator(
            python_callable=fn,
            python_version=python_version,
            task_id='task',
            dag=self.dag,
            **kwargs)
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_dill_warning(self):
        def f():
            pass
        with self.assertRaises(AirflowException):
            PythonVirtualenvOperator(
                python_callable=f,
                task_id='task',
                dag=self.dag,
                use_dill=True,
                system_site_packages=False)

    def test_no_requirements(self):
        """Tests that the python callable is invoked on task run."""
        def f():
            pass
        self._run_as_operator(f)

    def test_no_system_site_packages(self):
        def f():
            try:
                import funcsigs  # noqa: F401
            except ImportError:
                return True
            raise Exception
        self._run_as_operator(f, system_site_packages=False, requirements=['dill'])

    def test_system_site_packages(self):
        def f():
            import funcsigs  # noqa: F401
        self._run_as_operator(f, requirements=['funcsigs'], system_site_packages=True)

    def test_with_requirements_pinned(self):
        self.assertNotEqual(
            '0.4', funcsigs.__version__, 'Please update this string if this fails')

        def f():
            import funcsigs  # noqa: F401
            if funcsigs.__version__ != '0.4':
                raise Exception

        self._run_as_operator(f, requirements=['funcsigs==0.4'])

    def test_unpinned_requirements(self):
        def f():
            import funcsigs  # noqa: F401
        self._run_as_operator(
            f, requirements=['funcsigs', 'dill'], system_site_packages=False)

    def test_range_requirements(self):
        def f():
            import funcsigs  # noqa: F401
        self._run_as_operator(
            f, requirements=['funcsigs>1.0', 'dill'], system_site_packages=False)

    def test_fail(self):
        def f():
            raise Exception
        with self.assertRaises(CalledProcessError):
            self._run_as_operator(f)

    def test_python_2(self):
        def f():
            {}.iteritems()
        self._run_as_operator(f, python_version=2, requirements=['dill'])

    def test_python_2_7(self):
        def f():
            {}.iteritems()
            return True
        self._run_as_operator(f, python_version='2.7', requirements=['dill'])

    def test_python_3(self):
        def f():
            import sys
            print(sys.version)
            try:
                {}.iteritems()
            except AttributeError:
                return
            raise Exception
        self._run_as_operator(f, python_version=3, use_dill=False, requirements=['dill'])

    @staticmethod
    def _invert_python_major_version():
        if sys.version_info[0] == 2:
            return 3
        else:
            return 2

    def test_wrong_python_op_args(self):
        if sys.version_info[0] == 2:
            version = 3
        else:
            version = 2

        def f():
            pass

        with self.assertRaises(AirflowException):
            self._run_as_operator(f, python_version=version, op_args=[1])

    def test_without_dill(self):
        def f(a):
            return a
        self._run_as_operator(f, system_site_packages=False, use_dill=False, op_args=[4])

    def test_string_args(self):
        def f():
            global virtualenv_string_args
            print(virtualenv_string_args)
            if virtualenv_string_args[0] != virtualenv_string_args[2]:
                raise Exception
        self._run_as_operator(
            f, python_version=self._invert_python_major_version(), string_args=[1, 2, 1])

    def test_with_args(self):
        def f(a, b, c=False, d=False):
            if a == 0 and b == 1 and c and not d:
                return True
            else:
                raise Exception
        self._run_as_operator(f, op_args=[0, 1], op_kwargs={'c': True})

    def test_return_none(self):
        def f():
            return None
        self._run_as_operator(f)

    def test_lambda(self):
        with self.assertRaises(AirflowException):
            PythonVirtualenvOperator(
                python_callable=lambda x: 4,
                task_id='task',
                dag=self.dag)

    def test_nonimported_as_arg(self):
        def f(_):
            return None
        self._run_as_operator(f, op_args=[datetime.datetime.utcnow()])

    def test_context(self):
        def f(templates_dict):
            return templates_dict['ds']
        self._run_as_operator(f, templates_dict={'ds': '{{ ds }}'})

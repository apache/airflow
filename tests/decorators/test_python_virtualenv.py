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
import sys
from datetime import timedelta
from subprocess import CalledProcessError

import pytest

from airflow.decorators import task
from airflow.utils import timezone

from .test_python import TestPythonBase

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
END_DATE = timezone.datetime(2016, 1, 2)
INTERVAL = timedelta(hours=12)
FROZEN_NOW = timezone.datetime(2016, 1, 2, 12, 1, 1)

TI_CONTEXT_ENV_VARS = [
    'AIRFLOW_CTX_DAG_ID',
    'AIRFLOW_CTX_TASK_ID',
    'AIRFLOW_CTX_EXECUTION_DATE',
    'AIRFLOW_CTX_DAG_RUN_ID',
]


PYTHON_VERSION = sys.version_info[0]


class TestPythonVirtualenvDecorator(TestPythonBase):
    def test_add_dill(self):
        @task.virtualenv(use_dill=True, system_site_packages=False)
        def f():
            pass

        with self.dag:
            ret = f()

        assert 'dill' in ret.operator.requirements

    def test_no_requirements(self):
        """Tests that the python callable is invoked on task run."""

        @task.virtualenv()
        def f():
            pass

        with self.dag:
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_no_system_site_packages(self):
        @task.virtualenv(system_site_packages=False, python_version=PYTHON_VERSION, use_dill=True)
        def f():
            try:
                import funcsigs  # noqa: F401  # pylint: disable=redefined-outer-name,reimported,unused-import
            except ImportError:
                return True
            raise Exception

        with self.dag:
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)  # pylint: disable=no-member

    def test_remove_task_decorator(self):
        from airflow.decorators.python_virtualenv import _PythonVirtualenvDecoratedOperator

        py_source = "@task.virtualenv(use_dill=True)\ndef f():\nimport funcsigs"
        res = _PythonVirtualenvDecoratedOperator._remove_task_decorator(python_source=py_source)
        assert res == "def f():\nimport funcsigs"

    def test_remove_decorator_no_parens(self):
        from airflow.decorators.python_virtualenv import _PythonVirtualenvDecoratedOperator

        py_source = "@task.virtualenv\ndef f():\nimport funcsigs"
        res = _PythonVirtualenvDecoratedOperator._remove_task_decorator(python_source=py_source)
        assert res == "def f():\nimport funcsigs"

    def test_remove_decorator_nested(self):
        from airflow.decorators.python_virtualenv import _PythonVirtualenvDecoratedOperator

        py_source = "@foo\n@task.virtualenv\n@bar\ndef f():\nimport funcsigs"
        res = _PythonVirtualenvDecoratedOperator._remove_task_decorator(python_source=py_source)
        assert res == "@foo\n@bar\ndef f():\nimport funcsigs"

        py_source = "@foo\n@task.virtualenv()\n@bar\ndef f():\nimport funcsigs"
        res = _PythonVirtualenvDecoratedOperator._remove_task_decorator(python_source=py_source)
        assert res == "@foo\n@bar\ndef f():\nimport funcsigs"

    def test_system_site_packages(self):
        @task.virtualenv(
            system_site_packages=False,
            requirements=['funcsigs'],
            python_version=PYTHON_VERSION,
            use_dill=True,
        )
        def f():
            import funcsigs  # noqa: F401  # pylint: disable=redefined-outer-name,reimported,unused-import

        with self.dag:
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_with_requirements_pinned(self):
        @task.virtualenv(
            system_site_packages=False,
            requirements=['funcsigs==0.4'],
            python_version=PYTHON_VERSION,
            use_dill=True,
        )
        def f():
            import funcsigs  # noqa: F401  # pylint: disable=redefined-outer-name,reimported

            if funcsigs.__version__ != '0.4':
                raise Exception

        with self.dag:
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_unpinned_requirements(self):
        @task.virtualenv(
            system_site_packages=False,
            requirements=['funcsigs', 'dill'],
            python_version=PYTHON_VERSION,
            use_dill=True,
        )
        def f():
            import funcsigs  # noqa: F401  # pylint: disable=redefined-outer-name,reimported,unused-import

        with self.dag:
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_fail(self):
        @task.virtualenv()
        def f():
            raise Exception

        with self.dag:
            ret = f()

        with pytest.raises(CalledProcessError):
            ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_python_2(self):
        @task.virtualenv(python_version=2, requirements=['dill'])
        def f():
            {}.iteritems()  # pylint: disable=no-member

        with self.dag:
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    # def test_python_2_7(self):
    #     def f():
    #         {}.iteritems()  # pylint: disable=no-member
    #         return True
    #
    #     self._run_as_operator(f, python_version='2.7', requirements=['dill'])
    #
    # def test_python_3(self):
    #     def f():
    #         import sys  # pylint: disable=reimported,unused-import,redefined-outer-name
    #
    #         print(sys.version)
    #         try:
    #             {}.iteritems()  # pylint: disable=no-member
    #         except AttributeError:
    #             return
    #         raise Exception
    #
    #     self._run_as_operator(f, python_version=3, use_dill=False, requirements=['dill'])
    #
    # @staticmethod
    # def _invert_python_major_version():
    #     if sys.version_info[0] == 2:
    #         return 3
    #     else:
    #         return 2
    #
    # def test_wrong_python_op_args(self):
    #     if sys.version_info[0] == 2:
    #         version = 3
    #     else:
    #         version = 2
    #
    #     def f():
    #         pass
    #
    #     with pytest.raises(AirflowException):
    #         self._run_as_operator(f, python_version=version, op_args=[1])
    #
    # def test_without_dill(self):
    #     def f(a):
    #         return a
    #
    #     self._run_as_operator(f, system_site_packages=False, use_dill=False, op_args=[4])
    #
    # def test_string_args(self):
    #     def f():
    #         global virtualenv_string_args  # pylint: disable=global-statement
    #         print(virtualenv_string_args)
    #         if virtualenv_string_args[0] != virtualenv_string_args[2]:
    #             raise Exception
    #
    #     self._run_as_operator(f, python_version=self._invert_python_major_version(), string_args=[1, 2, 1])
    #
    # def test_with_args(self):
    #     def f(a, b, c=False, d=False):
    #         if a == 0 and b == 1 and c and not d:
    #             return True
    #         else:
    #             raise Exception
    #
    #     self._run_as_operator(f, op_args=[0, 1], op_kwargs={'c': True})
    #
    # def test_return_none(self):
    #     def f():
    #         return None
    #
    #     self._run_as_operator(f)
    #
    # def test_lambda(self):
    #     with pytest.raises(AirflowException):
    #         PythonVirtualenvOperator(python_callable=lambda x: 4, task_id='task', dag=self.dag)
    #
    # def test_nonimported_as_arg(self):
    #     def f(_):
    #         return None
    #
    #     self._run_as_operator(f, op_args=[datetime.utcnow()])
    #
    # def test_context(self):
    #     def f(templates_dict):
    #         return templates_dict['ds']
    #
    #     self._run_as_operator(f, templates_dict={'ds': '{{ ds }}'})
    #
    # def test_airflow_context(self):
    #     def f(
    #         # basic
    #         ds_nodash,
    #         inlets,
    #         next_ds,
    #         next_ds_nodash,
    #         outlets,
    #         params,
    #         prev_ds,
    #         prev_ds_nodash,
    #         run_id,
    #         task_instance_key_str,
    #         test_mode,
    #         tomorrow_ds,
    #         tomorrow_ds_nodash,
    #         ts,
    #         ts_nodash,
    #         ts_nodash_with_tz,
    #         yesterday_ds,
    #         yesterday_ds_nodash,
    #         # pendulum-specific
    #         execution_date,
    #         next_execution_date,
    #         prev_execution_date,
    #         prev_execution_date_success,
    #         prev_start_date_success,
    #         # airflow-specific
    #         macros,
    #         conf,
    #         dag,
    #         dag_run,
    #         task,
    #         # other
    #         **context,
    #     ):  # pylint: disable=unused-argument,too-many-arguments,too-many-locals
    #         pass
    #
    #     self._run_as_operator(f, use_dill=True, system_site_packages=True, requirements=None)
    #
    # def test_pendulum_context(self):
    #     def f(
    #         # basic
    #         ds_nodash,
    #         inlets,
    #         next_ds,
    #         next_ds_nodash,
    #         outlets,
    #         params,
    #         prev_ds,
    #         prev_ds_nodash,
    #         run_id,
    #         task_instance_key_str,
    #         test_mode,
    #         tomorrow_ds,
    #         tomorrow_ds_nodash,
    #         ts,
    #         ts_nodash,
    #         ts_nodash_with_tz,
    #         yesterday_ds,
    #         yesterday_ds_nodash,
    #         # pendulum-specific
    #         execution_date,
    #         next_execution_date,
    #         prev_execution_date,
    #         prev_execution_date_success,
    #         prev_start_date_success,
    #         # other
    #         **context,
    #     ):  # pylint: disable=unused-argument,too-many-arguments,too-many-locals
    #         pass
    #
    #     self._run_as_operator(
    #         f, use_dill=True, system_site_packages=False, requirements=['pendulum', 'lazy_object_proxy']
    #     )
    #
    # def test_base_context(self):
    #     @task.virtualenv(
    #         system_site_packages=False,
    #         requirements=['funcsigs', 'dill'],
    #         python_version=PYTHON_VERSION,
    #         use_dill=True,
    #     )
    #     def f(
    #         # basic
    #         ds_nodash,
    #         inlets,
    #         next_ds,
    #         next_ds_nodash,
    #         outlets,
    #         params,
    #         prev_ds,
    #         prev_ds_nodash,
    #         run_id,
    #         task_instance_key_str,
    #         test_mode,
    #         tomorrow_ds,
    #         tomorrow_ds_nodash,
    #         ts,
    #         ts_nodash,
    #         ts_nodash_with_tz,
    #         yesterday_ds,
    #         yesterday_ds_nodash,
    #         # other
    #         **context,
    #     ):  # pylint: disable=unused-argument,too-many-arguments,too-many-locals
    #         pass
    #
    #     with self.dag:
    #         ret = f()
    #
    #     ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
    #     # self._run_as_operator(f, use_dill=True, system_site_packages=False, requirements=None)

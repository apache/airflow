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

import inspect
import os
import pickle
import shutil
import subprocess
import sys
import types
import warnings
from abc import ABCMeta, abstractmethod
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import Any, Callable, Collection, Iterable, Mapping, Sequence

import dill

from airflow.exceptions import AirflowConfigException, AirflowException, RemovedInAirflow3Warning
from airflow.models.baseoperator import BaseOperator
from airflow.models.skipmixin import SkipMixin
from airflow.models.taskinstance import _CURRENT_CONTEXT
from airflow.utils.context import Context, context_copy_partial, context_merge
from airflow.utils.operator_helpers import KeywordParameters
from airflow.utils.process_utils import execute_in_subprocess
from airflow.utils.python_virtualenv import prepare_virtualenv, write_python_script


def task(python_callable: Callable | None = None, multiple_outputs: bool | None = None, **kwargs):
    """
    Deprecated function.
    Calls @task.python and allows users to turn a python function into
    an Airflow task. Please use the following instead:

    from airflow.decorators import task

    @task
    def my_task()

    :param python_callable: A reference to an object that is callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function (templated)
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable (templated)
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. Dict will unroll to xcom values with keys as keys.
        Defaults to False.
    :return:
    """
    # To maintain backwards compatibility, we import the task object into this file
    # This prevents breakages in dags that use `from airflow.operators.python import task`
    from airflow.decorators.python import python_task

    warnings.warn(
        """airflow.operators.python.task is deprecated. Please use the following instead

        from airflow.decorators import task
        @task
        def my_task()""",
        RemovedInAirflow3Warning,
        stacklevel=2,
    )
    return python_task(python_callable=python_callable, multiple_outputs=multiple_outputs, **kwargs)


class PythonOperator(BaseOperator):
    """
    Executes a Python callable.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PythonOperator`

    When running your callable, Airflow will pass a set of keyword arguments that can be used in your
    function. This set of kwargs correspond exactly to what you can use in your jinja templates.
    For this to work, you need to define ``**kwargs`` in your function header, or you can add directly the
    keyword arguments you would like to get - for example with the below code your callable will get
    the values of ``ti`` and ``next_ds`` context variables.

    With explicit arguments:

    .. code-block:: python

       def my_python_callable(ti, next_ds):
           pass

    With kwargs:

    .. code-block:: python

       def my_python_callable(**kwargs):
           ti = kwargs["ti"]
           next_ds = kwargs["next_ds"]


    :param python_callable: A reference to an object that is callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable
    :param templates_dict: a dictionary where the values are templates that
        will get templated by the Airflow engine sometime between
        ``__init__`` and ``execute`` takes place and are made available
        in your callable's context after the template has been applied. (templated)
    :param templates_exts: a list of file extensions to resolve while
        processing templated fields, for examples ``['.sql', '.hql']``
    :param show_return_value_in_logs: a bool value whether to show return_value
        logs. Defaults to True, which allows return value log output.
        It can be set to False to prevent log output of return value when you return huge data
        such as transmission a large amount of XCom to TaskAPI.
    """

    template_fields: Sequence[str] = ("templates_dict", "op_args", "op_kwargs")
    template_fields_renderers = {"templates_dict": "json", "op_args": "py", "op_kwargs": "py"}
    BLUE = "#ffefeb"
    ui_color = BLUE

    # since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects(e.g protobuf).
    shallow_copy_attrs: Sequence[str] = (
        "python_callable",
        "op_kwargs",
    )

    def __init__(
        self,
        *,
        python_callable: Callable,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        templates_dict: dict[str, Any] | None = None,
        templates_exts: Sequence[str] | None = None,
        show_return_value_in_logs: bool = True,
        **kwargs,
    ) -> None:
        if kwargs.get("provide_context"):
            warnings.warn(
                "provide_context is deprecated as of 2.0 and is no longer required",
                RemovedInAirflow3Warning,
                stacklevel=2,
            )
            kwargs.pop("provide_context", None)
        super().__init__(**kwargs)
        if not callable(python_callable):
            raise AirflowException("`python_callable` param must be callable")
        self.python_callable = python_callable
        self.op_args = op_args or ()
        self.op_kwargs = op_kwargs or {}
        self.templates_dict = templates_dict
        if templates_exts:
            self.template_ext = templates_exts
        self.show_return_value_in_logs = show_return_value_in_logs

    def execute(self, context: Context) -> Any:
        context_merge(context, self.op_kwargs, templates_dict=self.templates_dict)
        self.op_kwargs = self.determine_kwargs(context)

        return_value = self.execute_callable()
        if self.show_return_value_in_logs:
            self.log.info("Done. Returned value was: %s", return_value)
        else:
            self.log.info("Done. Returned value not shown")

        return return_value

    def determine_kwargs(self, context: Mapping[str, Any]) -> Mapping[str, Any]:
        return KeywordParameters.determine(self.python_callable, self.op_args, context).unpacking()

    def execute_callable(self) -> Any:
        """
        Calls the python callable with the given arguments.

        :return: the return value of the call.
        """
        return self.python_callable(*self.op_args, **self.op_kwargs)


class BranchPythonOperator(PythonOperator, SkipMixin):
    """
    A workflow can "branch" or follow a path after the execution of this task.

    It derives the PythonOperator and expects a Python function that returns
    a single task_id or list of task_ids to follow. The task_id(s) returned
    should point to a task directly downstream from {self}. All other "branches"
    or directly downstream tasks are marked with a state of ``skipped`` so that
    these paths can't move forward. The ``skipped`` states are propagated
    downstream to allow for the DAG state to fill up and the DAG run's state
    to be inferred.
    """

    def execute(self, context: Context) -> Any:
        branch = super().execute(context)
        self.log.info("Branch callable return %s", branch)
        self.skip_all_except(context["ti"], branch)
        return branch


class ShortCircuitOperator(PythonOperator, SkipMixin):
    """
    Allows a pipeline to continue based on the result of a ``python_callable``.

    The ShortCircuitOperator is derived from the PythonOperator and evaluates the result of a
    ``python_callable``. If the returned result is False or a falsy value, the pipeline will be
    short-circuited. Downstream tasks will be marked with a state of "skipped" based on the short-circuiting
    mode configured. If the returned result is True or a truthy value, downstream tasks proceed as normal and
    an ``XCom`` of the returned result is pushed.

    The short-circuiting can be configured to either respect or ignore the ``trigger_rule`` set for
    downstream tasks. If ``ignore_downstream_trigger_rules`` is set to True, the default setting, all
    downstream tasks are skipped without considering the ``trigger_rule`` defined for tasks. However, if this
    parameter is set to False, the direct downstream tasks are skipped but the specified ``trigger_rule`` for
    other subsequent downstream tasks are respected. In this mode, the operator assumes the direct downstream
    tasks were purposely meant to be skipped but perhaps not other subsequent tasks.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ShortCircuitOperator`

    :param ignore_downstream_trigger_rules: If set to True, all downstream tasks from this operator task will
        be skipped. This is the default behavior. If set to False, the direct, downstream task(s) will be
        skipped but the ``trigger_rule`` defined for a other downstream tasks will be respected.
    """

    def __init__(self, *, ignore_downstream_trigger_rules: bool = True, **kwargs) -> None:
        super().__init__(**kwargs)
        self.ignore_downstream_trigger_rules = ignore_downstream_trigger_rules

    def execute(self, context: Context) -> Any:
        condition = super().execute(context)
        self.log.info("Condition result is %s", condition)

        if condition:
            self.log.info("Proceeding with downstream tasks...")
            return condition

        downstream_tasks = context["task"].get_flat_relatives(upstream=False)
        self.log.debug("Downstream task IDs %s", downstream_tasks)

        if downstream_tasks:
            dag_run = context["dag_run"]
            execution_date = dag_run.execution_date

            if self.ignore_downstream_trigger_rules is True:
                self.log.info("Skipping all downstream tasks...")
                self.skip(dag_run, execution_date, downstream_tasks)
            else:
                self.log.info("Skipping downstream tasks while respecting trigger rules...")
                # Explicitly setting the state of the direct, downstream task(s) to "skipped" and letting the
                # Scheduler handle the remaining downstream task(s) appropriately.
                self.skip(dag_run, execution_date, context["task"].get_direct_relatives(upstream=False))

        self.log.info("Done.")


class _BasePythonVirtualenvOperator(PythonOperator, metaclass=ABCMeta):
    BASE_SERIALIZABLE_CONTEXT_KEYS = {
        "ds",
        "ds_nodash",
        "expanded_ti_count",
        "inlets",
        "next_ds",
        "next_ds_nodash",
        "outlets",
        "prev_ds",
        "prev_ds_nodash",
        "run_id",
        "task_instance_key_str",
        "test_mode",
        "tomorrow_ds",
        "tomorrow_ds_nodash",
        "ts",
        "ts_nodash",
        "ts_nodash_with_tz",
        "yesterday_ds",
        "yesterday_ds_nodash",
    }
    PENDULUM_SERIALIZABLE_CONTEXT_KEYS = {
        "data_interval_end",
        "data_interval_start",
        "execution_date",
        "logical_date",
        "next_execution_date",
        "prev_data_interval_end_success",
        "prev_data_interval_start_success",
        "prev_execution_date",
        "prev_execution_date_success",
        "prev_start_date_success",
    }
    AIRFLOW_SERIALIZABLE_CONTEXT_KEYS = {
        "macros",
        "conf",
        "dag",
        "dag_run",
        "task",
        "params",
        "triggering_dataset_events",
    }

    def __init__(
        self,
        *,
        python_callable: Callable,
        use_dill: bool = False,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        string_args: Iterable[str] | None = None,
        templates_dict: dict | None = None,
        templates_exts: list[str] | None = None,
        expect_airflow: bool = True,
        **kwargs,
    ):
        if (
            not isinstance(python_callable, types.FunctionType)
            or isinstance(python_callable, types.LambdaType)
            and python_callable.__name__ == "<lambda>"
        ):
            raise AirflowException("PythonVirtualenvOperator only supports functions for python_callable arg")
        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            **kwargs,
        )
        self.string_args = string_args or []
        self.use_dill = use_dill
        self.pickling_library = dill if self.use_dill else pickle
        self.expect_airflow = expect_airflow

    @abstractmethod
    def _iter_serializable_context_keys(self):
        pass

    def execute(self, context: Context) -> Any:
        serializable_keys = set(self._iter_serializable_context_keys())
        serializable_context = context_copy_partial(context, serializable_keys)
        return super().execute(context=serializable_context)

    def get_python_source(self):
        """Return the source of self.python_callable."""
        return dedent(inspect.getsource(self.python_callable))

    def _write_args(self, file: Path):
        if self.op_args or self.op_kwargs:
            file.write_bytes(self.pickling_library.dumps({"args": self.op_args, "kwargs": self.op_kwargs}))

    def _write_string_args(self, file: Path):
        file.write_text("\n".join(map(str, self.string_args)))

    def _read_result(self, path: Path):
        if path.stat().st_size == 0:
            return None
        try:
            return self.pickling_library.loads(path.read_bytes())
        except ValueError:
            self.log.error(
                "Error deserializing result. Note that result deserialization "
                "is not supported across major Python versions."
            )
            raise

    def __deepcopy__(self, memo):
        # module objects can't be copied _at all__
        memo[id(self.pickling_library)] = self.pickling_library
        return super().__deepcopy__(memo)

    def _execute_python_callable_in_subprocess(self, python_path: Path, tmp_dir: Path):
        op_kwargs: dict[str, Any] = {k: v for k, v in self.op_kwargs.items()}
        if self.templates_dict:
            op_kwargs["templates_dict"] = self.templates_dict
        input_path = tmp_dir / "script.in"
        output_path = tmp_dir / "script.out"
        string_args_path = tmp_dir / "string_args.txt"
        script_path = tmp_dir / "script.py"
        self._write_args(input_path)
        self._write_string_args(string_args_path)
        write_python_script(
            jinja_context=dict(
                op_args=self.op_args,
                op_kwargs=op_kwargs,
                expect_airflow=self.expect_airflow,
                pickling_library=self.pickling_library.__name__,
                python_callable=self.python_callable.__name__,
                python_callable_source=self.get_python_source(),
            ),
            filename=os.fspath(script_path),
            render_template_as_native_obj=self.dag.render_template_as_native_obj,
        )

        execute_in_subprocess(
            cmd=[
                os.fspath(python_path),
                os.fspath(script_path),
                os.fspath(input_path),
                os.fspath(output_path),
                os.fspath(string_args_path),
            ]
        )
        return self._read_result(output_path)

    def determine_kwargs(self, context: Mapping[str, Any]) -> Mapping[str, Any]:
        return KeywordParameters.determine(self.python_callable, self.op_args, context).serializing()


class PythonVirtualenvOperator(_BasePythonVirtualenvOperator):
    """
    Run a function in a virtualenv that is created and destroyed automatically.

    The function (has certain caveats) must be defined using def, and not be
    part of a class. All imports must happen inside the function
    and no variables outside the scope may be referenced. A global scope
    variable named virtualenv_string_args will be available (populated by
    string_args). In addition, one can pass stuff through op_args and op_kwargs, and one
    can use a return value.
    Note that if your virtualenv runs in a different Python major version than Airflow,
    you cannot use return values, op_args, op_kwargs, or use any macros that are being provided to
    Airflow through plugins. You can use string_args though.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PythonVirtualenvOperator`

    :param python_callable: A python function with no references to outside variables,
        defined with def, which will be run in a virtualenv
    :param requirements: Either a list of requirement strings, or a (templated)
        "requirements file" as specified by pip.
    :param python_version: The Python version to run the virtualenv with. Note that
        both 2 and 2.7 are acceptable forms.
    :param use_dill: Whether to use dill to serialize
        the args and result (pickle is default). This allow more complex types
        but requires you to include dill in your requirements.
    :param system_site_packages: Whether to include
        system_site_packages in your virtualenv.
        See virtualenv documentation for more information.
    :param pip_install_options: a list of pip install options when installing requirements
        See 'pip install -h' for available options
    :param op_args: A list of positional arguments to pass to python_callable.
    :param op_kwargs: A dict of keyword arguments to pass to python_callable.
    :param string_args: Strings that are present in the global var virtualenv_string_args,
        available to python_callable at runtime as a list[str]. Note that args are split
        by newline.
    :param templates_dict: a dictionary where the values are templates that
        will get templated by the Airflow engine sometime between
        ``__init__`` and ``execute`` takes place and are made available
        in your callable's context after the template has been applied
    :param templates_exts: a list of file extensions to resolve while
        processing templated fields, for examples ``['.sql', '.hql']``
    :param expect_airflow: expect Airflow to be installed in the target environment. If true, the operator
        will raise warning if Airflow is not installed, and it will attempt to load Airflow
        macros when starting.
    """

    template_fields: Sequence[str] = tuple({"requirements"} | set(PythonOperator.template_fields))
    template_ext: Sequence[str] = (".txt",)

    def __init__(
        self,
        *,
        python_callable: Callable,
        requirements: None | Iterable[str] | str = None,
        python_version: str | int | float | None = None,
        use_dill: bool = False,
        system_site_packages: bool = True,
        pip_install_options: list[str] | None = None,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        string_args: Iterable[str] | None = None,
        templates_dict: dict | None = None,
        templates_exts: list[str] | None = None,
        expect_airflow: bool = True,
        **kwargs,
    ):
        if (
            python_version
            and str(python_version)[0] != str(sys.version_info.major)
            and (op_args or op_kwargs)
        ):
            raise AirflowException(
                "Passing op_args or op_kwargs is not supported across different Python "
                "major versions for PythonVirtualenvOperator. Please use string_args."
                f"Sys version: {sys.version_info}. Venv version: {python_version}"
            )
        if not shutil.which("virtualenv"):
            raise AirflowException("PythonVirtualenvOperator requires virtualenv, please install it.")
        if not requirements:
            self.requirements: list[str] | str = []
        elif isinstance(requirements, str):
            self.requirements = requirements
        else:
            self.requirements = list(requirements)
        self.python_version = python_version
        self.system_site_packages = system_site_packages
        self.pip_install_options = pip_install_options
        super().__init__(
            python_callable=python_callable,
            use_dill=use_dill,
            op_args=op_args,
            op_kwargs=op_kwargs,
            string_args=string_args,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            expect_airflow=expect_airflow,
            **kwargs,
        )

    def execute_callable(self):
        with TemporaryDirectory(prefix="venv") as tmp_dir:
            tmp_path = Path(tmp_dir)
            requirements_file_name = f"{tmp_dir}/requirements.txt"

            if not isinstance(self.requirements, str):
                requirements_file_contents = "\n".join(str(dependency) for dependency in self.requirements)
            else:
                requirements_file_contents = self.requirements

            if not self.system_site_packages and self.use_dill:
                requirements_file_contents += "\ndill"

            with open(requirements_file_name, "w") as file:
                file.write(requirements_file_contents)
            prepare_virtualenv(
                venv_directory=tmp_dir,
                python_bin=f"python{self.python_version}" if self.python_version else None,
                system_site_packages=self.system_site_packages,
                requirements_file_path=requirements_file_name,
                pip_install_options=self.pip_install_options,
            )
            python_path = tmp_path / "bin" / "python"

            return self._execute_python_callable_in_subprocess(python_path, tmp_path)

    def _iter_serializable_context_keys(self):
        yield from self.BASE_SERIALIZABLE_CONTEXT_KEYS
        if self.system_site_packages or "apache-airflow" in self.requirements:
            yield from self.AIRFLOW_SERIALIZABLE_CONTEXT_KEYS
            yield from self.PENDULUM_SERIALIZABLE_CONTEXT_KEYS
        elif "pendulum" in self.requirements:
            yield from self.PENDULUM_SERIALIZABLE_CONTEXT_KEYS


class ExternalPythonOperator(_BasePythonVirtualenvOperator):
    """
    Run a function in a virtualenv that is not re-created.

    Reused as is without the overhead of creating the virtualenv (with certain caveats).

    The function must be defined using def, and not be
    part of a class. All imports must happen inside the function
    and no variables outside the scope may be referenced. A global scope
    variable named virtualenv_string_args will be available (populated by
    string_args). In addition, one can pass stuff through op_args and op_kwargs, and one
    can use a return value.
    Note that if your virtualenv runs in a different Python major version than Airflow,
    you cannot use return values, op_args, op_kwargs, or use any macros that are being provided to
    Airflow through plugins. You can use string_args though.

    If Airflow is installed in the external environment in different version that the version
    used by the operator, the operator will fail.,

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ExternalPythonOperator`

    :param python: Full path string (file-system specific) that points to a Python binary inside
        a virtualenv that should be used (in ``VENV/bin`` folder). Should be absolute path
        (so usually start with "/" or "X:/" depending on the filesystem/os used).
    :param python_callable: A python function with no references to outside variables,
        defined with def, which will be run in a virtualenv
    :param use_dill: Whether to use dill to serialize
        the args and result (pickle is default). This allow more complex types
        but if dill is not preinstalled in your venv, the task will fail with use_dill enabled.
    :param op_args: A list of positional arguments to pass to python_callable.
    :param op_kwargs: A dict of keyword arguments to pass to python_callable.
    :param string_args: Strings that are present in the global var virtualenv_string_args,
        available to python_callable at runtime as a list[str]. Note that args are split
        by newline.
    :param templates_dict: a dictionary where the values are templates that
        will get templated by the Airflow engine sometime between
        ``__init__`` and ``execute`` takes place and are made available
        in your callable's context after the template has been applied
    :param templates_exts: a list of file extensions to resolve while
        processing templated fields, for examples ``['.sql', '.hql']``
    :param expect_airflow: expect Airflow to be installed in the target environment. If true, the operator
        will raise warning if Airflow is not installed, and it will attempt to load Airflow
        macros when starting.
    """

    template_fields: Sequence[str] = tuple({"python"} | set(PythonOperator.template_fields))

    def __init__(
        self,
        *,
        python: str,
        python_callable: Callable,
        use_dill: bool = False,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        string_args: Iterable[str] | None = None,
        templates_dict: dict | None = None,
        templates_exts: list[str] | None = None,
        expect_airflow: bool = True,
        expect_pendulum: bool = False,
        **kwargs,
    ):
        if not python:
            raise ValueError("Python Path must be defined in ExternalPythonOperator")
        self.python = python
        self.expect_pendulum = expect_pendulum
        super().__init__(
            python_callable=python_callable,
            use_dill=use_dill,
            op_args=op_args,
            op_kwargs=op_kwargs,
            string_args=string_args,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            expect_airflow=expect_airflow,
            **kwargs,
        )

    def execute_callable(self):
        python_path = Path(self.python)
        if not python_path.exists():
            raise ValueError(f"Python Path '{python_path}' must exists")
        if not python_path.is_file():
            raise ValueError(f"Python Path '{python_path}' must be a file")
        if not python_path.is_absolute():
            raise ValueError(f"Python Path '{python_path}' must be an absolute path.")
        python_version_as_list_of_strings = self._get_python_version_from_environment()
        if (
            python_version_as_list_of_strings
            and str(python_version_as_list_of_strings[0]) != str(sys.version_info.major)
            and (self.op_args or self.op_kwargs)
        ):
            raise AirflowException(
                "Passing op_args or op_kwargs is not supported across different Python "
                "major versions for ExternalPythonOperator. Please use string_args."
                f"Sys version: {sys.version_info}. Venv version: {python_version_as_list_of_strings}"
            )
        with TemporaryDirectory(prefix="tmd") as tmp_dir:
            tmp_path = Path(tmp_dir)
            return self._execute_python_callable_in_subprocess(python_path, tmp_path)

    def _get_python_version_from_environment(self) -> list[str]:
        try:
            result = subprocess.check_output([self.python, "--version"], text=True)
            return result.strip().split(" ")[-1].split(".")
        except Exception as e:
            raise ValueError(f"Error while executing {self.python}: {e}")

    def _iter_serializable_context_keys(self):
        yield from self.BASE_SERIALIZABLE_CONTEXT_KEYS
        if self._get_airflow_version_from_target_env():
            yield from self.AIRFLOW_SERIALIZABLE_CONTEXT_KEYS
            yield from self.PENDULUM_SERIALIZABLE_CONTEXT_KEYS
        elif self._is_pendulum_installed_in_target_env():
            yield from self.PENDULUM_SERIALIZABLE_CONTEXT_KEYS

    def _is_pendulum_installed_in_target_env(self) -> bool:
        try:
            subprocess.check_call([self.python, "-c", "import pendulum"])
            return True
        except Exception as e:
            if self.expect_pendulum:
                self.log.warning("When checking for Pendulum installed in venv got %s", e)
                self.log.warning(
                    "Pendulum is not properly installed in the virtualenv "
                    "Pendulum context keys will not be available. "
                    "Please Install Pendulum or Airflow in your venv to access them."
                )
            return False

    def _get_airflow_version_from_target_env(self) -> str | None:
        from airflow import __version__ as airflow_version

        try:
            result = subprocess.check_output(
                [self.python, "-c", "from airflow import __version__; print(__version__)"], text=True
            )
            target_airflow_version = result.strip()
            if target_airflow_version != airflow_version:
                raise AirflowConfigException(
                    f"The version of Airflow installed for the {self.python}("
                    f"{target_airflow_version}) is different than the runtime Airflow version: "
                    f"{airflow_version}. Make sure your environment has the same Airflow version "
                    f"installed as the Airflow runtime."
                )
            return target_airflow_version
        except Exception as e:
            if self.expect_airflow:
                self.log.warning("When checking for Airflow installed in venv got %s", e)
                self.log.warning(
                    f"This means that Airflow is not properly installed by  "
                    f"{self.python}. Airflow context keys will not be available. "
                    f"Please Install Airflow {airflow_version} in your environment to access them."
                )
            return None


def get_current_context() -> Context:
    """
    Retrieve the execution context dictionary without altering user method's signature.
    This is the simplest method of retrieving the execution context dictionary.

    **Old style:**

    .. code:: python

        def my_task(**context):
            ti = context["ti"]

    **New style:**

    .. code:: python

        from airflow.operators.python import get_current_context


        def my_task():
            context = get_current_context()
            ti = context["ti"]

    Current context will only have value if this method was called after an operator
    was starting to execute.
    """
    if not _CURRENT_CONTEXT:
        raise AirflowException(
            "Current context was requested but no context was found! "
            "Are you running within an airflow task?"
        )
    return _CURRENT_CONTEXT[-1]

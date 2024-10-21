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

import importlib
import inspect
import json
import logging
import os
import shutil
import subprocess
import sys
import textwrap
import types
import warnings
from abc import ABCMeta, abstractmethod
from collections.abc import Container
from functools import cache
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Collection, Iterable, Mapping, NamedTuple, Sequence

import lazy_object_proxy

from airflow.exceptions import (
    AirflowConfigException,
    AirflowException,
    AirflowSkipException,
    DeserializingResultError,
    RemovedInAirflow3Warning,
)
from airflow.models.baseoperator import BaseOperator
from airflow.models.skipmixin import SkipMixin
from airflow.models.taskinstance import _CURRENT_CONTEXT
from airflow.models.variable import Variable
from airflow.operators.branch import BranchMixIn
from airflow.settings import _ENABLE_AIP_44
from airflow.typing_compat import Literal
from airflow.utils import hashlib_wrapper
from airflow.utils.context import context_copy_partial, context_get_outlet_events, context_merge
from airflow.utils.file import get_unique_dag_module_name
from airflow.utils.operator_helpers import ExecutionCallableRunner, KeywordParameters
from airflow.utils.process_utils import execute_in_subprocess
from airflow.utils.python_virtualenv import prepare_virtualenv, write_python_script
from airflow.utils.session import create_session

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from airflow.serialization.enums import Encoding
    from airflow.utils.context import Context


def is_venv_installed() -> bool:
    """
    Check if the virtualenv package is installed via checking if it is on the path or installed as package.

    :return: True if it is. Whichever way of checking it works, is fine.
    """
    if shutil.which("virtualenv") or importlib.util.find_spec("virtualenv"):
        return True
    return False


def task(python_callable: Callable | None = None, multiple_outputs: bool | None = None, **kwargs):
    """
    Use :func:`airflow.decorators.task` instead, this is deprecated.

    Calls ``@task.python`` and allows users to turn a Python function into
    an Airflow task.

    :param python_callable: A reference to an object that is callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function (templated)
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable (templated)
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. Dict will unroll to xcom values with keys as keys.
        Defaults to False.
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


@cache
def _parse_version_info(text: str) -> tuple[int, int, int, str, int]:
    """Parse python version info from a text."""
    parts = text.strip().split(".")
    if len(parts) != 5:
        msg = f"Invalid Python version info, expected 5 components separated by '.', but got {text!r}."
        raise ValueError(msg)
    try:
        return int(parts[0]), int(parts[1]), int(parts[2]), parts[3], int(parts[4])
    except ValueError:
        msg = f"Unable to convert parts {parts} parsed from {text!r} to (int, int, int, str, int)."
        raise ValueError(msg) from None


class _PythonVersionInfo(NamedTuple):
    """Provide the same interface as ``sys.version_info``."""

    major: int
    minor: int
    micro: int
    releaselevel: str
    serial: int

    @classmethod
    def from_executable(cls, executable: str) -> _PythonVersionInfo:
        """Parse python version info from an executable."""
        cmd = [executable, "-c", 'import sys; print(".".join(map(str, sys.version_info)))']
        try:
            result = subprocess.check_output(cmd, text=True)
        except Exception as e:
            raise ValueError(f"Error while executing command {cmd}: {e}")
        return cls(*_parse_version_info(result.strip()))


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
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function
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
    shallow_copy_attrs: ClassVar[Sequence[str]] = (
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
        self._asset_events = context_get_outlet_events(context)

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
        Call the python callable with the given arguments.

        :return: the return value of the call.
        """
        runner = ExecutionCallableRunner(self.python_callable, self._asset_events, logger=self.log)
        return runner.run(*self.op_args, **self.op_kwargs)


class BranchPythonOperator(PythonOperator, BranchMixIn):
    """
    A workflow can "branch" or follow a path after the execution of this task.

    It derives the PythonOperator and expects a Python function that returns
    a single task_id, a single task_group_id, or a list of task_ids and/or
    task_group_ids to follow. The task_id(s) and/or task_group_id(s) returned
    should point to a task or task group directly downstream from {self}. All
    other "branches" or directly downstream tasks are marked with a state of
    ``skipped`` so that these paths can't move forward. The ``skipped`` states
    are propagated downstream to allow for the DAG state to fill up and
    the DAG run's state to be inferred.
    """

    def execute(self, context: Context) -> Any:
        return self.do_branch(context, super().execute(context))


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
        skipped but the ``trigger_rule`` defined for all other downstream tasks will be respected.
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

        if not self.downstream_task_ids:
            self.log.info("No downstream tasks; nothing to do.")
            return condition

        dag_run = context["dag_run"]

        def get_tasks_to_skip():
            if self.ignore_downstream_trigger_rules is True:
                tasks = context["task"].get_flat_relatives(upstream=False)
            else:
                tasks = context["task"].get_direct_relatives(upstream=False)
            for t in tasks:
                if not t.is_teardown:
                    yield t

        to_skip = get_tasks_to_skip()

        # this let's us avoid an intermediate list unless debug logging
        if self.log.getEffectiveLevel() <= logging.DEBUG:
            self.log.debug("Downstream task IDs %s", to_skip := list(get_tasks_to_skip()))

        self.log.info("Skipping downstream tasks")

        self.skip(
            dag_run=dag_run,
            tasks=to_skip,
            map_index=context["ti"].map_index,
        )
        self.log.info("Done.")
        # returns the result of the super execute method as it is instead of returning None
        return condition


def _load_pickle():
    import pickle

    return pickle


def _load_dill():
    try:
        import dill
    except ModuleNotFoundError:
        log.error("Unable to import `dill` module. Please please make sure that it installed.")
        raise
    return dill


def _load_cloudpickle():
    try:
        import cloudpickle
    except ModuleNotFoundError:
        log.error(
            "Unable to import `cloudpickle` module. "
            "Please install it with: pip install 'apache-airflow[cloudpickle]'"
        )
        raise
    return cloudpickle


_SerializerTypeDef = Literal["pickle", "cloudpickle", "dill"]
_SERIALIZERS: dict[_SerializerTypeDef, Any] = {
    "pickle": lazy_object_proxy.Proxy(_load_pickle),
    "dill": lazy_object_proxy.Proxy(_load_dill),
    "cloudpickle": lazy_object_proxy.Proxy(_load_cloudpickle),
}


class _BasePythonVirtualenvOperator(PythonOperator, metaclass=ABCMeta):
    BASE_SERIALIZABLE_CONTEXT_KEYS = {
        "ds",
        "ds_nodash",
        "expanded_ti_count",
        "inlets",
        "map_index_template",
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
        "prev_end_date_success",
    }
    AIRFLOW_SERIALIZABLE_CONTEXT_KEYS = {
        "macros",
        "conf",
        "dag",
        "dag_run",
        "task",
        "params",
        "triggering_asset_events",
    }

    def __init__(
        self,
        *,
        python_callable: Callable,
        serializer: _SerializerTypeDef | None = None,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        string_args: Iterable[str] | None = None,
        templates_dict: dict | None = None,
        templates_exts: list[str] | None = None,
        expect_airflow: bool = True,
        skip_on_exit_code: int | Container[int] | None = None,
        env_vars: dict[str, str] | None = None,
        inherit_env: bool = True,
        use_dill: bool = False,
        use_airflow_context: bool = False,
        **kwargs,
    ):
        if (
            not isinstance(python_callable, types.FunctionType)
            or isinstance(python_callable, types.LambdaType)
            and python_callable.__name__ == "<lambda>"
        ):
            raise ValueError(f"{type(self).__name__} only supports functions for python_callable arg")
        if inspect.isgeneratorfunction(python_callable):
            raise ValueError(f"{type(self).__name__} does not support using 'yield' in python_callable")
        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            **kwargs,
        )
        self.string_args = string_args or []

        if use_dill:
            warnings.warn(
                "`use_dill` is deprecated and will be removed in a future version. "
                "Please provide serializer='dill' instead.",
                RemovedInAirflow3Warning,
                stacklevel=3,
            )
            if serializer:
                raise AirflowException(
                    "Both 'use_dill' and 'serializer' parameters are set. Please set only one of them"
                )
            serializer = "dill"
        serializer = serializer or "pickle"
        if serializer not in _SERIALIZERS:
            msg = (
                f"Unsupported serializer {serializer!r}. "
                f"Expected one of {', '.join(map(repr, _SERIALIZERS))}"
            )
            raise AirflowException(msg)

        self.pickling_library = _SERIALIZERS[serializer]
        self.serializer: _SerializerTypeDef = serializer

        self.expect_airflow = expect_airflow
        self.skip_on_exit_code = (
            skip_on_exit_code
            if isinstance(skip_on_exit_code, Container)
            else [skip_on_exit_code]
            if skip_on_exit_code is not None
            else []
        )
        self.env_vars = env_vars
        self.inherit_env = inherit_env
        self.use_airflow_context = use_airflow_context

    @abstractmethod
    def _iter_serializable_context_keys(self):
        pass

    def execute(self, context: Context) -> Any:
        serializable_keys = set(self._iter_serializable_context_keys())
        serializable_context = context_copy_partial(context, serializable_keys)
        return super().execute(context=serializable_context)

    def get_python_source(self):
        """Return the source of self.python_callable."""
        return textwrap.dedent(inspect.getsource(self.python_callable))

    def _write_args(self, file: Path):
        if self.op_args or self.op_kwargs:
            self.log.info("Use %r as serializer.", self.serializer)
            file.write_bytes(self.pickling_library.dumps({"args": self.op_args, "kwargs": self.op_kwargs}))

    def _write_string_args(self, file: Path):
        file.write_text("\n".join(map(str, self.string_args)))

    def _read_result(self, path: Path):
        if path.stat().st_size == 0:
            return None
        try:
            return self.pickling_library.loads(path.read_bytes())
        except ValueError as value_error:
            raise DeserializingResultError() from value_error

    def __deepcopy__(self, memo):
        # module objects can't be copied _at all__
        memo[id(self.pickling_library)] = self.pickling_library
        return super().__deepcopy__(memo)

    def _execute_python_callable_in_subprocess(self, python_path: Path):
        with TemporaryDirectory(prefix="venv-call") as tmp:
            tmp_dir = Path(tmp)
            op_kwargs: dict[str, Any] = dict(self.op_kwargs)
            if self.templates_dict:
                op_kwargs["templates_dict"] = self.templates_dict
            input_path = tmp_dir / "script.in"
            output_path = tmp_dir / "script.out"
            string_args_path = tmp_dir / "string_args.txt"
            script_path = tmp_dir / "script.py"
            termination_log_path = tmp_dir / "termination.log"
            airflow_context_path = tmp_dir / "airflow_context.json"

            self._write_args(input_path)
            self._write_string_args(string_args_path)

            if self.use_airflow_context and not _ENABLE_AIP_44:
                error_msg = "`get_current_context()` needs to be used with AIP-44 enabled."
                raise AirflowException(error_msg)

            jinja_context = {
                "op_args": self.op_args,
                "op_kwargs": op_kwargs,
                "expect_airflow": self.expect_airflow,
                "pickling_library": self.serializer,
                "python_callable": self.python_callable.__name__,
                "python_callable_source": self.get_python_source(),
                "use_airflow_context": self.use_airflow_context,
            }

            if inspect.getfile(self.python_callable) == self.dag.fileloc:
                jinja_context["modified_dag_module_name"] = get_unique_dag_module_name(self.dag.fileloc)

            write_python_script(
                jinja_context=jinja_context,
                filename=os.fspath(script_path),
                render_template_as_native_obj=self.dag.render_template_as_native_obj,
            )
            if self.use_airflow_context:
                from airflow.serialization.serialized_objects import BaseSerialization

                context = get_current_context()
                with create_session() as session:
                    # FIXME: DetachedInstanceError
                    dag_run, task_instance = context["dag_run"], context["task_instance"]
                    session.add_all([dag_run, task_instance])
                    serializable_context: dict[Encoding, Any] = BaseSerialization.serialize(
                        context, use_pydantic_models=True
                    )
                with airflow_context_path.open("w+") as file:
                    json.dump(serializable_context, file)

            env_vars = dict(os.environ) if self.inherit_env else {}
            if self.env_vars:
                env_vars.update(self.env_vars)

            try:
                execute_in_subprocess(
                    cmd=[
                        os.fspath(python_path),
                        os.fspath(script_path),
                        os.fspath(input_path),
                        os.fspath(output_path),
                        os.fspath(string_args_path),
                        os.fspath(termination_log_path),
                        os.fspath(airflow_context_path),
                    ],
                    env=env_vars,
                )
            except subprocess.CalledProcessError as e:
                if e.returncode in self.skip_on_exit_code:
                    raise AirflowSkipException(f"Process exited with code {e.returncode}. Skipping.")
                elif termination_log_path.exists() and termination_log_path.stat().st_size > 0:
                    error_msg = f"Process returned non-zero exit status {e.returncode}.\n"
                    with open(termination_log_path) as file:
                        error_msg += file.read()
                    raise AirflowException(error_msg) from None
                else:
                    raise

            if 0 in self.skip_on_exit_code:
                raise AirflowSkipException("Process exited with code 0. Skipping.")

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
        defined with def, which will be run in a virtual environment.
    :param requirements: Either a list of requirement strings, or a (templated)
        "requirements file" as specified by pip.
    :param python_version: The Python version to run the virtual environment with. Note that
        both 2 and 2.7 are acceptable forms.
    :param serializer: Which serializer use to serialize the args and result. It can be one of the following:

        - ``"pickle"``: (default) Use pickle for serialization. Included in the Python Standard Library.
        - ``"cloudpickle"``: Use cloudpickle for serialize more complex types,
          this requires to include cloudpickle in your requirements.
        - ``"dill"``: Use dill for serialize more complex types,
          this requires to include dill in your requirements.
    :param system_site_packages: Whether to include
        system_site_packages in your virtual environment.
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
    :param skip_on_exit_code: If python_callable exits with this exit code, leave the task
        in ``skipped`` state (default: None). If set to ``None``, any non-zero
        exit code will be treated as a failure.
    :param index_urls: an optional list of index urls to load Python packages from.
        If not provided the system pip conf will be used to source packages from.
    :param venv_cache_path: Optional path to the virtual environment parent folder in which the
        virtual environment will be cached, creates a sub-folder venv-{hash} whereas hash will be replaced
        with a checksum of requirements. If not provided the virtual environment will be created and deleted
        in a temp folder for every execution.
    :param env_vars: A dictionary containing additional environment variables to set for the virtual
        environment when it is executed.
    :param inherit_env: Whether to inherit the current environment variables when executing the virtual
        environment. If set to ``True``, the virtual environment will inherit the environment variables
        of the parent process (``os.environ``). If set to ``False``, the virtual environment will be
        executed with a clean environment.
    :param use_dill: Deprecated, use ``serializer`` instead. Whether to use dill to serialize
        the args and result (pickle is default). This allows more complex types
        but requires you to include dill in your requirements.
    :param use_airflow_context: Whether to provide ``get_current_context()`` to the python_callable.
    """

    template_fields: Sequence[str] = tuple(
        {"requirements", "index_urls", "venv_cache_path"}.union(PythonOperator.template_fields)
    )
    template_ext: Sequence[str] = (".txt",)

    def __init__(
        self,
        *,
        python_callable: Callable,
        requirements: None | Iterable[str] | str = None,
        python_version: str | None = None,
        serializer: _SerializerTypeDef | None = None,
        system_site_packages: bool = True,
        pip_install_options: list[str] | None = None,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        string_args: Iterable[str] | None = None,
        templates_dict: dict | None = None,
        templates_exts: list[str] | None = None,
        expect_airflow: bool = True,
        skip_on_exit_code: int | Container[int] | None = None,
        index_urls: None | Collection[str] | str = None,
        venv_cache_path: None | os.PathLike[str] = None,
        env_vars: dict[str, str] | None = None,
        inherit_env: bool = True,
        use_dill: bool = False,
        use_airflow_context: bool = False,
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
                f"Sys version: {sys.version_info}. Virtual environment version: {python_version}"
            )
        if python_version is not None and not isinstance(python_version, str):
            warnings.warn(
                "Passing non-string types (e.g. int or float) as python_version "
                "is deprecated. Please use string value instead.",
                RemovedInAirflow3Warning,
                stacklevel=2,
            )
        if not is_venv_installed():
            raise AirflowException("PythonVirtualenvOperator requires virtualenv, please install it.")
        if use_airflow_context and (not expect_airflow and not system_site_packages):
            error_msg = "use_airflow_context is set to True, but expect_airflow and system_site_packages are set to False."
            raise AirflowException(error_msg)
        if not requirements:
            self.requirements: list[str] = []
        elif isinstance(requirements, str):
            self.requirements = [requirements]
        else:
            self.requirements = list(requirements)
        self.python_version = python_version
        self.system_site_packages = system_site_packages
        self.pip_install_options = pip_install_options
        if isinstance(index_urls, str):
            self.index_urls: list[str] | None = [index_urls]
        elif isinstance(index_urls, Collection):
            self.index_urls = list(index_urls)
        else:
            self.index_urls = None
        self.venv_cache_path = venv_cache_path
        super().__init__(
            python_callable=python_callable,
            serializer=serializer,
            op_args=op_args,
            op_kwargs=op_kwargs,
            string_args=string_args,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            expect_airflow=expect_airflow,
            skip_on_exit_code=skip_on_exit_code,
            env_vars=env_vars,
            inherit_env=inherit_env,
            use_dill=use_dill,
            use_airflow_context=use_airflow_context,
            **kwargs,
        )

    def _requirements_list(self, exclude_cloudpickle: bool = False) -> list[str]:
        """Prepare a list of requirements that need to be installed for the virtual environment."""
        requirements = [str(dependency) for dependency in self.requirements]
        if not self.system_site_packages:
            if (
                self.serializer == "cloudpickle"
                and not exclude_cloudpickle
                and "cloudpickle" not in requirements
            ):
                requirements.append("cloudpickle")
            elif self.serializer == "dill" and "dill" not in requirements:
                requirements.append("dill")
        requirements.sort()  # Ensure a hash is stable
        return requirements

    def _prepare_venv(self, venv_path: Path) -> None:
        """Prepare the requirements and installs the virtual environment."""
        requirements_file = venv_path / "requirements.txt"
        requirements_file.write_text("\n".join(self._requirements_list()))
        prepare_virtualenv(
            venv_directory=str(venv_path),
            python_bin=f"python{self.python_version}" if self.python_version else "python",
            system_site_packages=self.system_site_packages,
            requirements_file_path=str(requirements_file),
            pip_install_options=self.pip_install_options,
            index_urls=self.index_urls,
        )

    def _calculate_cache_hash(self, exclude_cloudpickle: bool = False) -> tuple[str, str]:
        """
        Generate the hash of the cache folder to use.

        The following factors are used as input for the hash:
        - (sorted) list of requirements
        - pip install options
        - flag of system site packages
        - python version
        - Variable to override the hash with a cache key
        - Index URLs

        Returns a hash and the data dict which is the base for the hash as text.
        """
        hash_dict = {
            "requirements_list": self._requirements_list(exclude_cloudpickle=exclude_cloudpickle),
            "pip_install_options": self.pip_install_options,
            "index_urls": self.index_urls,
            "cache_key": str(Variable.get("PythonVirtualenvOperator.cache_key", "")),
            "python_version": self.python_version,
            "system_site_packages": self.system_site_packages,
        }
        hash_text = json.dumps(hash_dict, sort_keys=True)
        hash_object = hashlib_wrapper.md5(hash_text.encode())
        requirements_hash = hash_object.hexdigest()
        return requirements_hash[:8], hash_text

    def _ensure_venv_cache_exists(self, venv_cache_path: Path) -> Path:
        """Ensure a valid virtual environment is set up and will create inplace."""
        cache_hash, hash_data = self._calculate_cache_hash()
        venv_path = venv_cache_path / f"venv-{cache_hash}"
        self.log.info("Python virtual environment will be cached in %s", venv_path)
        venv_path.parent.mkdir(parents=True, exist_ok=True)
        with open(f"{venv_path}.lock", "w") as f:
            # Ensure that cache is not build by parallel workers
            import fcntl

            fcntl.flock(f, fcntl.LOCK_EX)

            hash_marker = venv_path / "install_complete_marker.json"
            try:
                if venv_path.exists():
                    if hash_marker.exists():
                        previous_hash_data = hash_marker.read_text(encoding="utf8")
                        if previous_hash_data == hash_data:
                            self.log.info("Re-using cached Python virtual environment in %s", venv_path)
                            return venv_path

                        _, hash_data_before_upgrade = self._calculate_cache_hash(exclude_cloudpickle=True)
                        if previous_hash_data == hash_data_before_upgrade:
                            self.log.warning(
                                "Found a previous virtual environment in  with outdated dependencies %s, "
                                "deleting and re-creating.",
                                venv_path,
                            )
                        else:
                            self.log.error(
                                "Unicorn alert: Found a previous virtual environment in %s "
                                "with the same hash but different parameters. Previous setup: '%s' / "
                                "Requested venv setup: '%s'. Please report a bug to airflow!",
                                venv_path,
                                previous_hash_data,
                                hash_data,
                            )
                    else:
                        self.log.warning(
                            "Found a previous (probably partial installed) virtual environment in %s, "
                            "deleting and re-creating.",
                            venv_path,
                        )

                    shutil.rmtree(venv_path)

                venv_path.mkdir(parents=True)
                self._prepare_venv(venv_path)
                hash_marker.write_text(hash_data, encoding="utf8")
            except Exception as e:
                shutil.rmtree(venv_path)
                raise AirflowException(f"Unable to create new virtual environment in {venv_path}") from e
            self.log.info("New Python virtual environment created in %s", venv_path)
            return venv_path

    def execute_callable(self):
        if self.venv_cache_path:
            venv_path = self._ensure_venv_cache_exists(Path(self.venv_cache_path))
            python_path = venv_path / "bin" / "python"
            return self._execute_python_callable_in_subprocess(python_path)

        with TemporaryDirectory(prefix="venv") as tmp_dir:
            tmp_path = Path(tmp_dir)
            self._prepare_venv(tmp_path)
            python_path = tmp_path / "bin" / "python"
            result = self._execute_python_callable_in_subprocess(python_path)
            return result

    def _iter_serializable_context_keys(self):
        yield from self.BASE_SERIALIZABLE_CONTEXT_KEYS
        if self.system_site_packages or "apache-airflow" in self.requirements:
            yield from self.AIRFLOW_SERIALIZABLE_CONTEXT_KEYS
            yield from self.PENDULUM_SERIALIZABLE_CONTEXT_KEYS
        elif "pendulum" in self.requirements:
            yield from self.PENDULUM_SERIALIZABLE_CONTEXT_KEYS


class BranchPythonVirtualenvOperator(PythonVirtualenvOperator, BranchMixIn):
    """
    A workflow can "branch" or follow a path after the execution of this task in a virtual environment.

    It derives the PythonVirtualenvOperator and expects a Python function that returns
    a single task_id, a single task_group_id, or a list of task_ids and/or
    task_group_ids to follow. The task_id(s) and/or task_group_id(s) returned
    should point to a task or task group directly downstream from {self}. All
    other "branches" or directly downstream tasks are marked with a state of
    ``skipped`` so that these paths can't move forward. The ``skipped`` states
    are propagated downstream to allow for the DAG state to fill up and
    the DAG run's state to be inferred.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BranchPythonVirtualenvOperator`
    """

    def execute(self, context: Context) -> Any:
        return self.do_branch(context, super().execute(context))


class ExternalPythonOperator(_BasePythonVirtualenvOperator):
    """
    Run a function in a virtualenv that is not re-created.

    Reused as is without the overhead of creating the virtual environment (with certain caveats).

    The function must be defined using def, and not be
    part of a class. All imports must happen inside the function
    and no variables outside the scope may be referenced. A global scope
    variable named virtualenv_string_args will be available (populated by
    string_args). In addition, one can pass stuff through op_args and op_kwargs, and one
    can use a return value.
    Note that if your virtual environment runs in a different Python major version than Airflow,
    you cannot use return values, op_args, op_kwargs, or use any macros that are being provided to
    Airflow through plugins. You can use string_args though.

    If Airflow is installed in the external environment in different version that the version
    used by the operator, the operator will fail.,

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ExternalPythonOperator`

    :param python: Full path string (file-system specific) that points to a Python binary inside
        a virtual environment that should be used (in ``VENV/bin`` folder). Should be absolute path
        (so usually start with "/" or "X:/" depending on the filesystem/os used).
    :param python_callable: A python function with no references to outside variables,
        defined with def, which will be run in a virtual environment.
    :param serializer: Which serializer use to serialize the args and result. It can be one of the following:

        - ``"pickle"``: (default) Use pickle for serialization. Included in the Python Standard Library.
        - ``"cloudpickle"``: Use cloudpickle for serialize more complex types,
          this requires to include cloudpickle in your requirements.
        - ``"dill"``: Use dill for serialize more complex types,
          this requires to include dill in your requirements.
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
    :param skip_on_exit_code: If python_callable exits with this exit code, leave the task
        in ``skipped`` state (default: None). If set to ``None``, any non-zero
        exit code will be treated as a failure.
    :param env_vars: A dictionary containing additional environment variables to set for the virtual
        environment when it is executed.
    :param inherit_env: Whether to inherit the current environment variables when executing the virtual
        environment. If set to ``True``, the virtual environment will inherit the environment variables
        of the parent process (``os.environ``). If set to ``False``, the virtual environment will be
        executed with a clean environment.
    :param use_dill: Deprecated, use ``serializer`` instead. Whether to use dill to serialize
        the args and result (pickle is default). This allows more complex types
        but requires you to include dill in your requirements.
    :param use_airflow_context: Whether to provide ``get_current_context()`` to the python_callable.
    """

    template_fields: Sequence[str] = tuple({"python"}.union(PythonOperator.template_fields))

    def __init__(
        self,
        *,
        python: str,
        python_callable: Callable,
        serializer: _SerializerTypeDef | None = None,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        string_args: Iterable[str] | None = None,
        templates_dict: dict | None = None,
        templates_exts: list[str] | None = None,
        expect_airflow: bool = True,
        expect_pendulum: bool = False,
        skip_on_exit_code: int | Container[int] | None = None,
        env_vars: dict[str, str] | None = None,
        inherit_env: bool = True,
        use_dill: bool = False,
        use_airflow_context: bool = False,
        **kwargs,
    ):
        if not python:
            raise ValueError("Python Path must be defined in ExternalPythonOperator")
        if use_airflow_context and not expect_airflow:
            error_msg = "use_airflow_context is set to True, but expect_airflow is set to False."
            raise AirflowException(error_msg)
        self.python = python
        self.expect_pendulum = expect_pendulum
        super().__init__(
            python_callable=python_callable,
            serializer=serializer,
            op_args=op_args,
            op_kwargs=op_kwargs,
            string_args=string_args,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            expect_airflow=expect_airflow,
            skip_on_exit_code=skip_on_exit_code,
            env_vars=env_vars,
            inherit_env=inherit_env,
            use_dill=use_dill,
            use_airflow_context=use_airflow_context,
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
        python_version = _PythonVersionInfo.from_executable(self.python)
        if python_version.major != sys.version_info.major and (self.op_args or self.op_kwargs):
            raise AirflowException(
                "Passing op_args or op_kwargs is not supported across different Python "
                "major versions for ExternalPythonOperator. Please use string_args."
                f"Sys version: {sys.version_info}. "
                f"Virtual environment version: {python_version}"
            )
        return self._execute_python_callable_in_subprocess(python_path)

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
                self.log.warning("When checking for Pendulum installed in virtual environment got %s", e)
                self.log.warning(
                    "Pendulum is not properly installed in the virtual environment "
                    "Pendulum context keys will not be available. "
                    "Please Install Pendulum or Airflow in your virtual environment to access them."
                )
            return False

    @property
    def _external_airflow_version_script(self):
        """
        Return python script which determines the version of the Apache Airflow.

        Import airflow as a module might take a while as a result,
        obtaining a version would take up to 1 second.
        On the other hand, `importlib.metadata.version` will retrieve the package version pretty fast
        something below 100ms; this includes new subprocess overhead.

        Possible side effect: It might be a situation that `importlib.metadata` is not available (Python < 3.8),
        as well as backport `importlib_metadata` which might indicate that venv doesn't contain an `apache-airflow`
        or something wrong with the environment.
        """
        return textwrap.dedent(
            """
            try:
                from importlib.metadata import version
            except ImportError:
                from importlib_metadata import version
            print(version("apache-airflow"))
            """
        )

    def _get_airflow_version_from_target_env(self) -> str | None:
        from airflow import __version__ as airflow_version

        try:
            result = subprocess.check_output(
                [self.python, "-c", self._external_airflow_version_script],
                text=True,
            )
            target_airflow_version = result.strip()
            if target_airflow_version != airflow_version:
                raise AirflowConfigException(
                    f"The version of Airflow installed for the {self.python} "
                    f"({target_airflow_version}) is different than the runtime Airflow version: "
                    f"{airflow_version}. Make sure your environment has the same Airflow version "
                    f"installed as the Airflow runtime."
                )
            return target_airflow_version
        except Exception as e:
            if self.expect_airflow:
                self.log.warning("When checking for Airflow installed in virtual environment got %s", e)
                self.log.warning(
                    "This means that Airflow is not properly installed by %s. "
                    "Airflow context keys will not be available. "
                    "Please Install Airflow %s in your environment to access them.",
                    self.python,
                    airflow_version,
                )
            return None


class BranchExternalPythonOperator(ExternalPythonOperator, BranchMixIn):
    """
    A workflow can "branch" or follow a path after the execution of this task.

    Extends ExternalPythonOperator, so expects to get Python:
    virtual environment that should be used (in ``VENV/bin`` folder). Should be absolute path,
    so it can run on separate virtual environment similarly to ExternalPythonOperator.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BranchExternalPythonOperator`
    """

    def execute(self, context: Context) -> Any:
        return self.do_branch(context, super().execute(context))


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

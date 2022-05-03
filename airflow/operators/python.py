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
import inspect
import os
import pickle
import shutil
import sys
import types
import warnings
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import Any, Callable, Collection, Dict, Iterable, List, Mapping, Optional, Sequence, Union

import dill

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.models.skipmixin import SkipMixin
from airflow.models.taskinstance import _CURRENT_CONTEXT
from airflow.utils.context import Context, context_copy_partial, context_merge
from airflow.utils.operator_helpers import KeywordParameters
from airflow.utils.process_utils import execute_in_subprocess
from airflow.utils.python_virtualenv import prepare_virtualenv, write_python_script


def task(python_callable: Optional[Callable] = None, multiple_outputs: Optional[bool] = None, **kwargs):
    """
    Deprecated function that calls @task.python and allows users to turn a python function into
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
        DeprecationWarning,
        stacklevel=2,
    )
    return python_task(python_callable=python_callable, multiple_outputs=multiple_outputs, **kwargs)


class PythonOperator(BaseOperator):
    """
    Executes a Python callable

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

    template_fields: Sequence[str] = ('templates_dict', 'op_args', 'op_kwargs')
    template_fields_renderers = {"templates_dict": "json", "op_args": "py", "op_kwargs": "py"}
    BLUE = '#ffefeb'
    ui_color = BLUE

    # since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects(e.g protobuf).
    shallow_copy_attrs: Sequence[str] = (
        'python_callable',
        'op_kwargs',
    )

    mapped_arguments_validated_by_init = True

    def __init__(
        self,
        *,
        python_callable: Callable,
        op_args: Optional[Collection[Any]] = None,
        op_kwargs: Optional[Mapping[str, Any]] = None,
        templates_dict: Optional[Dict[str, Any]] = None,
        templates_exts: Optional[Sequence[str]] = None,
        show_return_value_in_logs: bool = True,
        **kwargs,
    ) -> None:
        if kwargs.get("provide_context"):
            warnings.warn(
                "provide_context is deprecated as of 2.0 and is no longer required",
                DeprecationWarning,
                stacklevel=2,
            )
            kwargs.pop('provide_context', None)
        super().__init__(**kwargs)
        if not callable(python_callable):
            raise AirflowException('`python_callable` param must be callable')
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

    def execute_callable(self):
        """
        Calls the python callable with the given arguments.

        :return: the return value of the call.
        :rtype: any
        """
        return self.python_callable(*self.op_args, **self.op_kwargs)


class BranchPythonOperator(PythonOperator, SkipMixin):
    """
    Allows a workflow to "branch" or follow a path following the execution
    of this task.

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
        # TODO: The logic should be moved to SkipMixin to be available to all branch operators.
        if isinstance(branch, str):
            branches = {branch}
        elif isinstance(branch, list):
            branches = set(branch)
        elif branch is None:
            branches = set()
        else:
            raise AirflowException("Branch callable must return either None, a task ID, or a list of IDs")
        valid_task_ids = set(context["dag"].task_ids)
        invalid_task_ids = branches - valid_task_ids
        if invalid_task_ids:
            raise AirflowException(
                f"Branch callable must return valid task_ids. Invalid tasks found: {invalid_task_ids}"
            )
        self.skip_all_except(context['ti'], branch)
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
            self.log.info('Proceeding with downstream tasks...')
            return condition

        downstream_tasks = context['task'].get_flat_relatives(upstream=False)
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


class PythonVirtualenvOperator(PythonOperator):
    """
    Allows one to run a function in a virtualenv that is created and destroyed
    automatically (with certain caveats).

    The function must be defined using def, and not be
    part of a class. All imports must happen inside the function
    and no variables outside of the scope may be referenced. A global scope
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
    """

    template_fields: Sequence[str] = ('requirements',)
    template_ext: Sequence[str] = ('.txt',)
    BASE_SERIALIZABLE_CONTEXT_KEYS = {
        'ds',
        'ds_nodash',
        'inlets',
        'next_ds',
        'next_ds_nodash',
        'outlets',
        'prev_ds',
        'prev_ds_nodash',
        'run_id',
        'task_instance_key_str',
        'test_mode',
        'tomorrow_ds',
        'tomorrow_ds_nodash',
        'ts',
        'ts_nodash',
        'ts_nodash_with_tz',
        'yesterday_ds',
        'yesterday_ds_nodash',
    }
    PENDULUM_SERIALIZABLE_CONTEXT_KEYS = {
        'data_interval_end',
        'data_interval_start',
        'execution_date',
        'logical_date',
        'next_execution_date',
        'prev_data_interval_end_success',
        'prev_data_interval_start_success',
        'prev_execution_date',
        'prev_execution_date_success',
        'prev_start_date_success',
    }
    AIRFLOW_SERIALIZABLE_CONTEXT_KEYS = {'macros', 'conf', 'dag', 'dag_run', 'task', 'params'}

    def __init__(
        self,
        *,
        python_callable: Callable,
        requirements: Union[None, Iterable[str], str] = None,
        python_version: Optional[Union[str, int, float]] = None,
        use_dill: bool = False,
        system_site_packages: bool = True,
        pip_install_options: Optional[List[str]] = None,
        op_args: Optional[Collection[Any]] = None,
        op_kwargs: Optional[Mapping[str, Any]] = None,
        string_args: Optional[Iterable[str]] = None,
        templates_dict: Optional[Dict] = None,
        templates_exts: Optional[List[str]] = None,
        **kwargs,
    ):
        if (
            not isinstance(python_callable, types.FunctionType)
            or isinstance(python_callable, types.LambdaType)
            and python_callable.__name__ == "<lambda>"
        ):
            raise AirflowException('PythonVirtualenvOperator only supports functions for python_callable arg')
        if (
            python_version
            and str(python_version)[0] != str(sys.version_info.major)
            and (op_args or op_kwargs)
        ):
            raise AirflowException(
                "Passing op_args or op_kwargs is not supported across different Python "
                "major versions for PythonVirtualenvOperator. Please use string_args."
            )
        if not shutil.which("virtualenv"):
            raise AirflowException('PythonVirtualenvOperator requires virtualenv, please install it.')
        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            **kwargs,
        )
        if not requirements:
            self.requirements: Union[List[str], str] = []
        elif isinstance(requirements, str):
            self.requirements = requirements
        else:
            self.requirements = list(requirements)
        self.string_args = string_args or []
        self.python_version = python_version
        self.use_dill = use_dill
        self.system_site_packages = system_site_packages
        self.pip_install_options = pip_install_options
        self.pickling_library = dill if self.use_dill else pickle

    def execute(self, context: Context) -> Any:
        serializable_keys = set(self._iter_serializable_context_keys())
        serializable_context = context_copy_partial(context, serializable_keys)
        return super().execute(context=serializable_context)

    def determine_kwargs(self, context: Mapping[str, Any]) -> Mapping[str, Any]:
        return KeywordParameters.determine(self.python_callable, self.op_args, context).serializing()

    def execute_callable(self):
        with TemporaryDirectory(prefix='venv') as tmp_dir:
            requirements_file_name = f'{tmp_dir}/requirements.txt'

            if not isinstance(self.requirements, str):
                requirements_file_contents = "\n".join(str(dependency) for dependency in self.requirements)
            else:
                requirements_file_contents = self.requirements

            if not self.system_site_packages and self.use_dill:
                requirements_file_contents += '\ndill'

            with open(requirements_file_name, 'w') as file:
                file.write(requirements_file_contents)

            if self.templates_dict:
                self.op_kwargs['templates_dict'] = self.templates_dict

            input_filename = os.path.join(tmp_dir, 'script.in')
            output_filename = os.path.join(tmp_dir, 'script.out')
            string_args_filename = os.path.join(tmp_dir, 'string_args.txt')
            script_filename = os.path.join(tmp_dir, 'script.py')

            prepare_virtualenv(
                venv_directory=tmp_dir,
                python_bin=f'python{self.python_version}' if self.python_version else None,
                system_site_packages=self.system_site_packages,
                requirements_file_path=requirements_file_name,
                pip_install_options=self.pip_install_options,
            )

            self._write_args(input_filename)
            self._write_string_args(string_args_filename)
            write_python_script(
                jinja_context=dict(
                    op_args=self.op_args,
                    op_kwargs=self.op_kwargs,
                    pickling_library=self.pickling_library.__name__,
                    python_callable=self.python_callable.__name__,
                    python_callable_source=self.get_python_source(),
                ),
                filename=script_filename,
                render_template_as_native_obj=self.dag.render_template_as_native_obj,
            )

            execute_in_subprocess(
                cmd=[
                    f'{tmp_dir}/bin/python',
                    script_filename,
                    input_filename,
                    output_filename,
                    string_args_filename,
                ]
            )

            return self._read_result(output_filename)

    def get_python_source(self):
        """
        Returns the source of self.python_callable
        @return:
        """
        return dedent(inspect.getsource(self.python_callable))

    def _write_args(self, filename):
        if self.op_args or self.op_kwargs:
            with open(filename, 'wb') as file:
                self.pickling_library.dump({'args': self.op_args, 'kwargs': self.op_kwargs}, file)

    def _iter_serializable_context_keys(self):
        yield from self.BASE_SERIALIZABLE_CONTEXT_KEYS
        if self.system_site_packages or 'apache-airflow' in self.requirements:
            yield from self.AIRFLOW_SERIALIZABLE_CONTEXT_KEYS
            yield from self.PENDULUM_SERIALIZABLE_CONTEXT_KEYS
        elif 'pendulum' in self.requirements:
            yield from self.PENDULUM_SERIALIZABLE_CONTEXT_KEYS

    def _write_string_args(self, filename):
        with open(filename, 'w') as file:
            file.write('\n'.join(map(str, self.string_args)))

    def _read_result(self, filename):
        if os.stat(filename).st_size == 0:
            return None
        with open(filename, 'rb') as file:
            try:
                return self.pickling_library.load(file)
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


def get_current_context() -> Context:
    """
    Obtain the execution context for the currently executing operator without
    altering user method's signature.
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

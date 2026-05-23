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
from abc import ABCMeta, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import attrs

from airflow.sdk import TriggerRule, timezone
from airflow.sdk.bases.decorator import (
    DecoratedMappedOperator,
    FParams,
    FReturn,
    OperatorSubclass,
    _TaskDecorator,
    get_unique_task_id,
)
from airflow.sdk.bases.operator import (
    BaseOperator,
    coerce_resources,
    coerce_timedelta,
    get_merged_defaults,
    parse_retries,
)
from airflow.sdk.definitions._internal.contextmanager import (
    DagContext,
    TaskGroupContext,
)
from airflow.sdk.definitions._internal.expandinput import (
    EXPAND_INPUT_EMPTY,
    DecoratedExpandInput,
    DictOfListsExpandInput,
    ExpandInput,
    ListOfDictsExpandInput,
    OperatorExpandArgument,
    OperatorExpandKwargsArgument,
)
from airflow.sdk.definitions._internal.types import NOTSET
from airflow.sdk.definitions.mappedoperator import (
    MappedOperator,
    OperatorPartial,
    ensure_xcomarg_return_value,
    prevent_duplicates,
    validate_mapping_kwargs,
)
from airflow.sdk.definitions.xcom_arg import XComArg

if TYPE_CHECKING:
    from airflow.sdk.definitions.iterableoperator import IterableOperator, MappedIterableOperator
    from airflow.sdk.definitions.mappedoperator import ValidationSource
    from airflow.sdk.definitions.param import ParamsDict

T = TypeVar("T", bound=OperatorPartial | _TaskDecorator)


@attrs.define(kw_only=True, repr=False)
class PartitionableOperator(Generic[T], metaclass=ABCMeta):
    """
    Intermediate abstraction for partitioned mapping.

    This class decorates an OperatorPartial and stores partition configuration for partitioned mapping.
    It is used to facilitate partitioned expansion of operators, allowing tasks to be mapped over partitions
    of data and then iterate over the partitioned data.

    :param operator_partial: The partial operator to be partitioned.
    :param size: The number of partitions to create.
    """

    operator_partial: T
    size: int

    @property
    def operator_class(self) -> type[BaseOperator]:
        return self.operator_partial.operator_class

    @property
    def kwargs(self) -> dict[str, Any]:
        return self.operator_partial.kwargs

    @abstractmethod
    def iterate(self, **mapped_kwargs: OperatorExpandArgument) -> Any:
        """
        Iterate the operator over the provided mapped keyword arguments.

        :param mapped_kwargs: Keyword arguments to expand against.
        :return: An expanded operator or XComArg, depending on the subclass implementation.
        """

    @abstractmethod
    def iterate_kwargs(self, kwargs: OperatorExpandKwargsArgument, *, strict: bool = True) -> Any:
        """
        Iterate the operator over a list of dictionaries or XComArg.

        :param kwargs: List of dicts or XComArg to expand against.
        :param strict: Whether to enforce strict argument checking.
        :return: An expanded operator or XComArg, depending on the subclass implementation.
        """

    @abstractmethod
    def _iterate(
        self,
        expand_input: ExpandInput,
        *,
        strict: bool,
    ) -> IterableOperator | MappedIterableOperator:
        """
        Create an iterable operator for the given expansion input.

        This method calls the _expand method first to get a MappedOperator based on expansion input,
        then wraps it in either an IterableOperator or MappedIterableOperator depending on the partition size.

        :param expand_input: The input to iterate against.
        :param strict: Whether to enforce strict argument checking.
        :return: An IterableOperator or MappedIterableOperator.
        """

    @abstractmethod
    def _expand(
        self,
        expand_input: ExpandInput,
        *,
        strict: bool,
        apply_upstream_relationship: bool = True,
    ) -> MappedOperator:
        """
        Create a mapped operator for the given expansion input.

        :param expand_input: The input to expand against.
        :param strict: Whether to enforce strict argument checking.
        :param apply_upstream_relationship: Whether to apply upstream relationships.
        :return: A MappedOperator instance.
        """


@attrs.define(kw_only=True, repr=False)
class PartitionedOperator(PartitionableOperator[OperatorPartial]):
    """
    Concrete implementation of PartitionableOperator for classic (non-decorated) operators.

    This class wraps an OperatorPartial and provides partitioned expansion and iteration logic
    for classic Airflow operators. It enables mapping tasks over partitions of data, supporting
    both direct expansion via keyword arguments and expansion via a list of dictionaries or XComArg.

    :param operator_partial: The OperatorPartial instance to be partitioned and expanded.
    :param size: The number of partitions to create for mapping.
    """

    @property
    def params(self) -> ParamsDict | dict:
        return self.operator_partial.params

    @property
    def _expand_called(self) -> bool:
        return self.operator_partial._expand_called

    @_expand_called.setter
    def _expand_called(self, value: bool) -> None:
        self.operator_partial._expand_called = value

    def iterate(self, **mapped_kwargs: OperatorExpandArgument) -> IterableOperator | MappedIterableOperator:
        if not mapped_kwargs:
            raise TypeError("no arguments to iterate against")

        validate_mapping_kwargs(self.operator_class, "iterate", mapped_kwargs)
        prevent_duplicates(
            self.kwargs,
            mapped_kwargs,
            fail_reason="unmappable or already specified",
        )
        # Since the input is already checked at parse time, we can set strict
        # to False to skip the checks on execution.
        expand_input = DictOfListsExpandInput(mapped_kwargs)
        return self._iterate(expand_input, strict=False)

    def iterate_kwargs(
        self, kwargs: OperatorExpandKwargsArgument, *, strict: bool = True
    ) -> IterableOperator | MappedIterableOperator:
        if isinstance(kwargs, Sequence):
            for item in kwargs:
                if not isinstance(item, (XComArg, Mapping)):
                    raise TypeError(f"expected XComArg or list[dict], not {type(kwargs).__name__}")
        elif not isinstance(kwargs, XComArg):
            raise TypeError(f"expected XComArg or list[dict], not {type(kwargs).__name__}")

        expand_input = ListOfDictsExpandInput(kwargs)
        return self._iterate(expand_input, strict=strict)

    def _iterate(
        self,
        expand_input: ExpandInput,
        *,
        strict: bool,
    ) -> IterableOperator | MappedIterableOperator:
        from airflow.sdk.definitions.iterableoperator import IterableOperator, MappedIterableOperator

        operator = self._expand(expand_input, strict=strict, apply_upstream_relationship=False)

        if self.size > 0:
            return MappedIterableOperator(
                mapped_operator=operator,
                expand_input=expand_input,
                partition_size=self.size,
            )
        return IterableOperator(
            operator=operator,
            expand_input=expand_input,
        )

    def _expand(
        self,
        expand_input: ExpandInput,
        *,
        strict: bool,
        apply_upstream_relationship: bool = True,
    ) -> MappedOperator:
        from airflow.providers.standard.operators.empty import EmptyOperator
        from airflow.providers.standard.utils.skipmixin import SkipMixin
        from airflow.sdk import BaseSensorOperator

        self._expand_called = True
        ensure_xcomarg_return_value(expand_input.value)

        partial_kwargs = self.kwargs.copy()
        task_id = partial_kwargs.pop("task_id")
        dag = partial_kwargs.pop("dag")
        task_group = partial_kwargs.pop("task_group")
        start_date = partial_kwargs.pop("start_date", None)
        end_date = partial_kwargs.pop("end_date", None)
        start_from_trigger = (
            partial_kwargs["start_from_trigger"]
            if "start_from_trigger" in partial_kwargs
            else getattr(self.operator_class, "start_from_trigger", False)
        )
        start_trigger_args = (
            partial_kwargs["start_trigger_args"]
            if "start_trigger_args" in partial_kwargs
            else getattr(self.operator_class, "start_trigger_args", None)
        )

        try:
            operator_name = self.operator_class.custom_operator_name  # type: ignore
        except AttributeError:
            operator_name = self.operator_class.__name__

        return MappedOperator(
            operator_class=self.operator_class,
            expand_input=expand_input,
            partial_kwargs=partial_kwargs,
            task_id=task_id,
            params=self.params,
            operator_extra_links=self.operator_class.operator_extra_links,
            template_ext=self.operator_class.template_ext,
            template_fields=self.operator_class.template_fields,
            template_fields_renderers=self.operator_class.template_fields_renderers,
            ui_color=self.operator_class.ui_color,
            ui_fgcolor=self.operator_class.ui_fgcolor,
            is_empty=issubclass(self.operator_class, EmptyOperator),
            is_sensor=issubclass(self.operator_class, BaseSensorOperator),
            can_skip_downstream=issubclass(self.operator_class, SkipMixin),
            task_module=self.operator_class.__module__,
            task_type=self.operator_class.__name__,
            operator_name=operator_name,
            dag=dag,
            task_group=task_group,
            start_date=start_date,
            end_date=end_date,
            disallow_kwargs_override=strict,
            # For classic operators, this points to expand_input because kwargs
            # to BaseOperator.expand() contribute to operator arguments.
            expand_input_attr="expand_input",
            start_from_trigger=start_from_trigger,
            start_trigger_args=start_trigger_args,
            apply_upstream_relationship=apply_upstream_relationship,
        )


@attrs.define(kw_only=True, repr=False)
class DecoratedPartitionedOperator(PartitionableOperator[_TaskDecorator]):
    """
    Concrete implementation of PartitionableOperator for decorated (TaskFlow) operators.

    This class wraps a _TaskDecorator and provides partitioned expansion and iteration logic
    for TaskFlow-style decorated Airflow operators. It enables mapping decorated tasks over
    partitions of data, returning XComArg objects for downstream dependencies and supporting
    both direct expansion via keyword arguments and expansion via a list of dictionaries or XComArg.

    :param operator_partial: The _TaskDecorator instance to be partitioned and expanded.
    :param size: The number of partitions to create for mapping.
    """

    @property
    def is_setup(self) -> bool:
        return self.operator_partial.is_setup

    @property
    def is_teardown(self) -> bool:
        return self.operator_partial.is_teardown

    @property
    def function(self) -> Callable[FParams, FReturn]:
        return self.operator_partial.function

    @property
    def operator_class(self) -> type[OperatorSubclass]:
        return self.operator_partial.operator_class

    @property
    def multiple_outputs(self) -> bool:
        return self.operator_partial.multiple_outputs

    @property
    def on_failure_fail_dagrun(self) -> bool:
        return self.operator_partial.on_failure_fail_dagrun

    def _validate_arg_names(self, func: ValidationSource, kwargs: dict[str, Any]):
        self.operator_partial._validate_arg_names(func, kwargs)

    def iterate(self, **map_kwargs: OperatorExpandArgument) -> XComArg:
        if self.kwargs.get("trigger_rule") == TriggerRule.ALWAYS and any(
            [isinstance(expanded, XComArg) for expanded in map_kwargs.values()]
        ):
            raise ValueError(
                "Task-generated iterating within a task using 'iterate' is not allowed with trigger rule 'always'."
            )
        if not map_kwargs:
            raise TypeError("no arguments to expand against")
        self._validate_arg_names("expand", map_kwargs)
        prevent_duplicates(self.kwargs, map_kwargs, fail_reason="mapping already partial")
        # Since the input is already checked at parse time, we can set strict
        # to False to skip the checks on execution.
        if self.is_teardown:
            if "trigger_rule" in self.kwargs:
                raise ValueError("Trigger rule not configurable for teardown tasks.")
            self.kwargs.update(trigger_rule=TriggerRule.ALL_DONE_SETUP_SUCCESS)
        expand_input = DictOfListsExpandInput(map_kwargs)
        operator = self._iterate(expand_input, strict=False)
        return XComArg(operator=operator)

    def iterate_kwargs(self, kwargs: OperatorExpandKwargsArgument, *, strict: bool = True) -> XComArg:
        if (
            self.kwargs.get("trigger_rule") == TriggerRule.ALWAYS
            and not isinstance(kwargs, XComArg)
            and any(
                [
                    isinstance(v, XComArg)
                    for kwarg in kwargs
                    if not isinstance(kwarg, XComArg)
                    for v in kwarg.values()
                ]
            )
        ):
            raise ValueError(
                "Task-generated iterating within a task using 'iterate_kwargs' is not allowed with trigger rule 'always'."
            )
        if isinstance(kwargs, Sequence):
            for item in kwargs:
                if not isinstance(item, (XComArg, Mapping)):
                    raise TypeError(f"expected XComArg or list[dict], not {type(kwargs).__name__}")
        elif not isinstance(kwargs, XComArg):
            raise TypeError(f"expected XComArg or list[dict], not {type(kwargs).__name__}")
        expand_input = ListOfDictsExpandInput(kwargs)
        operator = self._iterate(expand_input, strict=strict)
        return XComArg(operator=operator)

    def _iterate(
        self,
        expand_input: ExpandInput,
        *,
        strict: bool,
    ) -> IterableOperator | MappedIterableOperator:
        from airflow.sdk.definitions.iterableoperator import IterableOperator, MappedIterableOperator

        operator = self._expand(expand_input, strict=strict, apply_upstream_relationship=False)

        if self.size > 0:
            return MappedIterableOperator(
                mapped_operator=operator,
                expand_input=DecoratedExpandInput(expand_input),
                partition_size=self.size,
            )
        return IterableOperator(operator=operator, expand_input=DecoratedExpandInput(expand_input))

    def _expand(
        self,
        expand_input: ExpandInput,
        *,
        strict: bool,
        apply_upstream_relationship: bool = True,
    ) -> MappedOperator:
        ensure_xcomarg_return_value(expand_input.value)

        task_kwargs = self.kwargs.copy()
        dag = task_kwargs.pop("dag", None) or DagContext.get_current()
        task_group = task_kwargs.pop("task_group", None) or TaskGroupContext.get_current(dag)

        default_args, partial_params = get_merged_defaults(
            dag=dag,
            task_group=task_group,
            task_params=task_kwargs.pop("params", None),
            task_default_args=task_kwargs.pop("default_args", None),
        )
        partial_kwargs: dict[str, Any] = {
            "is_setup": self.is_setup,
            "is_teardown": self.is_teardown,
            "on_failure_fail_dagrun": self.on_failure_fail_dagrun,
        }
        base_signature = inspect.signature(BaseOperator)
        ignore = {
            "default_args",  # This is target we are working on now.
            "kwargs",  # A common name for a keyword argument.
            "do_xcom_push",  # In the same boat as `multiple_outputs`
            "multiple_outputs",  # We will use `self.multiple_outputs` instead.
            "params",  # Already handled above `partial_params`.
            "task_concurrency",  # Deprecated(replaced by `max_active_tis_per_dag`).
        }
        partial_keys = set(base_signature.parameters) - ignore
        partial_kwargs.update({key: value for key, value in default_args.items() if key in partial_keys})
        partial_kwargs.update(task_kwargs)

        task_id = get_unique_task_id(partial_kwargs.pop("task_id"), dag, task_group)
        if task_group:
            task_id = task_group.child_id(task_id)

        # Logic here should be kept in sync with BaseOperatorMeta.partial().
        if partial_kwargs.get("wait_for_downstream"):
            partial_kwargs["depends_on_past"] = True
        start_date = timezone.convert_to_utc(partial_kwargs.pop("start_date", None))
        end_date = timezone.convert_to_utc(partial_kwargs.pop("end_date", None))
        if "pool_slots" in partial_kwargs:
            if partial_kwargs["pool_slots"] < 1:
                dag_str = ""
                if dag:
                    dag_str = f" in dag {dag.dag_id}"
                raise ValueError(f"pool slots for {task_id}{dag_str} cannot be less than 1")

        for fld, convert in (
            ("retries", parse_retries),
            ("retry_delay", coerce_timedelta),
            ("max_retry_delay", coerce_timedelta),
            ("resources", coerce_resources),
        ):
            if (v := partial_kwargs.get(fld, NOTSET)) is not NOTSET:
                partial_kwargs[fld] = convert(v)

        partial_kwargs.setdefault("executor_config", {})
        partial_kwargs.setdefault("op_args", [])
        partial_kwargs.setdefault("op_kwargs", {})

        try:
            operator_name = self.operator_class.custom_operator_name  # type: ignore
        except AttributeError:
            operator_name = self.operator_class.__name__

        return DecoratedMappedOperator(
            operator_class=self.operator_class,
            expand_input=EXPAND_INPUT_EMPTY,  # Don't use this; mapped values go to op_kwargs_expand_input.
            partial_kwargs=partial_kwargs,
            task_id=task_id,
            params=partial_params,
            operator_extra_links=self.operator_class.operator_extra_links,
            template_ext=self.operator_class.template_ext,
            template_fields=self.operator_class.template_fields,
            template_fields_renderers=self.operator_class.template_fields_renderers,
            ui_color=self.operator_class.ui_color,
            ui_fgcolor=self.operator_class.ui_fgcolor,
            is_empty=False,
            is_sensor=self.operator_class._is_sensor,
            can_skip_downstream=self.operator_class._can_skip_downstream,
            task_module=self.operator_class.__module__,
            task_type=self.operator_class.__name__,
            operator_name=operator_name,
            dag=dag,
            task_group=task_group,
            start_date=start_date,
            end_date=end_date,
            multiple_outputs=self.multiple_outputs,
            python_callable=self.function,
            op_kwargs_expand_input=expand_input,
            disallow_kwargs_override=strict,
            # Different from classic operators, kwargs passed to a taskflow
            # task's expand() contribute to the op_kwargs operator argument, not
            # the operator arguments themselves, and should expand against it.
            expand_input_attr="op_kwargs_expand_input",
            start_trigger_args=self.operator_class.start_trigger_args,
            start_from_trigger=self.operator_class.start_from_trigger,
            apply_upstream_relationship=apply_upstream_relationship,
        )

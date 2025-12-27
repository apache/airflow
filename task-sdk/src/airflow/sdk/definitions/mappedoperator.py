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

import contextlib
import copy
import warnings
from collections.abc import Collection, Iterable, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypeGuard

import attrs
import methodtools
from lazy_object_proxy import Proxy

from airflow.sdk.bases.xcom import BaseXCom
from airflow.sdk.definitions._internal.abstractoperator import (
    DEFAULT_EXECUTOR,
    DEFAULT_IGNORE_FIRST_DEPENDS_ON_PAST,
    DEFAULT_OWNER,
    DEFAULT_POOL_NAME,
    DEFAULT_POOL_SLOTS,
    DEFAULT_PRIORITY_WEIGHT,
    DEFAULT_QUEUE,
    DEFAULT_RETRIES,
    DEFAULT_RETRY_DELAY,
    DEFAULT_TRIGGER_RULE,
    DEFAULT_WAIT_FOR_PAST_DEPENDS_BEFORE_SKIPPING,
    DEFAULT_WEIGHT_RULE,
    AbstractOperator,
    TaskStateChangeCallbackAttrType,
)
from airflow.sdk.definitions._internal.expandinput import (
    DictOfListsExpandInput,
    ListOfDictsExpandInput,
    is_mappable,
)
from airflow.sdk.definitions._internal.types import NOTSET
from airflow.serialization.enums import DagAttributeTypes
from airflow.task.priority_strategy import PriorityWeightStrategy, validate_and_load_priority_weight_strategy

if TYPE_CHECKING:
    import datetime

    import jinja2  # Slow import.
    import pendulum

    from airflow.sdk import DAG, BaseOperator, BaseOperatorLink, Context, TaskGroup, TriggerRule, XComArg
    from airflow.sdk.definitions._internal.expandinput import (
        ExpandInput,
        OperatorExpandArgument,
        OperatorExpandKwargsArgument,
    )
    from airflow.sdk.definitions.operator_resources import Resources
    from airflow.sdk.definitions.param import ParamsDict
    from airflow.triggers.base import StartTriggerArgs

ValidationSource = Literal["expand"] | Literal["partial"]


def validate_mapping_kwargs(op: type[BaseOperator], func: ValidationSource, value: dict[str, Any]) -> None:
    # use a dict so order of args is same as code order
    unknown_args = value.copy()
    for klass in op.mro():
        init = klass.__init__  # type: ignore[misc]
        try:
            param_names = init._BaseOperatorMeta__param_names
        except AttributeError:
            continue
        for name in param_names:
            value = unknown_args.pop(name, NOTSET)
            if func != "expand":
                continue
            if value is NOTSET:
                continue
            if is_mappable(value):
                continue
            type_name = type(value).__name__
            error = f"{op.__name__}.expand() got an unexpected type {type_name!r} for keyword argument {name}"
            raise ValueError(error)
        if not unknown_args:
            return  # If we have no args left to check: stop looking at the MRO chain.

    if len(unknown_args) == 1:
        error = f"an unexpected keyword argument {unknown_args.popitem()[0]!r}"
    else:
        names = ", ".join(repr(n) for n in unknown_args)
        error = f"unexpected keyword arguments {names}"
    raise TypeError(f"{op.__name__}.{func}() got {error}")


def _is_container(obj: Any) -> bool:
    """Test if an object is a container (iterable) but not a string."""
    if isinstance(obj, Proxy):
        # Proxy of any object is considered a container because it implements __iter__
        # to forward the call to the lazily initialized object
        # Unwrap Proxy before checking __iter__ to evaluate the proxied object
        obj = obj.__wrapped__
    return hasattr(obj, "__iter__") and not isinstance(obj, str)


def ensure_xcomarg_return_value(arg: Any) -> None:
    from airflow.sdk.definitions.xcom_arg import XComArg

    if isinstance(arg, XComArg):
        for operator, key in arg.iter_references():
            if key != BaseXCom.XCOM_RETURN_KEY:
                raise ValueError(f"cannot map over XCom with custom key {key!r} from {operator}")
    elif not _is_container(arg):
        return
    elif isinstance(arg, Mapping):
        for v in arg.values():
            ensure_xcomarg_return_value(v)
    elif isinstance(arg, Iterable):
        for v in arg:
            ensure_xcomarg_return_value(v)


def is_mappable_value(value: Any) -> TypeGuard[Collection]:
    """
    Whether a value can be used for task mapping.

    We only allow collections with guaranteed ordering, but exclude character
    sequences since that's usually not what users would expect to be mappable.

    :meta private:
    """
    if not isinstance(value, (Sequence, dict)):
        return False
    if isinstance(value, (bytearray, bytes, str)):
        return False
    return True


def prevent_duplicates(kwargs1: dict[str, Any], kwargs2: Mapping[str, Any], *, fail_reason: str) -> None:
    """
    Ensure *kwargs1* and *kwargs2* do not contain common keys.

    :raises TypeError: If common keys are found.
    """
    duplicated_keys = set(kwargs1).intersection(kwargs2)
    if not duplicated_keys:
        return
    if len(duplicated_keys) == 1:
        raise TypeError(f"{fail_reason} argument: {duplicated_keys.pop()}")
    duplicated_keys_display = ", ".join(sorted(duplicated_keys))
    raise TypeError(f"{fail_reason} arguments: {duplicated_keys_display}")


@attrs.define(kw_only=True, repr=False)
class OperatorPartial:
    """
    An "intermediate state" returned by ``BaseOperator.partial()``.

    This only exists at Dag-parsing time; the only intended usage is for the
    user to call ``.expand()`` on it at some point (usually in a method chain) to
    create a ``MappedOperator`` to add into the Dag.
    """

    operator_class: type[BaseOperator]
    kwargs: dict[str, Any]
    params: ParamsDict | dict

    _expand_called: bool = False  # Set when expand() is called to ease user debugging.

    def __attrs_post_init__(self):
        validate_mapping_kwargs(self.operator_class, "partial", self.kwargs)

    def __repr__(self) -> str:
        args = ", ".join(f"{k}={v!r}" for k, v in self.kwargs.items())
        return f"{self.operator_class.__name__}.partial({args})"

    def __del__(self):
        if not self._expand_called:
            try:
                task_id = repr(self.kwargs["task_id"])
            except KeyError:
                task_id = f"at {hex(id(self))}"
            warnings.warn(f"Task {task_id} was never mapped!", category=UserWarning, stacklevel=1)

    def expand(self, **mapped_kwargs: OperatorExpandArgument) -> MappedOperator:
        if not mapped_kwargs:
            raise TypeError("no arguments to expand against")
        validate_mapping_kwargs(self.operator_class, "expand", mapped_kwargs)
        prevent_duplicates(self.kwargs, mapped_kwargs, fail_reason="unmappable or already specified")
        # Since the input is already checked at parse time, we can set strict
        # to False to skip the checks on execution.
        return self._expand(DictOfListsExpandInput(mapped_kwargs), strict=False)

    def expand_kwargs(self, kwargs: OperatorExpandKwargsArgument, *, strict: bool = True) -> MappedOperator:
        from airflow.sdk.definitions.xcom_arg import XComArg

        if isinstance(kwargs, Sequence):
            for item in kwargs:
                if not isinstance(item, (XComArg, Mapping)):
                    raise TypeError(f"expected XComArg or list[dict], not {type(kwargs).__name__}")
        elif not isinstance(kwargs, XComArg):
            raise TypeError(f"expected XComArg or list[dict], not {type(kwargs).__name__}")
        return self._expand(ListOfDictsExpandInput(kwargs), strict=strict)

    def _expand(self, expand_input: ExpandInput, *, strict: bool) -> MappedOperator:
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
        start_from_trigger = bool(
            partial_kwargs.get("start_from_trigger", False)
            or getattr(self.operator_class, "start_from_trigger", False)
        )
        start_trigger_args = partial_kwargs.get("start_trigger_args", None) or getattr(
            self.operator_class, "start_trigger_args", None
        )

        try:
            operator_name = self.operator_class.custom_operator_name  # type: ignore
        except AttributeError:
            operator_name = self.operator_class.__name__

        op = MappedOperator(
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
            # TODO: Move these to task SDK's BaseOperator and remove getattr
            start_trigger_args=start_trigger_args,
            start_from_trigger=start_from_trigger,
        )
        return op


@attrs.define(
    kw_only=True,
    # Disable custom __getstate__ and __setstate__ generation since it interacts
    # badly with Airflow's Dag serialization and pickling. When a mapped task is
    # deserialized, subclasses are coerced into MappedOperator, but when it goes
    # through Dag pickling, all attributes defined in the subclasses are dropped
    # by attrs's custom state management. Since attrs does not do anything too
    # special here (the logic is only important for slots=True), we use Python's
    # built-in implementation, which works (as proven by good old BaseOperator).
    getstate_setstate=False,
)
class MappedOperator(AbstractOperator):
    """Object representing a mapped operator in a Dag."""

    operator_class: type[BaseOperator]

    _is_mapped: bool = attrs.field(init=False, default=True)

    expand_input: ExpandInput
    partial_kwargs: dict[str, Any]

    # Needed for serialization.
    task_id: str
    params: ParamsDict | dict
    operator_extra_links: Collection[BaseOperatorLink]
    template_ext: Sequence[str]
    template_fields: Collection[str]
    template_fields_renderers: dict[str, str]
    ui_color: str
    ui_fgcolor: str
    _is_empty: bool = attrs.field(alias="is_empty")
    _can_skip_downstream: bool = attrs.field(alias="can_skip_downstream")
    _is_sensor: bool = attrs.field(alias="is_sensor", default=False)
    _task_module: str
    task_type: str
    _operator_name: str
    start_trigger_args: StartTriggerArgs | None
    start_from_trigger: bool
    _needs_expansion: bool = True

    dag: DAG | None
    task_group: TaskGroup | None
    start_date: pendulum.DateTime | None
    end_date: pendulum.DateTime | None
    upstream_task_ids: set[str] = attrs.field(factory=set, init=False)
    downstream_task_ids: set[str] = attrs.field(factory=set, init=False)

    _disallow_kwargs_override: bool
    """Whether execution fails if ``expand_input`` has duplicates to ``partial_kwargs``.

    If *False*, values from ``expand_input`` under duplicate keys override those
    under corresponding keys in ``partial_kwargs``.
    """

    _expand_input_attr: str
    """Where to get kwargs to calculate expansion length against.

    This should be a name to call ``getattr()`` on.
    """

    HIDE_ATTRS_FROM_UI: ClassVar[frozenset[str]] = AbstractOperator.HIDE_ATTRS_FROM_UI | frozenset(
        ("parse_time_mapped_ti_count", "operator_class", "start_trigger_args", "start_from_trigger")
    )

    def __hash__(self):
        return id(self)

    def __repr__(self):
        return f"<Mapped({self.task_type}): {self.task_id}>"

    def __attrs_post_init__(self):
        from airflow.sdk.definitions.xcom_arg import XComArg

        if self.get_closest_mapped_task_group() is not None:
            raise NotImplementedError("operator expansion in an expanded task group is not yet supported")

        if self.task_group:
            self.task_group.add(self)
        if self.dag:
            self.dag.add_task(self)
        XComArg.apply_upstream_relationship(self, self._get_specified_expand_input().value)
        for k, v in self.partial_kwargs.items():
            if k in self.template_fields:
                XComArg.apply_upstream_relationship(self, v)

    @methodtools.lru_cache(maxsize=None)
    @classmethod
    def get_serialized_fields(cls):
        # Not using 'cls' here since we only want to serialize base fields.
        return (frozenset(attrs.fields_dict(MappedOperator))) - {
            "_is_empty",
            "_can_skip_downstream",
            "dag",
            "deps",
            "expand_input",  # This is needed to be able to accept XComArg.
            "task_group",
            "upstream_task_ids",
            "_is_setup",
            "_is_teardown",
            "_on_failure_fail_dagrun",
            "operator_class",
            "_needs_expansion",
            "partial_kwargs",
            "operator_extra_links",
        }

    @property
    def operator_name(self) -> str:
        return self._operator_name

    @property
    def roots(self) -> Sequence[AbstractOperator]:
        """Implementing DAGNode."""
        return [self]

    @property
    def leaves(self) -> Sequence[AbstractOperator]:
        """Implementing DAGNode."""
        return [self]

    @property
    def task_display_name(self) -> str:
        return self.partial_kwargs.get("task_display_name") or self.task_id

    @property
    def owner(self) -> str:
        return self.partial_kwargs.get("owner", DEFAULT_OWNER)

    @owner.setter
    def owner(self, value: str) -> None:
        self.partial_kwargs["owner"] = value

    @property
    def email(self) -> None | str | Iterable[str]:
        return self.partial_kwargs.get("email")

    @property
    def email_on_failure(self) -> bool:
        return self.partial_kwargs.get("email_on_failure", True)

    @property
    def email_on_retry(self) -> bool:
        return self.partial_kwargs.get("email_on_retry", True)

    @property
    def map_index_template(self) -> None | str:
        return self.partial_kwargs.get("map_index_template")

    @map_index_template.setter
    def map_index_template(self, value: str | None) -> None:
        self.partial_kwargs["map_index_template"] = value

    @property
    def trigger_rule(self) -> TriggerRule:
        return self.partial_kwargs.get("trigger_rule", DEFAULT_TRIGGER_RULE)

    @trigger_rule.setter
    def trigger_rule(self, value):
        self.partial_kwargs["trigger_rule"] = value

    @property
    def is_setup(self) -> bool:
        return bool(self.partial_kwargs.get("is_setup"))

    @is_setup.setter
    def is_setup(self, value: bool) -> None:
        self.partial_kwargs["is_setup"] = value

    @property
    def is_teardown(self) -> bool:
        return bool(self.partial_kwargs.get("is_teardown"))

    @is_teardown.setter
    def is_teardown(self, value: bool) -> None:
        self.partial_kwargs["is_teardown"] = value

    @property
    def depends_on_past(self) -> bool:
        return bool(self.partial_kwargs.get("depends_on_past"))

    @depends_on_past.setter
    def depends_on_past(self, value: bool) -> None:
        self.partial_kwargs["depends_on_past"] = value

    @property
    def ignore_first_depends_on_past(self) -> bool:
        value = self.partial_kwargs.get("ignore_first_depends_on_past", DEFAULT_IGNORE_FIRST_DEPENDS_ON_PAST)
        return bool(value)

    @ignore_first_depends_on_past.setter
    def ignore_first_depends_on_past(self, value: bool) -> None:
        self.partial_kwargs["ignore_first_depends_on_past"] = value

    @property
    def wait_for_past_depends_before_skipping(self) -> bool:
        value = self.partial_kwargs.get(
            "wait_for_past_depends_before_skipping", DEFAULT_WAIT_FOR_PAST_DEPENDS_BEFORE_SKIPPING
        )
        return bool(value)

    @wait_for_past_depends_before_skipping.setter
    def wait_for_past_depends_before_skipping(self, value: bool) -> None:
        self.partial_kwargs["wait_for_past_depends_before_skipping"] = value

    @property
    def wait_for_downstream(self) -> bool:
        return bool(self.partial_kwargs.get("wait_for_downstream"))

    @wait_for_downstream.setter
    def wait_for_downstream(self, value: bool) -> None:
        self.partial_kwargs["wait_for_downstream"] = value

    @property
    def retries(self) -> int:
        return self.partial_kwargs.get("retries", DEFAULT_RETRIES)

    @retries.setter
    def retries(self, value: int) -> None:
        self.partial_kwargs["retries"] = value

    @property
    def queue(self) -> str:
        return self.partial_kwargs.get("queue", DEFAULT_QUEUE)

    @queue.setter
    def queue(self, value: str) -> None:
        self.partial_kwargs["queue"] = value

    @property
    def pool(self) -> str:
        return self.partial_kwargs.get("pool", DEFAULT_POOL_NAME)

    @pool.setter
    def pool(self, value: str) -> None:
        self.partial_kwargs["pool"] = value

    @property
    def pool_slots(self) -> int:
        return self.partial_kwargs.get("pool_slots", DEFAULT_POOL_SLOTS)

    @pool_slots.setter
    def pool_slots(self, value: int) -> None:
        self.partial_kwargs["pool_slots"] = value

    @property
    def execution_timeout(self) -> datetime.timedelta | None:
        return self.partial_kwargs.get("execution_timeout")

    @execution_timeout.setter
    def execution_timeout(self, value: datetime.timedelta | None) -> None:
        self.partial_kwargs["execution_timeout"] = value

    @property
    def max_retry_delay(self) -> datetime.timedelta | None:
        return self.partial_kwargs.get("max_retry_delay")

    @max_retry_delay.setter
    def max_retry_delay(self, value: datetime.timedelta | None) -> None:
        self.partial_kwargs["max_retry_delay"] = value

    @property
    def retry_delay(self) -> datetime.timedelta:
        return self.partial_kwargs.get("retry_delay", DEFAULT_RETRY_DELAY)

    @retry_delay.setter
    def retry_delay(self, value: datetime.timedelta) -> None:
        self.partial_kwargs["retry_delay"] = value

    @property
    def retry_exponential_backoff(self) -> float:
        value = self.partial_kwargs.get("retry_exponential_backoff", 0)
        if value is True:
            return 2.0
        if value is False:
            return 0.0
        return float(value)

    @retry_exponential_backoff.setter
    def retry_exponential_backoff(self, value: float) -> None:
        self.partial_kwargs["retry_exponential_backoff"] = value

    @property
    def priority_weight(self) -> int:
        return self.partial_kwargs.get("priority_weight", DEFAULT_PRIORITY_WEIGHT)

    @priority_weight.setter
    def priority_weight(self, value: int) -> None:
        self.partial_kwargs["priority_weight"] = value

    @property
    def weight_rule(self) -> PriorityWeightStrategy:
        return validate_and_load_priority_weight_strategy(
            self.partial_kwargs.get("weight_rule", DEFAULT_WEIGHT_RULE)
        )

    @weight_rule.setter
    def weight_rule(self, value: str | PriorityWeightStrategy) -> None:
        self.partial_kwargs["weight_rule"] = validate_and_load_priority_weight_strategy(value)

    @property
    def max_active_tis_per_dag(self) -> int | None:
        return self.partial_kwargs.get("max_active_tis_per_dag")

    @max_active_tis_per_dag.setter
    def max_active_tis_per_dag(self, value: int | None) -> None:
        self.partial_kwargs["max_active_tis_per_dag"] = value

    @property
    def max_active_tis_per_dagrun(self) -> int | None:
        return self.partial_kwargs.get("max_active_tis_per_dagrun")

    @max_active_tis_per_dagrun.setter
    def max_active_tis_per_dagrun(self, value: int | None) -> None:
        self.partial_kwargs["max_active_tis_per_dagrun"] = value

    @property
    def resources(self) -> Resources | None:
        return self.partial_kwargs.get("resources")

    @property
    def on_execute_callback(self) -> TaskStateChangeCallbackAttrType:
        return self.partial_kwargs.get("on_execute_callback") or []

    @on_execute_callback.setter
    def on_execute_callback(self, value: TaskStateChangeCallbackAttrType) -> None:
        self.partial_kwargs["on_execute_callback"] = value or []

    @property
    def on_failure_callback(self) -> TaskStateChangeCallbackAttrType:
        return self.partial_kwargs.get("on_failure_callback") or []

    @on_failure_callback.setter
    def on_failure_callback(self, value: TaskStateChangeCallbackAttrType) -> None:
        self.partial_kwargs["on_failure_callback"] = value or []

    @property
    def on_retry_callback(self) -> TaskStateChangeCallbackAttrType:
        return self.partial_kwargs.get("on_retry_callback") or []

    @on_retry_callback.setter
    def on_retry_callback(self, value: TaskStateChangeCallbackAttrType) -> None:
        self.partial_kwargs["on_retry_callback"] = value or []

    @property
    def on_success_callback(self) -> TaskStateChangeCallbackAttrType:
        return self.partial_kwargs.get("on_success_callback") or []

    @on_success_callback.setter
    def on_success_callback(self, value: TaskStateChangeCallbackAttrType) -> None:
        self.partial_kwargs["on_success_callback"] = value or []

    @property
    def on_skipped_callback(self) -> TaskStateChangeCallbackAttrType:
        return self.partial_kwargs.get("on_skipped_callback") or []

    @on_skipped_callback.setter
    def on_skipped_callback(self, value: TaskStateChangeCallbackAttrType) -> None:
        self.partial_kwargs["on_skipped_callback"] = value or []

    @property
    def has_on_execute_callback(self) -> bool:
        return bool(self.on_execute_callback)

    @property
    def has_on_failure_callback(self) -> bool:
        return bool(self.on_failure_callback)

    @property
    def has_on_retry_callback(self) -> bool:
        return bool(self.on_retry_callback)

    @property
    def has_on_success_callback(self) -> bool:
        return bool(self.on_success_callback)

    @property
    def has_on_skipped_callback(self) -> bool:
        return bool(self.on_skipped_callback)

    @property
    def run_as_user(self) -> str | None:
        return self.partial_kwargs.get("run_as_user")

    @property
    def executor(self) -> str | None:
        return self.partial_kwargs.get("executor", DEFAULT_EXECUTOR)

    @property
    def executor_config(self) -> dict:
        return self.partial_kwargs.get("executor_config", {})

    @property
    def inlets(self) -> list[Any]:
        return self.partial_kwargs.get("inlets", [])

    @inlets.setter
    def inlets(self, value: list[Any]) -> None:
        self.partial_kwargs["inlets"] = value

    @property
    def outlets(self) -> list[Any]:
        return self.partial_kwargs.get("outlets", [])

    @outlets.setter
    def outlets(self, value: list[Any]) -> None:
        self.partial_kwargs["outlets"] = value

    @property
    def doc(self) -> str | None:
        return self.partial_kwargs.get("doc")

    @property
    def doc_md(self) -> str | None:
        return self.partial_kwargs.get("doc_md")

    @property
    def doc_json(self) -> str | None:
        return self.partial_kwargs.get("doc_json")

    @property
    def doc_yaml(self) -> str | None:
        return self.partial_kwargs.get("doc_yaml")

    @property
    def doc_rst(self) -> str | None:
        return self.partial_kwargs.get("doc_rst")

    @property
    def allow_nested_operators(self) -> bool:
        return bool(self.partial_kwargs.get("allow_nested_operators"))

    def get_dag(self) -> DAG | None:
        """Implement Operator."""
        return self.dag

    @property
    def output(self) -> XComArg:
        """Return reference to XCom pushed by current operator."""
        from airflow.sdk.definitions.xcom_arg import XComArg

        return XComArg(operator=self)

    def serialize_for_task_group(self) -> tuple[DagAttributeTypes, Any]:
        """Implement DAGNode."""
        return DagAttributeTypes.OP, self.task_id

    def _expand_mapped_kwargs(self, context: Mapping[str, Any]) -> tuple[Mapping[str, Any], set[int]]:
        """
        Get the kwargs to create the unmapped operator.

        This exists because taskflow operators expand against op_kwargs, not the
        entire operator kwargs dict.
        """
        return self._get_specified_expand_input().resolve(context)

    def _get_unmap_kwargs(self, mapped_kwargs: Mapping[str, Any], *, strict: bool) -> dict[str, Any]:
        """
        Get init kwargs to unmap the underlying operator class.

        :param mapped_kwargs: The dict returned by ``_expand_mapped_kwargs``.
        """
        if strict:
            prevent_duplicates(
                self.partial_kwargs,
                mapped_kwargs,
                fail_reason="unmappable or already specified",
            )

        # If params appears in the mapped kwargs, we need to merge it into the
        # partial params, overriding existing keys.
        params = copy.copy(self.params)
        with contextlib.suppress(KeyError):
            params.update(mapped_kwargs["params"])

        # Ordering is significant; mapped kwargs should override partial ones,
        # and the specially handled params should be respected.
        return {
            "task_id": self.task_id,
            "dag": self.dag,
            "task_group": self.task_group,
            "start_date": self.start_date,
            "end_date": self.end_date,
            **self.partial_kwargs,
            **mapped_kwargs,
            "params": params,
        }

    def unmap(self, resolve: None | Mapping[str, Any]) -> BaseOperator:
        """
        Get the "normal" Operator after applying the current mapping.

        :meta private:
        """
        if isinstance(resolve, Mapping):
            kwargs = resolve
        elif resolve is not None:
            kwargs, _ = self._expand_mapped_kwargs(*resolve)
        else:
            raise RuntimeError("cannot unmap a non-serialized operator without context")
        kwargs = self._get_unmap_kwargs(kwargs, strict=self._disallow_kwargs_override)
        is_setup = kwargs.pop("is_setup", False)
        is_teardown = kwargs.pop("is_teardown", False)
        on_failure_fail_dagrun = kwargs.pop("on_failure_fail_dagrun", False)
        kwargs["task_id"] = self.task_id
        op = self.operator_class(**kwargs, _airflow_from_mapped=True)
        op.is_setup = is_setup
        op.is_teardown = is_teardown
        op.on_failure_fail_dagrun = on_failure_fail_dagrun
        op.downstream_task_ids = self.downstream_task_ids
        op.upstream_task_ids = self.upstream_task_ids
        return op

    def _get_specified_expand_input(self) -> ExpandInput:
        """Input received from the expand call on the operator."""
        return getattr(self, self._expand_input_attr)

    def prepare_for_execution(self) -> MappedOperator:
        # Since a mapped operator cannot be used for execution, and an unmapped
        # BaseOperator needs to be created later (see render_template_fields),
        # we don't need to create a copy of the MappedOperator here.
        return self

    # TODO (GH-52141): Do we need this in the SDK?
    def iter_mapped_dependencies(self) -> Iterator[AbstractOperator]:
        """Upstream dependencies that provide XComs used by this task for task mapping."""
        from airflow.sdk.definitions.xcom_arg import XComArg

        for operator, _ in XComArg.iter_xcom_references(self._get_specified_expand_input()):
            yield operator

    def render_template_fields(
        self,
        context: Context,
        jinja_env: jinja2.Environment | None = None,
    ) -> None:
        """
        Template all attributes listed in *self.template_fields*.

        This updates *context* to reference the map-expanded task and relevant
        information, without modifying the mapped operator. The expanded task
        in *context* is then rendered in-place.

        :param context: Context dict with values to apply on content.
        :param jinja_env: Jinja environment to use for rendering.
        """
        from airflow.sdk.execution_time.context import context_update_for_unmapped

        if not jinja_env:
            jinja_env = self.get_template_env()

        mapped_kwargs, seen_oids = self._expand_mapped_kwargs(context)
        unmapped_task = self.unmap(mapped_kwargs)
        context_update_for_unmapped(context, unmapped_task)

        # Since the operators that extend `BaseOperator` are not subclasses of
        # `MappedOperator`, we need to call `_do_render_template_fields` from
        # the unmapped task in order to call the operator method when we override
        # it to customize the parsing of nested fields.
        unmapped_task._do_render_template_fields(
            parent=unmapped_task,
            template_fields=self.template_fields,
            context=context,
            jinja_env=jinja_env,
            seen_oids=seen_oids,
        )

    def expand_start_trigger_args(self, *, context: Context) -> StartTriggerArgs | None:
        """
        Get the kwargs to create the unmapped start_trigger_args.

        This method is for allowing mapped operator to start execution from triggerer.
        """
        from airflow.triggers.base import StartTriggerArgs

        if not self.start_trigger_args:
            return None

        mapped_kwargs, _ = self._expand_mapped_kwargs(context)
        if self._disallow_kwargs_override:
            prevent_duplicates(
                self.partial_kwargs,
                mapped_kwargs,
                fail_reason="unmappable or already specified",
            )

        # Ordering is significant; mapped kwargs should override partial ones.
        trigger_kwargs = mapped_kwargs.get(
            "trigger_kwargs",
            self.partial_kwargs.get("trigger_kwargs", self.start_trigger_args.trigger_kwargs),
        )
        next_kwargs = mapped_kwargs.get(
            "next_kwargs",
            self.partial_kwargs.get("next_kwargs", self.start_trigger_args.next_kwargs),
        )
        timeout = mapped_kwargs.get(
            "trigger_timeout", self.partial_kwargs.get("trigger_timeout", self.start_trigger_args.timeout)
        )
        return StartTriggerArgs(
            trigger_cls=self.start_trigger_args.trigger_cls,
            trigger_kwargs=trigger_kwargs,
            next_method=self.start_trigger_args.next_method,
            next_kwargs=next_kwargs,
            timeout=timeout,
        )

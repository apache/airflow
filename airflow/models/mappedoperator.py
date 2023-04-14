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

import collections
import collections.abc
import contextlib
import copy
import datetime
import warnings
from typing import TYPE_CHECKING, Any, ClassVar, Collection, Iterable, Iterator, Mapping, Sequence, Union

import attr
import pendulum
from sqlalchemy.orm.session import Session

from airflow import settings
from airflow.compat.functools import cache
from airflow.exceptions import AirflowException, UnmappableOperator
from airflow.models.abstractoperator import (
    DEFAULT_IGNORE_FIRST_DEPENDS_ON_PAST,
    DEFAULT_OWNER,
    DEFAULT_POOL_SLOTS,
    DEFAULT_PRIORITY_WEIGHT,
    DEFAULT_QUEUE,
    DEFAULT_RETRIES,
    DEFAULT_RETRY_DELAY,
    DEFAULT_TRIGGER_RULE,
    DEFAULT_WAIT_FOR_PAST_DEPENDS_BEFORE_SKIPPING,
    DEFAULT_WEIGHT_RULE,
    AbstractOperator,
    NotMapped,
    TaskStateChangeCallback,
)
from airflow.models.expandinput import (
    DictOfListsExpandInput,
    ExpandInput,
    ListOfDictsExpandInput,
    OperatorExpandArgument,
    OperatorExpandKwargsArgument,
    is_mappable,
)
from airflow.models.param import ParamsDict
from airflow.models.pool import Pool
from airflow.serialization.enums import DagAttributeTypes
from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.ti_deps.deps.mapped_task_expanded import MappedTaskIsExpanded
from airflow.typing_compat import Literal
from airflow.utils.context import Context, context_update_for_unmapped
from airflow.utils.helpers import is_container, prevent_duplicates
from airflow.utils.operator_resources import Resources
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.types import NOTSET
from airflow.utils.xcom import XCOM_RETURN_KEY

if TYPE_CHECKING:
    import jinja2  # Slow import.

    from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
    from airflow.models.dag import DAG
    from airflow.models.operator import Operator
    from airflow.models.xcom_arg import XComArg
    from airflow.utils.task_group import TaskGroup

ValidationSource = Union[Literal["expand"], Literal["partial"]]


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


def ensure_xcomarg_return_value(arg: Any) -> None:
    from airflow.models.xcom_arg import XComArg

    if isinstance(arg, XComArg):
        for operator, key in arg.iter_references():
            if key != XCOM_RETURN_KEY:
                raise ValueError(f"cannot map over XCom with custom key {key!r} from {operator}")
    elif not is_container(arg):
        return
    elif isinstance(arg, collections.abc.Mapping):
        for v in arg.values():
            ensure_xcomarg_return_value(v)
    elif isinstance(arg, collections.abc.Iterable):
        for v in arg:
            ensure_xcomarg_return_value(v)


@attr.define(kw_only=True, repr=False)
class OperatorPartial:
    """An "intermediate state" returned by ``BaseOperator.partial()``.

    This only exists at DAG-parsing time; the only intended usage is for the
    user to call ``.expand()`` on it at some point (usually in a method chain) to
    create a ``MappedOperator`` to add into the DAG.
    """

    operator_class: type[BaseOperator]
    kwargs: dict[str, Any]
    params: ParamsDict | dict

    _expand_called: bool = False  # Set when expand() is called to ease user debugging.

    def __attrs_post_init__(self):
        from airflow.operators.subdag import SubDagOperator

        if issubclass(self.operator_class, SubDagOperator):
            raise TypeError("Mapping over deprecated SubDagOperator is not supported")
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
            warnings.warn(f"Task {task_id} was never mapped!")

    def expand(self, **mapped_kwargs: OperatorExpandArgument) -> MappedOperator:
        if not mapped_kwargs:
            raise TypeError("no arguments to expand against")
        validate_mapping_kwargs(self.operator_class, "expand", mapped_kwargs)
        prevent_duplicates(self.kwargs, mapped_kwargs, fail_reason="unmappable or already specified")
        # Since the input is already checked at parse time, we can set strict
        # to False to skip the checks on execution.
        return self._expand(DictOfListsExpandInput(mapped_kwargs), strict=False)

    def expand_kwargs(self, kwargs: OperatorExpandKwargsArgument, *, strict: bool = True) -> MappedOperator:
        from airflow.models.xcom_arg import XComArg

        if isinstance(kwargs, collections.abc.Sequence):
            for item in kwargs:
                if not isinstance(item, (XComArg, collections.abc.Mapping)):
                    raise TypeError(f"expected XComArg or list[dict], not {type(kwargs).__name__}")
        elif not isinstance(kwargs, XComArg):
            raise TypeError(f"expected XComArg or list[dict], not {type(kwargs).__name__}")
        return self._expand(ListOfDictsExpandInput(kwargs), strict=strict)

    def _expand(self, expand_input: ExpandInput, *, strict: bool) -> MappedOperator:
        from airflow.operators.empty import EmptyOperator

        self._expand_called = True
        ensure_xcomarg_return_value(expand_input.value)

        partial_kwargs = self.kwargs.copy()
        task_id = partial_kwargs.pop("task_id")
        dag = partial_kwargs.pop("dag")
        task_group = partial_kwargs.pop("task_group")
        start_date = partial_kwargs.pop("start_date")
        end_date = partial_kwargs.pop("end_date")

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
            deps=MappedOperator.deps_for(self.operator_class),
            operator_extra_links=self.operator_class.operator_extra_links,
            template_ext=self.operator_class.template_ext,
            template_fields=self.operator_class.template_fields,
            template_fields_renderers=self.operator_class.template_fields_renderers,
            ui_color=self.operator_class.ui_color,
            ui_fgcolor=self.operator_class.ui_fgcolor,
            is_empty=issubclass(self.operator_class, EmptyOperator),
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
        )
        return op


@attr.define(
    kw_only=True,
    # Disable custom __getstate__ and __setstate__ generation since it interacts
    # badly with Airflow's DAG serialization and pickling. When a mapped task is
    # deserialized, subclasses are coerced into MappedOperator, but when it goes
    # through DAG pickling, all attributes defined in the subclasses are dropped
    # by attrs's custom state management. Since attrs does not do anything too
    # special here (the logic is only important for slots=True), we use Python's
    # built-in implementation, which works (as proven by good old BaseOperator).
    getstate_setstate=False,
)
class MappedOperator(AbstractOperator):
    """Object representing a mapped operator in a DAG."""

    # This attribute serves double purpose. For a "normal" operator instance
    # loaded from DAG, this holds the underlying non-mapped operator class that
    # can be used to create an unmapped operator for execution. For an operator
    # recreated from a serialized DAG, however, this holds the serialized data
    # that can be used to unmap this into a SerializedBaseOperator.
    operator_class: type[BaseOperator] | dict[str, Any]

    expand_input: ExpandInput
    partial_kwargs: dict[str, Any]

    # Needed for serialization.
    task_id: str
    params: ParamsDict | dict
    deps: frozenset[BaseTIDep]
    operator_extra_links: Collection[BaseOperatorLink]
    template_ext: Sequence[str]
    template_fields: Collection[str]
    template_fields_renderers: dict[str, str]
    ui_color: str
    ui_fgcolor: str
    _is_empty: bool
    _task_module: str
    _task_type: str
    _operator_name: str

    dag: DAG | None
    task_group: TaskGroup | None
    start_date: pendulum.DateTime | None
    end_date: pendulum.DateTime | None
    upstream_task_ids: set[str] = attr.ib(factory=set, init=False)
    downstream_task_ids: set[str] = attr.ib(factory=set, init=False)

    _disallow_kwargs_override: bool
    """Whether execution fails if ``expand_input`` has duplicates to ``partial_kwargs``.

    If *False*, values from ``expand_input`` under duplicate keys override those
    under corresponding keys in ``partial_kwargs``.
    """

    _expand_input_attr: str
    """Where to get kwargs to calculate expansion length against.

    This should be a name to call ``getattr()`` on.
    """

    subdag: None = None  # Since we don't support SubDagOperator, this is always None.

    HIDE_ATTRS_FROM_UI: ClassVar[frozenset[str]] = AbstractOperator.HIDE_ATTRS_FROM_UI | frozenset(
        (
            "parse_time_mapped_ti_count",
            "operator_class",
        )
    )

    def __hash__(self):
        return id(self)

    def __repr__(self):
        return f"<Mapped({self._task_type}): {self.task_id}>"

    def __attrs_post_init__(self):
        from airflow.models.xcom_arg import XComArg

        if self.get_closest_mapped_task_group() is not None:
            raise NotImplementedError("operator expansion in an expanded task group is not yet supported")

        if self.task_group:
            self.task_group.add(self)
        if self.dag:
            self.dag.add_task(self)
        XComArg.apply_upstream_relationship(self, self.expand_input.value)
        for k, v in self.partial_kwargs.items():
            if k in self.template_fields:
                XComArg.apply_upstream_relationship(self, v)
        if self.partial_kwargs.get("sla") is not None:
            raise AirflowException(
                f"SLAs are unsupported with mapped tasks. Please set `sla=None` for task "
                f"{self.task_id!r}."
            )

    @classmethod
    @cache
    def get_serialized_fields(cls):
        # Not using 'cls' here since we only want to serialize base fields.
        return frozenset(attr.fields_dict(MappedOperator)) - {
            "dag",
            "deps",
            "expand_input",  # This is needed to be able to accept XComArg.
            "subdag",
            "task_group",
            "upstream_task_ids",
        }

    @staticmethod
    @cache
    def deps_for(operator_class: type[BaseOperator]) -> frozenset[BaseTIDep]:
        operator_deps = operator_class.deps
        if not isinstance(operator_deps, collections.abc.Set):
            raise UnmappableOperator(
                f"'deps' must be a set defined as a class-level variable on {operator_class.__name__}, "
                f"not a {type(operator_deps).__name__}"
            )
        return operator_deps | {MappedTaskIsExpanded()}

    @property
    def task_type(self) -> str:
        """Implementing Operator."""
        return self._task_type

    @property
    def operator_name(self) -> str:
        return self._operator_name

    @property
    def inherits_from_empty_operator(self) -> bool:
        """Implementing Operator."""
        return self._is_empty

    @property
    def roots(self) -> Sequence[AbstractOperator]:
        """Implementing DAGNode."""
        return [self]

    @property
    def leaves(self) -> Sequence[AbstractOperator]:
        """Implementing DAGNode."""
        return [self]

    @property
    def owner(self) -> str:  # type: ignore[override]
        return self.partial_kwargs.get("owner", DEFAULT_OWNER)

    @property
    def email(self) -> None | str | Iterable[str]:
        return self.partial_kwargs.get("email")

    @property
    def trigger_rule(self) -> TriggerRule:
        return self.partial_kwargs.get("trigger_rule", DEFAULT_TRIGGER_RULE)

    @property
    def depends_on_past(self) -> bool:
        return bool(self.partial_kwargs.get("depends_on_past"))

    @property
    def ignore_first_depends_on_past(self) -> bool:
        value = self.partial_kwargs.get("ignore_first_depends_on_past", DEFAULT_IGNORE_FIRST_DEPENDS_ON_PAST)
        return bool(value)

    @property
    def wait_for_past_depends_before_skipping(self) -> bool:
        value = self.partial_kwargs.get(
            "wait_for_past_depends_before_skipping", DEFAULT_WAIT_FOR_PAST_DEPENDS_BEFORE_SKIPPING
        )
        return bool(value)

    @property
    def wait_for_downstream(self) -> bool:
        return bool(self.partial_kwargs.get("wait_for_downstream"))

    @property
    def retries(self) -> int | None:
        return self.partial_kwargs.get("retries", DEFAULT_RETRIES)

    @property
    def queue(self) -> str:
        return self.partial_kwargs.get("queue", DEFAULT_QUEUE)

    @property
    def pool(self) -> str:
        return self.partial_kwargs.get("pool", Pool.DEFAULT_POOL_NAME)

    @property
    def pool_slots(self) -> str | None:
        return self.partial_kwargs.get("pool_slots", DEFAULT_POOL_SLOTS)

    @property
    def execution_timeout(self) -> datetime.timedelta | None:
        return self.partial_kwargs.get("execution_timeout")

    @property
    def max_retry_delay(self) -> datetime.timedelta | None:
        return self.partial_kwargs.get("max_retry_delay")

    @property
    def retry_delay(self) -> datetime.timedelta:
        return self.partial_kwargs.get("retry_delay", DEFAULT_RETRY_DELAY)

    @property
    def retry_exponential_backoff(self) -> bool:
        return bool(self.partial_kwargs.get("retry_exponential_backoff"))

    @property
    def priority_weight(self) -> int:  # type: ignore[override]
        return self.partial_kwargs.get("priority_weight", DEFAULT_PRIORITY_WEIGHT)

    @property
    def weight_rule(self) -> int:  # type: ignore[override]
        return self.partial_kwargs.get("weight_rule", DEFAULT_WEIGHT_RULE)

    @property
    def sla(self) -> datetime.timedelta | None:
        return self.partial_kwargs.get("sla")

    @property
    def max_active_tis_per_dag(self) -> int | None:
        return self.partial_kwargs.get("max_active_tis_per_dag")

    @property
    def max_active_tis_per_dagrun(self) -> int | None:
        return self.partial_kwargs.get("max_active_tis_per_dagrun")

    @property
    def resources(self) -> Resources | None:
        return self.partial_kwargs.get("resources")

    @property
    def on_execute_callback(self) -> None | TaskStateChangeCallback | list[TaskStateChangeCallback]:
        return self.partial_kwargs.get("on_execute_callback")

    @on_execute_callback.setter
    def on_execute_callback(self, value: TaskStateChangeCallback | None) -> None:
        self.partial_kwargs["on_execute_callback"] = value

    @property
    def on_failure_callback(self) -> None | TaskStateChangeCallback | list[TaskStateChangeCallback]:
        return self.partial_kwargs.get("on_failure_callback")

    @on_failure_callback.setter
    def on_failure_callback(self, value: TaskStateChangeCallback | None) -> None:
        self.partial_kwargs["on_failure_callback"] = value

    @property
    def on_retry_callback(self) -> None | TaskStateChangeCallback | list[TaskStateChangeCallback]:
        return self.partial_kwargs.get("on_retry_callback")

    @on_retry_callback.setter
    def on_retry_callback(self, value: TaskStateChangeCallback | None) -> None:
        self.partial_kwargs["on_retry_callback"] = value

    @property
    def on_success_callback(self) -> None | TaskStateChangeCallback | list[TaskStateChangeCallback]:
        return self.partial_kwargs.get("on_success_callback")

    @on_success_callback.setter
    def on_success_callback(self, value: TaskStateChangeCallback | None) -> None:
        self.partial_kwargs["on_success_callback"] = value

    @property
    def run_as_user(self) -> str | None:
        return self.partial_kwargs.get("run_as_user")

    @property
    def executor_config(self) -> dict:
        return self.partial_kwargs.get("executor_config", {})

    @property  # type: ignore[override]
    def inlets(self) -> list[Any]:  # type: ignore[override]
        return self.partial_kwargs.get("inlets", [])

    @inlets.setter
    def inlets(self, value: list[Any]) -> None:  # type: ignore[override]
        self.partial_kwargs["inlets"] = value

    @property  # type: ignore[override]
    def outlets(self) -> list[Any]:  # type: ignore[override]
        return self.partial_kwargs.get("outlets", [])

    @outlets.setter
    def outlets(self, value: list[Any]) -> None:  # type: ignore[override]
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

    def get_dag(self) -> DAG | None:
        """Implementing Operator."""
        return self.dag

    @property
    def output(self) -> XComArg:
        """Returns reference to XCom pushed by current operator"""
        from airflow.models.xcom_arg import XComArg

        return XComArg(operator=self)

    def serialize_for_task_group(self) -> tuple[DagAttributeTypes, Any]:
        """Implementing DAGNode."""
        return DagAttributeTypes.OP, self.task_id

    def _expand_mapped_kwargs(self, context: Context, session: Session) -> tuple[Mapping[str, Any], set[int]]:
        """Get the kwargs to create the unmapped operator.

        This exists because taskflow operators expand against op_kwargs, not the
        entire operator kwargs dict.
        """
        return self._get_specified_expand_input().resolve(context, session)

    def _get_unmap_kwargs(self, mapped_kwargs: Mapping[str, Any], *, strict: bool) -> dict[str, Any]:
        """Get init kwargs to unmap the underlying operator class.

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

    def unmap(self, resolve: None | Mapping[str, Any] | tuple[Context, Session]) -> BaseOperator:
        """Get the "normal" Operator after applying the current mapping.

        The *resolve* argument is only used if ``operator_class`` is a real
        class, i.e. if this operator is not serialized. If ``operator_class`` is
        not a class (i.e. this DAG has been deserialized), this returns a
        SerializedBaseOperator that "looks like" the actual unmapping result.

        If *resolve* is a two-tuple (context, session), the information is used
        to resolve the mapped arguments into init arguments. If it is a mapping,
        no resolving happens, the mapping directly provides those init arguments
        resolved from mapped kwargs.

        :meta private:
        """
        if isinstance(self.operator_class, type):
            if isinstance(resolve, collections.abc.Mapping):
                kwargs = resolve
            elif resolve is not None:
                kwargs, _ = self._expand_mapped_kwargs(*resolve)
            else:
                raise RuntimeError("cannot unmap a non-serialized operator without context")
            kwargs = self._get_unmap_kwargs(kwargs, strict=self._disallow_kwargs_override)
            op = self.operator_class(**kwargs, _airflow_from_mapped=True)
            # We need to overwrite task_id here because BaseOperator further
            # mangles the task_id based on the task hierarchy (namely, group_id
            # is prepended, and '__N' appended to deduplicate). This is hacky,
            # but better than duplicating the whole mangling logic.
            op.task_id = self.task_id
            return op

        # After a mapped operator is serialized, there's no real way to actually
        # unmap it since we've lost access to the underlying operator class.
        # This tries its best to simply "forward" all the attributes on this
        # mapped operator to a new SerializedBaseOperator instance.
        from airflow.serialization.serialized_objects import SerializedBaseOperator

        op = SerializedBaseOperator(task_id=self.task_id, params=self.params, _airflow_from_mapped=True)
        SerializedBaseOperator.populate_operator(op, self.operator_class)
        return op

    def _get_specified_expand_input(self) -> ExpandInput:
        """Input received from the expand call on the operator."""
        return getattr(self, self._expand_input_attr)

    def prepare_for_execution(self) -> MappedOperator:
        # Since a mapped operator cannot be used for execution, and an unmapped
        # BaseOperator needs to be created later (see render_template_fields),
        # we don't need to create a copy of the MappedOperator here.
        return self

    def iter_mapped_dependencies(self) -> Iterator[Operator]:
        """Upstream dependencies that provide XComs used by this task for task mapping."""
        from airflow.models.xcom_arg import XComArg

        for operator, _ in XComArg.iter_xcom_references(self._get_specified_expand_input()):
            yield operator

    @cache
    def get_parse_time_mapped_ti_count(self) -> int:
        current_count = self._get_specified_expand_input().get_parse_time_mapped_ti_count()
        try:
            parent_count = super().get_parse_time_mapped_ti_count()
        except NotMapped:
            return current_count
        return parent_count * current_count

    def get_mapped_ti_count(self, run_id: str, *, session: Session) -> int:
        current_count = self._get_specified_expand_input().get_total_map_length(run_id, session=session)
        try:
            parent_count = super().get_mapped_ti_count(run_id, session=session)
        except NotMapped:
            return current_count
        return parent_count * current_count

    def render_template_fields(
        self,
        context: Context,
        jinja_env: jinja2.Environment | None = None,
    ) -> None:
        """Template all attributes listed in *self.template_fields*.

        This updates *context* to reference the map-expanded task and relevant
        information, without modifying the mapped operator. The expanded task
        in *context* is then rendered in-place.

        :param context: Context dict with values to apply on content.
        :param jinja_env: Jinja environment to use for rendering.
        """
        if not jinja_env:
            jinja_env = self.get_template_env()

        # Ideally we'd like to pass in session as an argument to this function,
        # but we can't easily change this function signature since operators
        # could override this. We can't use @provide_session since it closes and
        # expunges everything, which we don't want to do when we are so "deep"
        # in the weeds here. We don't close this session for the same reason.
        session = settings.Session()

        mapped_kwargs, seen_oids = self._expand_mapped_kwargs(context, session)
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
            session=session,
        )

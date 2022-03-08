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

import collections
import collections.abc
import datetime
import functools
import operator
import warnings
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Collection,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
)

import attr
import pendulum
from sqlalchemy import func, or_
from sqlalchemy.orm.session import Session

from airflow.compat.functools import cache
from airflow.exceptions import UnmappableOperator
from airflow.models.abstractoperator import (
    DEFAULT_OWNER,
    DEFAULT_POOL_SLOTS,
    DEFAULT_PRIORITY_WEIGHT,
    DEFAULT_QUEUE,
    DEFAULT_RETRIES,
    DEFAULT_RETRY_DELAY,
    DEFAULT_TRIGGER_RULE,
    DEFAULT_WEIGHT_RULE,
    AbstractOperator,
    TaskStateChangeCallback,
)
from airflow.models.pool import Pool
from airflow.serialization.enums import DagAttributeTypes
from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.ti_deps.deps.mapped_task_expanded import MappedTaskIsExpanded
from airflow.typing_compat import Literal
from airflow.utils.context import Context
from airflow.utils.helpers import is_container
from airflow.utils.operator_resources import Resources
from airflow.utils.state import State, TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.types import NOTSET

if TYPE_CHECKING:
    import jinja2  # Slow import.

    from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
    from airflow.models.dag import DAG
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.xcom_arg import XComArg
    from airflow.utils.task_group import TaskGroup

    # BaseOperator.apply() can be called on an XComArg, sequence, or dict (not
    # any mapping since we need the value to be ordered).
    Mappable = Union[XComArg, Sequence, dict]

ValidationSource = Union[Literal["map"], Literal["partial"]]


# For isinstance() check.
@cache
def get_mappable_types() -> Tuple[type, ...]:
    from airflow.models.xcom_arg import XComArg

    return (XComArg, dict, list)


def validate_mapping_kwargs(op: Type["BaseOperator"], func: ValidationSource, value: Dict[str, Any]) -> None:
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
            if func != "apply":
                continue
            if value is NOTSET:
                continue
            if isinstance(value, get_mappable_types()):
                continue
            type_name = type(value).__name__
            error = f"{op.__name__}.apply() got an unexpected type {type_name!r} for keyword argument {name}"
            raise ValueError(error)
        if not unknown_args:
            return  # If we have no args left to check: stop looking at the MRO chain.

    if len(unknown_args) == 1:
        error = f"an unexpected keyword argument {unknown_args.popitem()[0]!r}"
    else:
        names = ", ".join(repr(n) for n in unknown_args)
        error = f"unexpected keyword arguments {names}"
    raise TypeError(f"{op.__name__}.{func}() got {error}")


def prevent_duplicates(kwargs1: Dict[str, Any], kwargs2: Dict[str, Any], *, fail_reason: str) -> None:
    duplicated_keys = set(kwargs1).intersection(kwargs2)
    if not duplicated_keys:
        return
    if len(duplicated_keys) == 1:
        raise TypeError(f"{fail_reason} argument: {duplicated_keys.pop()}")
    duplicated_keys_display = ", ".join(sorted(duplicated_keys))
    raise TypeError(f"{fail_reason} arguments: {duplicated_keys_display}")


def ensure_xcomarg_return_value(arg: Any) -> None:
    from airflow.models.xcom_arg import XCOM_RETURN_KEY, XComArg

    if isinstance(arg, XComArg):
        if arg.key != XCOM_RETURN_KEY:
            raise ValueError(f"cannot map over XCom with custom key {arg.key!r} from {arg.operator}")
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
    user to call ``.apply()`` on it at some point (usually in a method chain) to
    create a ``MappedOperator`` to add into the DAG.
    """

    operator_class: Type["BaseOperator"]
    kwargs: Dict[str, Any]

    _apply_called: bool = False  # Set when apply() is called to ease user debugging.

    def __attrs_post_init__(self):
        from airflow.operators.subdag import SubDagOperator

        if issubclass(self.operator_class, SubDagOperator):
            raise TypeError("Mapping over deprecated SubDagOperator is not supported")
        validate_mapping_kwargs(self.operator_class, "partial", self.kwargs)

    def __repr__(self) -> str:
        args = ", ".join(f"{k}={v!r}" for k, v in self.kwargs.items())
        return f"{self.operator_class.__name__}.partial({args})"

    def __del__(self):
        if not self._apply_called:
            warnings.warn(f"{self!r} was never mapped!")

    def apply(self, **mapped_kwargs: "Mappable") -> "MappedOperator":
        from airflow.operators.dummy import DummyOperator

        validate_mapping_kwargs(self.operator_class, "apply", mapped_kwargs)
        prevent_duplicates(self.kwargs, mapped_kwargs, fail_reason="mapping already partial")
        ensure_xcomarg_return_value(mapped_kwargs)

        partial_kwargs = self.kwargs.copy()
        task_id = partial_kwargs.pop("task_id")
        params = partial_kwargs.pop("params")
        dag = partial_kwargs.pop("dag")
        task_group = partial_kwargs.pop("task_group")
        start_date = partial_kwargs.pop("start_date")
        end_date = partial_kwargs.pop("end_date")

        op = MappedOperator(
            operator_class=self.operator_class,
            mapped_kwargs=mapped_kwargs,
            partial_kwargs=partial_kwargs,
            task_id=task_id,
            params=params,
            deps=MappedOperator.deps_for(self.operator_class),
            operator_extra_links=self.operator_class.operator_extra_links,
            template_ext=self.operator_class.template_ext,
            template_fields=self.operator_class.template_fields,
            ui_color=self.operator_class.ui_color,
            ui_fgcolor=self.operator_class.ui_fgcolor,
            is_dummy=issubclass(self.operator_class, DummyOperator),
            task_module=self.operator_class.__module__,
            task_type=self.operator_class.__name__,
            dag=dag,
            task_group=task_group,
            start_date=start_date,
            end_date=end_date,
        )
        self._apply_called = True
        return op


@attr.define(kw_only=True)
class MappedOperator(AbstractOperator):
    """Object representing a mapped operator in a DAG."""

    operator_class: Union[Type["BaseOperator"], str]
    mapped_kwargs: Dict[str, "Mappable"]
    partial_kwargs: Dict[str, Any]

    # Needed for serialization.
    task_id: str
    params: Optional[dict]
    deps: FrozenSet[BaseTIDep]
    operator_extra_links: Collection["BaseOperatorLink"]
    template_ext: Collection[str]
    template_fields: Collection[str]
    ui_color: str
    ui_fgcolor: str
    _is_dummy: bool
    _task_module: str
    _task_type: str

    dag: Optional["DAG"]
    task_group: Optional["TaskGroup"]
    start_date: Optional[pendulum.DateTime]
    end_date: Optional[pendulum.DateTime]
    upstream_task_ids: Set[str] = attr.ib(factory=set, init=False)
    downstream_task_ids: Set[str] = attr.ib(factory=set, init=False)

    is_mapped: ClassVar[bool] = True
    subdag: None = None  # Since we don't support SubDagOperator, this is always None.

    def __repr__(self):
        return f"<Mapped({self._task_type}): {self.task_id}>"

    def __attrs_post_init__(self):
        from airflow.models.xcom_arg import XComArg

        self._validate_argument_count()
        if self.task_group:
            self.task_group.add(self)
        if self.dag:
            self.dag.add_task(self)
        for k, v in self.mapped_kwargs.items():
            if k in self.template_fields:
                XComArg.apply_upstream_relationship(self, v)
        for k, v in self.partial_kwargs.items():
            if k in self.template_fields:
                XComArg.apply_upstream_relationship(self, v)

    @classmethod
    @cache
    def get_serialized_fields(cls):
        # Not using 'cls' here since we only want to serialize base fields.
        return frozenset(attr.fields_dict(MappedOperator)) - {
            "dag",
            "deps",
            "is_mapped",
            "subdag",
            "task_group",
            "upstream_task_ids",
        }

    @staticmethod
    @cache
    def deps_for(operator_class: Type["BaseOperator"]) -> FrozenSet[BaseTIDep]:
        operator_deps = operator_class.deps
        if not isinstance(operator_deps, collections.abc.Set):
            raise UnmappableOperator(
                f"'deps' must be a set defined as a class-level variable on {operator_class.__name__}, "
                f"not a {type(operator_deps).__name__}"
            )
        return operator_deps | {MappedTaskIsExpanded()}

    def _validate_argument_count(self) -> None:
        """Validate mapping arguments by unmapping with mocked values.

        This ensures the user passed enough arguments in the DAG definition for
        the operator to work in the task runner. This does not guarantee the
        arguments are *valid* (that depends on the actual mapping values), but
        makes sure there are *enough* of them.
        """
        if isinstance(self.operator_class, str):
            return  # No need to validate deserialized operator.
        self.operator_class.validate_mapped_arguments(**self._get_unmap_kwargs())

    @property
    def task_type(self) -> str:
        """Implementing Operator."""
        return self._task_type

    @property
    def inherits_from_dummy_operator(self) -> bool:
        """Implementing Operator."""
        return self._is_dummy

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
    def email(self) -> Union[None, str, Iterable[str]]:
        return self.partial_kwargs.get("email")

    @property
    def trigger_rule(self) -> TriggerRule:
        return self.partial_kwargs.get("trigger_rule", DEFAULT_TRIGGER_RULE)

    @property
    def depends_on_past(self) -> bool:
        return bool(self.partial_kwargs.get("depends_on_past"))

    @property
    def wait_for_downstream(self) -> bool:
        return bool(self.partial_kwargs.get("wait_for_downstream"))

    @property
    def retries(self) -> Optional[int]:
        return self.partial_kwargs.get("retries", DEFAULT_RETRIES)

    @property
    def queue(self) -> str:
        return self.partial_kwargs.get("queue", DEFAULT_QUEUE)

    @property
    def pool(self) -> str:
        return self.partial_kwargs.get("pool", Pool.DEFAULT_POOL_NAME)

    @property
    def pool_slots(self) -> Optional[str]:
        return self.partial_kwargs.get("pool_slots", DEFAULT_POOL_SLOTS)

    @property
    def execution_timeout(self) -> Optional[datetime.timedelta]:
        return self.partial_kwargs.get("execution_timeout")

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
    def sla(self) -> Optional[datetime.timedelta]:
        return self.partial_kwargs.get("sla")

    @property
    def max_active_tis_per_dag(self) -> Optional[int]:
        return self.partial_kwargs.get("max_active_tis_per_dag")

    @property
    def resources(self) -> Optional[Resources]:
        return self.partial_kwargs.get("resources")

    @property
    def on_execute_callback(self) -> Optional[TaskStateChangeCallback]:
        return self.partial_kwargs.get("on_execute_callback")

    @property
    def on_failure_callback(self) -> Optional[TaskStateChangeCallback]:
        return self.partial_kwargs.get("on_failure_callback")

    @property
    def on_retry_callback(self) -> Optional[TaskStateChangeCallback]:
        return self.partial_kwargs.get("on_retry_callback")

    @property
    def on_success_callback(self) -> Optional[TaskStateChangeCallback]:
        return self.partial_kwargs.get("on_success_callback")

    @property
    def run_as_user(self) -> Optional[str]:
        return self.partial_kwargs.get("run_as_user")

    @property
    def executor_config(self) -> dict:
        return self.partial_kwargs.get("executor_config", {})

    @property
    def inlets(self) -> Optional[Any]:
        return self.partial_kwargs.get("inlets", None)

    @property
    def outlets(self) -> Optional[Any]:
        return self.partial_kwargs.get("outlets", None)

    def get_dag(self) -> Optional["DAG"]:
        """Implementing Operator."""
        return self.dag

    def serialize_for_task_group(self) -> Tuple[DagAttributeTypes, Any]:
        """Implementing DAGNode."""
        return DagAttributeTypes.OP, self.task_id

    def _get_unmap_kwargs(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "dag": self.dag,
            "task_group": self.task_group,
            "params": self.params,
            "start_date": self.start_date,
            "end_date": self.end_date,
            **self.partial_kwargs,
            **self.mapped_kwargs,
        }

    def unmap(self) -> "BaseOperator":
        """Get the "normal" Operator after applying the current mapping."""
        dag = self.dag
        if not dag:
            raise RuntimeError("Cannot unmap a task without a DAG")
        if isinstance(self.operator_class, str):
            raise RuntimeError("Cannot unmap a deserialized operator")
        dag._remove_task(self.task_id)
        return self.operator_class(**self._get_unmap_kwargs())

    def _get_expansion_kwargs(self) -> Dict[str, "Mappable"]:
        """The kwargs to calculate expansion length against.

        This is ``self.mapped_kwargs`` for classic operators because kwargs to
        ``BaseOperator.apply()`` contribute to operator arguments.
        """
        return self.mapped_kwargs

    def _get_map_lengths(self, run_id: str, *, session: Session) -> Dict[str, int]:
        # TODO: Find a way to cache this.
        from airflow.models.taskmap import TaskMap
        from airflow.models.xcom_arg import XComArg

        expansion_kwargs = self._get_expansion_kwargs()

        # Populate literal mapped arguments first.
        map_lengths: Dict[str, int] = collections.defaultdict(int)
        map_lengths.update((k, len(v)) for k, v in expansion_kwargs.items() if not isinstance(v, XComArg))

        # Build a reverse mapping of what arguments each task contributes to.
        dep_keys: Dict[str, Set[str]] = collections.defaultdict(set)
        for k, v in expansion_kwargs.items():
            if not isinstance(v, XComArg):
                continue
            dep_keys[v.operator.task_id].add(k)

        taskmap_query = session.query(TaskMap.task_id, TaskMap.length).filter(
            TaskMap.dag_id == self.dag_id,
            TaskMap.run_id == run_id,
            TaskMap.task_id.in_(list(dep_keys)),
        )
        for task_id, length in taskmap_query:
            for mapped_arg_name in dep_keys[task_id]:
                map_lengths[mapped_arg_name] += length

        if len(map_lengths) < len(expansion_kwargs):
            keys = ", ".join(repr(k) for k in sorted(set(expansion_kwargs).difference(map_lengths)))
            raise RuntimeError(f"Failed to populate all mapping metadata; missing: {keys}")

        return map_lengths

    def expand_mapped_task(self, run_id: str, *, session: Session) -> Sequence["TaskInstance"]:
        """Create the mapped task instances for mapped task.

        :return: The mapped task instances, in ascending order by map index.
        """
        from airflow.models.taskinstance import TaskInstance
        from airflow.settings import task_instance_mutation_hook

        total_length = functools.reduce(operator.mul, self._get_map_lengths(run_id, session=session).values())

        state: Optional[TaskInstanceState] = None
        unmapped_ti: Optional[TaskInstance] = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == self.dag_id,
                TaskInstance.task_id == self.task_id,
                TaskInstance.run_id == run_id,
                TaskInstance.map_index == -1,
                or_(TaskInstance.state.in_(State.unfinished), TaskInstance.state.is_(None)),
            )
            .one_or_none()
        )

        ret: List[TaskInstance] = []

        if unmapped_ti:
            # The unmapped task instance still exists and is unfinished, i.e. we
            # haven't tried to run it before.
            if total_length < 1:
                # If the upstream maps this to a zero-length value, simply marked the
                # unmapped task instance as SKIPPED (if needed).
                self.log.info(
                    "Marking %s as SKIPPED since the map has %d values to expand",
                    unmapped_ti,
                    total_length,
                )
                unmapped_ti.state = TaskInstanceState.SKIPPED
                session.flush()
                return ret
            # Otherwise convert this into the first mapped index, and create
            # TaskInstance for other indexes.
            unmapped_ti.map_index = 0
            state = unmapped_ti.state
            self.log.debug("Updated in place to become %s", unmapped_ti)
            ret.append(unmapped_ti)
            indexes_to_map = range(1, total_length)
        else:
            # Only create "missing" ones.
            current_max_mapping = (
                session.query(func.max(TaskInstance.map_index))
                .filter(
                    TaskInstance.dag_id == self.dag_id,
                    TaskInstance.task_id == self.task_id,
                    TaskInstance.run_id == run_id,
                )
                .scalar()
            )
            indexes_to_map = range(current_max_mapping + 1, total_length)

        for index in indexes_to_map:
            # TODO: Make more efficient with bulk_insert_mappings/bulk_save_mappings.
            # TODO: Change `TaskInstance` ctor to take Operator, not BaseOperator
            ti = TaskInstance(self, run_id=run_id, map_index=index, state=state)  # type: ignore
            self.log.debug("Expanding TIs upserted %s", ti)
            task_instance_mutation_hook(ti)
            ti = session.merge(ti)
            ti.task = self
            ret.append(ti)

        # Set to "REMOVED" any (old) TaskInstances with map indices greater
        # than the current map value
        session.query(TaskInstance).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id == self.task_id,
            TaskInstance.run_id == run_id,
            TaskInstance.map_index >= total_length,
        ).update({TaskInstance.state: TaskInstanceState.REMOVED})

        session.flush()

        return ret

    def prepare_for_execution(self) -> "MappedOperator":
        # Since a mapped operator cannot be used for execution, and an unmapped
        # BaseOperator needs to be created later (see render_template_fields),
        # we don't need to create a copy of the MappedOperator here.
        return self

    def render_template_fields(
        self,
        context: Context,
        jinja_env: Optional["jinja2.Environment"] = None,
    ) -> Optional["BaseOperator"]:
        """Template all attributes listed in template_fields.

        Different from the BaseOperator implementation, this renders the
        template fields on the *unmapped* BaseOperator.

        :param context: Dict with values to apply on content
        :param jinja_env: Jinja environment
        :return: The unmapped, populated BaseOperator
        """
        if not jinja_env:
            jinja_env = self.get_template_env()
        unmapped_task = self.unmap()
        self._do_render_template_fields(
            parent=unmapped_task,
            template_fields=unmapped_task.template_fields,
            context=context,
            jinja_env=jinja_env,
            seen_oids=set(),
        )
        return unmapped_task

    def _render_template_field(
        self,
        key: str,
        value: Any,
        context: Context,
        jinja_env: Optional["jinja2.Environment"] = None,
        seen_oids: Optional[Set] = None,
        *,
        session: Session,
    ) -> Any:
        """Override the ordinary template rendering to add more logic.

        Specifically, if we're rendering a mapped argument, we need to "unmap"
        the value as well to assign it to the unmapped operator.
        """
        value = super()._render_template_field(key, value, context, jinja_env, seen_oids, session=session)
        return self._expand_mapped_field(key, value, context, session=session)

    def _expand_mapped_field(self, key: str, value: Any, context: Context, *, session: Session) -> Any:
        map_index = context["ti"].map_index
        if map_index < 0:
            return value
        expansion_kwargs = self._get_expansion_kwargs()
        all_lengths = self._get_map_lengths(context["run_id"], session=session)

        def _find_index_for_this_field(index: int) -> int:
            # Need to use self.mapped_kwargs for the original argument order.
            for mapped_key in reversed(list(expansion_kwargs)):
                mapped_length = all_lengths[mapped_key]
                if mapped_length < 1:
                    raise RuntimeError(f"cannot expand field mapped to length {mapped_length!r}")
                if mapped_key == key:
                    return index % mapped_length
                index //= mapped_length
            return -1

        found_index = _find_index_for_this_field(map_index)
        if found_index < 0:
            return value
        if isinstance(value, collections.abc.Sequence):
            return value[found_index]
        if not isinstance(value, dict):
            raise TypeError(f"can't map over value of type {type(value)}")
        for i, (k, v) in enumerate(value.items()):
            if i == found_index:
                return k, v
        raise IndexError(f"index {map_index} is over mapped length")

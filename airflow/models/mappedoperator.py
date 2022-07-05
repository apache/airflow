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
    Iterator,
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

from airflow import settings
from airflow.compat.functools import cache, cached_property
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
    from airflow.models.operator import Operator
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.xcom_arg import XComArg
    from airflow.utils.task_group import TaskGroup

    # BaseOperator.expand() can be called on an XComArg, sequence, or dict (not
    # any mapping since we need the value to be ordered).
    Mappable = Union[XComArg, Sequence, dict]

ValidationSource = Union[Literal["expand"], Literal["partial"]]


MAPPABLE_LITERAL_TYPES = (dict, list)


# For isinstance() check.
@cache
def get_mappable_types() -> Tuple[type, ...]:
    from airflow.models.xcom_arg import XComArg

    return (XComArg,) + MAPPABLE_LITERAL_TYPES


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
            if func != "expand":
                continue
            if value is NOTSET:
                continue
            if isinstance(value, get_mappable_types()):
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
    user to call ``.expand()`` on it at some point (usually in a method chain) to
    create a ``MappedOperator`` to add into the DAG.
    """

    operator_class: Type["BaseOperator"]
    kwargs: Dict[str, Any]

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

    def expand(self, **mapped_kwargs: "Mappable") -> "MappedOperator":
        if not mapped_kwargs:
            raise TypeError("no arguments to expand against")
        return self._expand(**mapped_kwargs)

    def _expand(self, **mapped_kwargs: "Mappable") -> "MappedOperator":
        self._expand_called = True

        from airflow.operators.empty import EmptyOperator

        validate_mapping_kwargs(self.operator_class, "expand", mapped_kwargs)
        prevent_duplicates(self.kwargs, mapped_kwargs, fail_reason="unmappable or already specified")
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
            template_fields_renderers=self.operator_class.template_fields_renderers,
            ui_color=self.operator_class.ui_color,
            ui_fgcolor=self.operator_class.ui_fgcolor,
            is_empty=issubclass(self.operator_class, EmptyOperator),
            task_module=self.operator_class.__module__,
            task_type=self.operator_class.__name__,
            dag=dag,
            task_group=task_group,
            start_date=start_date,
            end_date=end_date,
            # For classic operators, this points to mapped_kwargs because kwargs
            # to BaseOperator.expand() contribute to operator arguments.
            expansion_kwargs_attr="mapped_kwargs",
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
    operator_class: Union[Type["BaseOperator"], Dict[str, Any]]

    mapped_kwargs: Dict[str, "Mappable"]
    partial_kwargs: Dict[str, Any]

    # Needed for serialization.
    task_id: str
    params: Optional[dict]
    deps: FrozenSet[BaseTIDep]
    operator_extra_links: Collection["BaseOperatorLink"]
    template_ext: Sequence[str]
    template_fields: Collection[str]
    template_fields_renderers: Dict[str, str]
    ui_color: str
    ui_fgcolor: str
    _is_empty: bool
    _task_module: str
    _task_type: str

    dag: Optional["DAG"]
    task_group: Optional["TaskGroup"]
    start_date: Optional[pendulum.DateTime]
    end_date: Optional[pendulum.DateTime]
    upstream_task_ids: Set[str] = attr.ib(factory=set, init=False)
    downstream_task_ids: Set[str] = attr.ib(factory=set, init=False)

    _expansion_kwargs_attr: str
    """Where to get kwargs to calculate expansion length against.

    This should be a name to call ``getattr()`` on.
    """

    is_mapped: ClassVar[bool] = True
    subdag: None = None  # Since we don't support SubDagOperator, this is always None.

    HIDE_ATTRS_FROM_UI: ClassVar[FrozenSet[str]] = AbstractOperator.HIDE_ATTRS_FROM_UI | frozenset(
        (
            'parse_time_mapped_ti_count',
            'operator_class',
        )
    )

    def __hash__(self):
        return id(self)

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
            XComArg.apply_upstream_relationship(self, v)
        for k, v in self.partial_kwargs.items():
            if k in self.template_fields:
                XComArg.apply_upstream_relationship(self, v)
        if self.partial_kwargs.get('sla') is not None:
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
            "is_mapped",
            "mapped_kwargs",  # This is needed to be able to accept XComArg.
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
        if not isinstance(self.operator_class, type):
            return  # No need to validate deserialized operator.
        self.operator_class.validate_mapped_arguments(**self._get_unmap_kwargs())

    @property
    def task_type(self) -> str:
        """Implementing Operator."""
        return self._task_type

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
    def email(self) -> Union[None, str, Iterable[str]]:
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
    def max_retry_delay(self) -> Optional[datetime.timedelta]:
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

    @property
    def doc(self) -> Optional[str]:
        return self.partial_kwargs.get("doc")

    @property
    def doc_md(self) -> Optional[str]:
        return self.partial_kwargs.get("doc_md")

    @property
    def doc_json(self) -> Optional[str]:
        return self.partial_kwargs.get("doc_json")

    @property
    def doc_yaml(self) -> Optional[str]:
        return self.partial_kwargs.get("doc_yaml")

    @property
    def doc_rst(self) -> Optional[str]:
        return self.partial_kwargs.get("doc_rst")

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

    def unmap(self, unmap_kwargs: Optional[Dict[str, Any]] = None) -> "BaseOperator":
        """
        Get the "normal" Operator after applying the current mapping.

        If ``operator_class`` is not a class (i.e. this DAG has been deserialized) then this will return a
        SerializedBaseOperator that aims to "look like" the real operator.

        :param unmap_kwargs: Override the args to pass to the Operator constructor. Only used when
            ``operator_class`` is still an actual class.

        :meta private:
        """
        if isinstance(self.operator_class, type):
            # We can't simply specify task_id here because BaseOperator further
            # mangles the task_id based on the task hierarchy (namely, group_id
            # is prepended, and '__N' appended to deduplicate). Instead of
            # recreating the whole logic here, we just overwrite task_id later.
            if unmap_kwargs is None:
                unmap_kwargs = self._get_unmap_kwargs()
            op = self.operator_class(**unmap_kwargs, _airflow_from_mapped=True)
            op.task_id = self.task_id
            return op

        # After a mapped operator is serialized, there's no real way to actually
        # unmap it since we've lost access to the underlying operator class.
        # This tries its best to simply "forward" all the attributes on this
        # mapped operator to a new SerializedBaseOperator instance.
        from airflow.serialization.serialized_objects import SerializedBaseOperator

        op = SerializedBaseOperator(task_id=self.task_id, _airflow_from_mapped=True)
        SerializedBaseOperator.populate_operator(op, self.operator_class)
        return op

    def _get_expansion_kwargs(self) -> Dict[str, "Mappable"]:
        """The kwargs to calculate expansion length against."""
        return getattr(self, self._expansion_kwargs_attr)

    def _get_map_lengths(self, run_id: str, *, session: Session) -> Dict[str, int]:
        """Return dict of argument name to map length.

        If any arguments are not known right now (upstream task not finished) they will not be present in the
        dict.
        """
        # TODO: Find a way to cache this.
        from airflow.models.taskmap import TaskMap
        from airflow.models.xcom import XCom
        from airflow.models.xcom_arg import XComArg

        expansion_kwargs = self._get_expansion_kwargs()

        # Populate literal mapped arguments first.
        map_lengths: Dict[str, int] = collections.defaultdict(int)
        map_lengths.update((k, len(v)) for k, v in expansion_kwargs.items() if not isinstance(v, XComArg))

        # Build a reverse mapping of what arguments each task contributes to.
        mapped_dep_keys: Dict[str, Set[str]] = collections.defaultdict(set)
        non_mapped_dep_keys: Dict[str, Set[str]] = collections.defaultdict(set)
        for k, v in expansion_kwargs.items():
            if not isinstance(v, XComArg):
                continue
            if v.operator.is_mapped:
                mapped_dep_keys[v.operator.task_id].add(k)
            else:
                non_mapped_dep_keys[v.operator.task_id].add(k)
            # TODO: It's not possible now, but in the future (AIP-42 Phase 2)
            # we will add support for depending on one single mapped task
            # instance. When that happens, we need to further analyze the mapped
            # case to contain only tasks we depend on "as a whole", and put
            # those we only depend on individually to the non-mapped lookup.

        # Collect lengths from unmapped upstreams.
        taskmap_query = session.query(TaskMap.task_id, TaskMap.length).filter(
            TaskMap.dag_id == self.dag_id,
            TaskMap.run_id == run_id,
            TaskMap.task_id.in_(non_mapped_dep_keys),
            TaskMap.map_index < 0,
        )
        for task_id, length in taskmap_query:
            for mapped_arg_name in non_mapped_dep_keys[task_id]:
                map_lengths[mapped_arg_name] += length

        # Collect lengths from mapped upstreams.
        xcom_query = (
            session.query(XCom.task_id, func.count(XCom.map_index))
            .group_by(XCom.task_id)
            .filter(
                XCom.dag_id == self.dag_id,
                XCom.run_id == run_id,
                XCom.task_id.in_(mapped_dep_keys),
                XCom.map_index >= 0,
            )
        )
        for task_id, length in xcom_query:
            for mapped_arg_name in mapped_dep_keys[task_id]:
                map_lengths[mapped_arg_name] += length
        return map_lengths

    @cache
    def _resolve_map_lengths(self, run_id: str, *, session: Session) -> Dict[str, int]:
        """Return dict of argument name to map length, or throw if some are not resolvable"""
        expansion_kwargs = self._get_expansion_kwargs()
        map_lengths = self._get_map_lengths(run_id, session=session)
        if len(map_lengths) < len(expansion_kwargs):
            keys = ", ".join(repr(k) for k in sorted(set(expansion_kwargs).difference(map_lengths)))
            raise RuntimeError(f"Failed to populate all mapping metadata; missing: {keys}")

        return map_lengths

    def expand_mapped_task(self, run_id: str, *, session: Session) -> Tuple[Sequence["TaskInstance"], int]:
        """Create the mapped task instances for mapped task.

        :return: The newly created mapped TaskInstances (if any) in ascending order by map index, and the
            maximum map_index.
        """
        from airflow.models.taskinstance import TaskInstance
        from airflow.settings import task_instance_mutation_hook

        total_length = functools.reduce(
            operator.mul, self._resolve_map_lengths(run_id, session=session).values()
        )

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

        all_expanded_tis: List[TaskInstance] = []

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
            else:
                # Otherwise convert this into the first mapped index, and create
                # TaskInstance for other indexes.
                unmapped_ti.map_index = 0
                self.log.debug("Updated in place to become %s", unmapped_ti)
                all_expanded_tis.append(unmapped_ti)
            state = unmapped_ti.state
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
            ti = TaskInstance(self, run_id=run_id, map_index=index, state=state)
            self.log.debug("Expanding TIs upserted %s", ti)
            task_instance_mutation_hook(ti)
            ti = session.merge(ti)
            ti.refresh_from_task(self)  # session.merge() loses task information.
            all_expanded_tis.append(ti)

        # Set to "REMOVED" any (old) TaskInstances with map indices greater
        # than the current map value
        session.query(TaskInstance).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id == self.task_id,
            TaskInstance.run_id == run_id,
            TaskInstance.map_index >= total_length,
        ).update({TaskInstance.state: TaskInstanceState.REMOVED})

        session.flush()
        return all_expanded_tis, total_length

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
        # Before we unmap we have to resolve the mapped arguments, otherwise the real operator constructor
        # could be called with an XComArg, rather than the value it resolves to.
        #
        # We also need to resolve _all_ mapped arguments, even if they aren't marked as templated
        kwargs = self._get_unmap_kwargs()

        template_fields = set(self.template_fields)

        # Ideally we'd like to pass in session as an argument to this function, but since operators _could_
        # override this we can't easily change this function signature.
        # We can't use @provide_session, as that closes and expunges everything, which we don't want to do
        # when we are so "deep" in the weeds here.
        #
        # Nor do we want to close the session -- that would expunge all the things from the internal cache
        # which we don't want to do either
        session = settings.Session()
        self._resolve_expansion_kwargs(kwargs, template_fields, context, session)

        unmapped_task = self.unmap(unmap_kwargs=kwargs)
        self._do_render_template_fields(
            parent=unmapped_task,
            template_fields=template_fields,
            context=context,
            jinja_env=jinja_env,
            seen_oids=set(),
            session=session,
        )
        return unmapped_task

    def _resolve_expansion_kwargs(
        self, kwargs: Dict[str, Any], template_fields: Set[str], context: Context, session: Session
    ) -> None:
        """Update mapped fields in place in kwargs dict"""
        from airflow.models.xcom_arg import XComArg

        expansion_kwargs = self._get_expansion_kwargs()

        for k, v in expansion_kwargs.items():
            if isinstance(v, XComArg):
                v = v.resolve(context, session=session)
            v = self._expand_mapped_field(k, v, context, session=session)
            template_fields.discard(k)
            kwargs[k] = v

    def _expand_mapped_field(self, key: str, value: Any, context: Context, *, session: Session) -> Any:
        map_index = context["ti"].map_index
        if map_index < 0:
            return value
        expansion_kwargs = self._get_expansion_kwargs()
        all_lengths = self._resolve_map_lengths(context["run_id"], session=session)

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

    def iter_mapped_dependencies(self) -> Iterator["Operator"]:
        """Upstream dependencies that provide XComs used by this task for task mapping."""
        from airflow.models.xcom_arg import XComArg

        for ref in XComArg.iter_xcom_args(self._get_expansion_kwargs()):
            yield ref.operator

    @cached_property
    def parse_time_mapped_ti_count(self) -> Optional[int]:
        """
        Number of mapped TaskInstances that can be created at DagRun create time.

        :return: None if non-literal mapped arg encountered, or else total number of mapped TIs this task
            should have
        """
        total = 0

        for value in self._get_expansion_kwargs().values():
            if not isinstance(value, MAPPABLE_LITERAL_TYPES):
                # None literal type encountered, so give up
                return None
            if total == 0:
                total = len(value)
            else:
                total *= len(value)
        return total

    @cache
    def run_time_mapped_ti_count(self, run_id: str, *, session: Session) -> Optional[int]:
        """
        Number of mapped TaskInstances that can be created at run time, or None if upstream tasks are not
        complete yet.

        :return: None if upstream tasks are not complete yet, or else total number of mapped TIs this task
            should have
        """
        lengths = self._get_map_lengths(run_id, session=session)
        expansion_kwargs = self._get_expansion_kwargs()

        if not lengths or not expansion_kwargs:
            return None

        total = 1
        for name in expansion_kwargs:
            val = lengths.get(name)
            if val is None:
                return None
            total *= val

        return total

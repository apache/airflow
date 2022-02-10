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
import unittest.mock
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
from airflow.models.xcom_arg import XComArg
from airflow.serialization.enums import DagAttributeTypes
from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.ti_deps.deps.mapped_task_expanded import MappedTaskIsExpanded
from airflow.utils.session import NEW_SESSION
from airflow.utils.state import State, TaskInstanceState
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
    from airflow.models.dag import DAG
    from airflow.models.taskinstance import TaskInstance


def validate_mapping_kwargs(op: Type["BaseOperator"], func: str, value: Dict[str, Any]) -> None:
    # use a dict so order of args is same as code order
    unknown_args = value.copy()
    for klass in op.mro():
        init = klass.__init__  # type: ignore
        try:
            param_names = init._BaseOperatorMeta__param_names
        except AttributeError:
            continue
        for name in param_names:
            unknown_args.pop(name, None)
        if not unknown_args:
            return  # If we have no args left ot check: stop looking at the MRO chian.

    if len(unknown_args) == 1:
        error = f"unexpected keyword argument {unknown_args.popitem()[0]!r}"
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


@attr.define(kw_only=True, repr=False)
class OperatorPartial:
    """An "intermediate state" returned by ``BaseOperator.partial()``.

    This only exists at DAG-parsing time; the only intended usage is for the
    user to call ``.map()`` on it at some point (usually in a method chain) to
    create a ``MappedOperator`` to add into the DAG.
    """

    operator_class: Type["BaseOperator"]
    kwargs: Dict[str, Any]

    _map_called: bool = False  # Set when map() is called to ease user debugging.

    def __attrs_post_init__(self):
        from airflow.operators.subdag import SubDagOperator

        if issubclass(self.operator_class, SubDagOperator):
            raise TypeError("Mapping over deprecated SubDagOperator is not supported")
        validate_mapping_kwargs(self.operator_class, "partial", self.kwargs)

    def __repr__(self) -> str:
        args = ", ".join(f"{k}={v!r}" for k, v in self.kwargs.items())
        return f"{self.operator_class.__name__}.partial({args})"

    def __del__(self):
        if not self._map_called:
            warnings.warn(f"{self!r} was never mapped!")

    def map(self, **mapped_kwargs) -> "MappedOperator":
        from airflow.operators.dummy import DummyOperator

        validate_mapping_kwargs(self.operator_class, "map", mapped_kwargs)

        partial_kwargs = self.kwargs.copy()
        task_id = partial_kwargs.pop("task_id")
        params = partial_kwargs.pop("params")
        dag = partial_kwargs.pop("dag")
        task_group = partial_kwargs.pop("task_group")
        start_date = partial_kwargs.pop("start_date")
        end_date = partial_kwargs.pop("end_date")

        operator = MappedOperator(
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
        self._map_called = True
        return operator


@attr.define(kw_only=True)
class MappedOperator(AbstractOperator):
    """Object representing a mapped operator in a DAG."""

    operator_class: Union[Type["BaseOperator"], str]
    mapped_kwargs: Dict[str, Any]
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
    task_group: Optional[TaskGroup]
    start_date: Optional[pendulum.DateTime]
    end_date: Optional[pendulum.DateTime]
    upstream_task_ids: Set[str] = attr.ib(factory=set, init=False)
    downstream_task_ids: Set[str] = attr.ib(factory=set, init=False)

    inlets: list = attr.ib(factory=list)
    outlets: list = attr.ib(factory=list)

    is_mapped: ClassVar[bool] = True
    subdag: None = None  # Since we don't support SubDagOperator, this is always None.

    def __repr__(self):
        return f"<Mapped({self._task_type}): {self.task_id}>"

    def __attrs_post_init__(self):
        prevent_duplicates(self.partial_kwargs, self.mapped_kwargs, fail_reason="mapping already partial")
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
        return operator_class.deps | {MappedTaskIsExpanded()}

    def _validate_argument_count(self) -> None:
        """Validate mapping arguments by unmapping with mocked values.

        This ensures the user passed enough arguments in the DAG definition for
        the operator to work in the task runner. This does not guarantee the
        arguments are *valid* (that depends on the actual mapping values), but
        makes sure there are *enough* of them.
        """
        if isinstance(self.operator_class, str):
            return  # No need to validate deserialized operator.
        operator = self._create_unmapped_operator(
            mapped_kwargs={k: unittest.mock.MagicMock(name=k) for k in self.mapped_kwargs},
            partial_kwargs=self.partial_kwargs,
            real=False,
        )
        if operator.task_group:
            operator.task_group._remove(operator)
        dag = operator.get_dag()
        if dag:
            dag._remove_task(operator.task_id)

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
        return self.partial_kwargs.get("run_as_user", {})

    def get_dag(self) -> Optional["DAG"]:
        """Implementing Operator."""
        return self.dag

    def serialize_for_task_group(self) -> Tuple[DagAttributeTypes, Any]:
        """Implementing DAGNode."""
        return DagAttributeTypes.OP, self.task_id

    def _create_unmapped_operator(
        self,
        *,
        mapped_kwargs: Dict[str, Any],
        partial_kwargs: Dict[str, Any],
        real: bool,
    ) -> "BaseOperator":
        assert not isinstance(self.operator_class, str)
        return self.operator_class(
            task_id=self.task_id,
            dag=self.dag,
            task_group=self.task_group,
            params=self.params,
            start_date=self.start_date,
            end_date=self.end_date,
            _airflow_map_validation=not real,
            **mapped_kwargs,
            **partial_kwargs,
        )

    def unmap(self) -> "BaseOperator":
        """Get the "normal" Operator after applying the current mapping"""
        dag = self.dag
        if not dag:
            raise RuntimeError("Cannot unmap a task without a DAG")
        dag._remove_task(self.task_id)
        return self._create_unmapped_operator(
            mapped_kwargs=self.mapped_kwargs,
            partial_kwargs=self.partial_kwargs,
            real=True,
        )

    def expand_mapped_task(
        self,
        upstream_ti: "TaskInstance",
        session: Session = NEW_SESSION,
    ) -> Sequence["TaskInstance"]:
        """Create the mapped task instances for mapped task.

        :return: The mapped task instances, in ascending order by map index.
        """
        # TODO: support having multiuple mapped upstreams?
        from airflow.models.taskinstance import TaskInstance
        from airflow.models.taskmap import TaskMap
        from airflow.settings import task_instance_mutation_hook

        task_map_info_length: Optional[int] = (
            session.query(TaskMap.length)
            .filter_by(
                dag_id=upstream_ti.dag_id,
                task_id=upstream_ti.task_id,
                run_id=upstream_ti.run_id,
                map_index=upstream_ti.map_index,
            )
            .scalar()
        )
        if task_map_info_length is None:
            # TODO: What would lead to this? How can this be better handled?
            raise RuntimeError("mapped operator cannot be expanded; upstream not found")

        state = None
        unmapped_ti: Optional[TaskInstance] = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == upstream_ti.dag_id,
                TaskInstance.run_id == upstream_ti.run_id,
                TaskInstance.task_id == self.task_id,
                TaskInstance.map_index == -1,
                or_(TaskInstance.state.in_(State.unfinished), TaskInstance.state.is_(None)),
            )
            .one_or_none()
        )

        ret: List[TaskInstance] = []

        if unmapped_ti:
            # The unmapped task instance still exists and is unfinished, i.e. we
            # haven't tried to run it before.
            if task_map_info_length < 1:
                # If the upstream maps this to a zero-length value, simply marked the
                # unmapped task instance as SKIPPED (if needed).
                self.log.info("Marking %s as SKIPPED since the map has 0 values to expand", unmapped_ti)
                unmapped_ti.state = TaskInstanceState.SKIPPED
                session.flush()
                return ret
            # Otherwise convert this into the first mapped index, and create
            # TaskInstance for other indexes.
            unmapped_ti.map_index = 0
            state = unmapped_ti.state
            self.log.debug("Updated in place to become %s", unmapped_ti)
            ret.append(unmapped_ti)
            indexes_to_map = range(1, task_map_info_length)
        else:
            # Only create "missing" ones.
            current_max_mapping = (
                session.query(func.max(TaskInstance.map_index))
                .filter(
                    TaskInstance.dag_id == upstream_ti.dag_id,
                    TaskInstance.task_id == self.task_id,
                    TaskInstance.run_id == upstream_ti.run_id,
                )
                .scalar()
            )
            indexes_to_map = range(current_max_mapping + 1, task_map_info_length)

        for index in indexes_to_map:
            # TODO: Make more efficient with bulk_insert_mappings/bulk_save_mappings.
            # TODO: Change `TaskInstance` ctor to take Operator, not BaseOperator
            ti = TaskInstance(self, run_id=upstream_ti.run_id, map_index=index, state=state)  # type: ignore
            self.log.debug("Expanding TIs upserted %s", ti)
            task_instance_mutation_hook(ti)
            ret.append(session.merge(ti))

        # Set to "REMOVED" any (old) TaskInstances with map indices greater
        # than the current map value
        session.query(TaskInstance).filter(
            TaskInstance.dag_id == upstream_ti.dag_id,
            TaskInstance.task_id == self.task_id,
            TaskInstance.run_id == upstream_ti.run_id,
            TaskInstance.map_index >= task_map_info_length,
        ).update({TaskInstance.state: TaskInstanceState.REMOVED})

        session.flush()

        return ret

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

import copy
import functools
import itertools
import operator
import re
import weakref
from typing import TYPE_CHECKING, TypedDict, cast, overload

import attrs
import structlog
from sqlalchemy import func, or_, select, tuple_

from airflow._shared.timezones.timezone import coerce_datetime
from airflow.configuration import conf as airflow_conf
from airflow.exceptions import AirflowException, TaskNotFound
from airflow.models.dag import DagModel
from airflow.models.dag_version import DagVersion
from airflow.models.dagrun import DagRun
from airflow.models.deadline import Deadline
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.models.tasklog import LogTemplate
from airflow.observability.stats import Stats
from airflow.sdk.definitions.deadline import DeadlineReference
from airflow.serialization.definitions.param import SerializedParamsDict
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

if TYPE_CHECKING:
    import datetime
    from collections.abc import Collection, Iterable, Sequence
    from typing import Any, Literal

    from pendulum.tz.timezone import FixedTimezone, Timezone
    from pydantic import NonNegativeInt
    from sqlalchemy.orm import Session
    from typing_extensions import TypeIs

    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk import DAG
    from airflow.sdk.definitions.deadline import DeadlineAlert
    from airflow.serialization.definitions.taskgroup import SerializedTaskGroup
    from airflow.serialization.serialized_objects import LazyDeserializedDAG, SerializedOperator
    from airflow.timetables.base import Timetable
    from airflow.utils.types import DagRunTriggeredByType

log = structlog.get_logger(__name__)


# TODO (GH-52141): Share definition with SDK?
class EdgeInfoType(TypedDict):
    """
    Extra metadata that the Dag can store about an edge.

    This is duplicated from SDK.
    """

    label: str | None


@attrs.define(eq=False, hash=False, slots=False)
class SerializedDAG:
    """
    Serialized representation of a ``DAG`` instance.

    A stringified DAG can only be used in the scope of scheduler and webserver.
    Fields that are not serializable, such as functions and customer defined
    classes, are casted to strings.
    """

    dag_id: str
    dag_display_name: str = attrs.field(default=attrs.Factory(operator.attrgetter("dag_id"), takes_self=True))

    # Default values of fields below should match schema default.
    access_control: dict[str, dict[str, Collection[str]]] | None = None
    catchup: bool = False
    dagrun_timeout: datetime.timedelta | None = None
    deadline: list[DeadlineAlert] | DeadlineAlert | None = None
    default_args: dict[str, Any] = attrs.field(factory=dict)
    description: str | None = None
    disable_bundle_versioning: bool = False
    doc_md: str | None = None
    edge_info: dict[str, dict[str, EdgeInfoType]] = attrs.field(factory=dict)
    end_date: datetime.datetime | None = None
    fail_fast: bool = False
    has_on_failure_callback: bool = False
    has_on_success_callback: bool = False
    is_paused_upon_creation: bool | None = None
    max_active_runs: int = 16
    max_active_tasks: int = 16
    max_consecutive_failed_dag_runs: int = 0
    owner_links: dict[str, str] = attrs.field(factory=dict)
    params: SerializedParamsDict = attrs.field(factory=SerializedParamsDict)
    partial: bool = False
    render_template_as_native_obj: bool = False
    start_date: datetime.datetime | None = None
    tags: set[str] = attrs.field(factory=set)
    template_searchpath: tuple[str, ...] | None = None

    # These are set dynamically during deserialization.
    task_dict: dict[str, SerializedOperator] = attrs.field(init=False)
    task_group: SerializedTaskGroup = attrs.field(init=False)
    timetable: Timetable = attrs.field(init=False)
    timezone: FixedTimezone | Timezone = attrs.field(init=False)

    # Only on serialized dag.
    last_loaded: datetime.datetime = attrs.field(init=False)
    # Determine the relative fileloc based only on the serialize dag.
    _processor_dags_folder: str = attrs.field(init=False)

    def __init__(self, *, dag_id: str) -> None:
        self.__attrs_init__(dag_id=dag_id, dag_display_name=dag_id)  # type: ignore[attr-defined]

    def __repr__(self) -> str:
        return f"<SerializedDAG: {self.dag_id}>"

    @classmethod
    def get_serialized_fields(cls) -> frozenset[str]:
        return frozenset(
            {
                "access_control",
                "catchup",
                "dag_display_name",
                "dag_id",
                "dagrun_timeout",
                "deadline",
                "default_args",
                "description",
                "disable_bundle_versioning",
                "doc_md",
                "edge_info",
                "end_date",
                "fail_fast",
                "fileloc",
                "is_paused_upon_creation",
                "max_active_runs",
                "max_active_tasks",
                "max_consecutive_failed_dag_runs",
                "owner_links",
                "relative_fileloc",
                "render_template_as_native_obj",
                "start_date",
                "tags",
                "task_group",
                "timetable",
                "timezone",
            }
        )

    @classmethod
    @provide_session
    def bulk_write_to_db(
        cls,
        bundle_name: str,
        bundle_version: str | None,
        dags: Collection[DAG | LazyDeserializedDAG],
        parse_duration: float | None = None,
        session: Session = NEW_SESSION,
    ) -> None:
        """
        Ensure the DagModel rows for the given dags are up-to-date in the dag table in the DB.

        :param dags: the DAG objects to save to the DB
        :return: None
        """
        if not dags:
            return

        from airflow.dag_processing.collection import AssetModelOperation, DagModelOperation
        from airflow.serialization.serialized_objects import LazyDeserializedDAG

        log.info("Bulk-writing dags to db", count=len(dags))
        dag_op = DagModelOperation(
            bundle_name=bundle_name,
            bundle_version=bundle_version,
            dags={d.dag_id: LazyDeserializedDAG.from_dag(d) for d in dags},
        )

        orm_dags = dag_op.add_dags(session=session)
        dag_op.update_dags(orm_dags, parse_duration, session=session)

        asset_op = AssetModelOperation.collect(dag_op.dags)

        orm_assets = asset_op.sync_assets(session=session)
        orm_asset_aliases = asset_op.sync_asset_aliases(session=session)
        session.flush()  # This populates id so we can create fks in later calls.

        orm_dags = dag_op.find_orm_dags(session=session)  # Refetch so relationship is up to date.
        asset_op.add_dag_asset_references(orm_dags, orm_assets, session=session)
        asset_op.add_dag_asset_alias_references(orm_dags, orm_asset_aliases, session=session)
        asset_op.add_dag_asset_name_uri_references(session=session)
        asset_op.add_task_asset_references(orm_dags, orm_assets, session=session)
        asset_op.activate_assets_if_possible(orm_assets.values(), session=session)
        session.flush()  # Activation is needed when we add trigger references.

        asset_op.add_asset_trigger_references(orm_assets, session=session)
        dag_op.update_dag_asset_expression(orm_dags=orm_dags, orm_assets=orm_assets)
        session.flush()

    @property
    def tasks(self) -> Sequence[SerializedOperator]:
        return list(self.task_dict.values())

    @property
    def task_ids(self) -> list[str]:
        return list(self.task_dict)

    @property
    def roots(self) -> list[SerializedOperator]:
        return [task for task in self.tasks if not task.upstream_list]

    @property
    def owner(self) -> str:
        return ", ".join({t.owner for t in self.tasks})

    def has_task(self, task_id: str) -> bool:
        return task_id in self.task_dict

    def get_task(self, task_id: str) -> SerializedOperator:
        if task_id in self.task_dict:
            return self.task_dict[task_id]
        raise TaskNotFound(f"Task {task_id} not found")

    @property
    def task_group_dict(self):
        return {k: v for k, v in self.task_group.get_task_group_dict().items() if k is not None}

    def partial_subset(
        self,
        task_ids: str | Iterable[str],
        include_downstream: bool = False,
        include_upstream: bool = True,
        include_direct_upstream: bool = False,
        exclude_original: bool = False,
    ):
        from airflow.serialization.definitions.baseoperator import SerializedBaseOperator
        from airflow.serialization.definitions.mappedoperator import SerializedMappedOperator

        def is_task(obj) -> TypeIs[SerializedOperator]:
            return isinstance(obj, (SerializedMappedOperator, SerializedBaseOperator))

        # deep-copying self.task_dict and self.task_group takes a long time, and we don't want all
        # the tasks anyway, so we copy the tasks manually later
        memo = {id(self.task_dict): None, id(self.task_group): None}
        dag = copy.deepcopy(self, memo)

        if isinstance(task_ids, str):
            matched_tasks = [t for t in self.tasks if task_ids in t.task_id]
        else:
            matched_tasks = [t for t in self.tasks if t.task_id in task_ids]

        also_include_ids: set[str] = set()
        for t in matched_tasks:
            if include_downstream:
                for rel in t.get_flat_relatives(upstream=False):
                    also_include_ids.add(rel.task_id)
                    if rel not in matched_tasks:  # if it's in there, we're already processing it
                        # need to include setups and teardowns for tasks that are in multiple
                        # non-collinear setup/teardown paths
                        if not rel.is_setup and not rel.is_teardown:
                            also_include_ids.update(
                                x.task_id for x in rel.get_upstreams_only_setups_and_teardowns()
                            )
            if include_upstream:
                also_include_ids.update(x.task_id for x in t.get_upstreams_follow_setups())
            else:
                if not t.is_setup and not t.is_teardown:
                    also_include_ids.update(x.task_id for x in t.get_upstreams_only_setups_and_teardowns())
            if t.is_setup and not include_downstream:
                also_include_ids.update(x.task_id for x in t.downstream_list if x.is_teardown)

        also_include: list[SerializedOperator] = [self.task_dict[x] for x in also_include_ids]
        direct_upstreams: list[SerializedOperator] = []
        if include_direct_upstream:
            for t in itertools.chain(matched_tasks, also_include):
                direct_upstreams.extend(u for u in t.upstream_list if is_task(u))

        # Make sure to not recursively deepcopy the dag or task_group while copying the task.
        # task_group is reset later
        def _deepcopy_task(t) -> SerializedOperator:
            memo.setdefault(id(t.task_group), None)
            return copy.deepcopy(t, memo)

        # Compiling the unique list of tasks that made the cut
        if exclude_original:
            matched_tasks = []
        dag.task_dict = {
            t.task_id: _deepcopy_task(t)
            for t in itertools.chain(matched_tasks, also_include, direct_upstreams)
        }

        def filter_task_group(group, parent_group):
            """Exclude tasks not included in the partial dag from the given TaskGroup."""
            # We want to deepcopy _most but not all_ attributes of the task group, so we create a shallow copy
            # and then manually deep copy the instances. (memo argument to deepcopy only works for instances
            # of classes, not "native" properties of an instance)
            copied = copy.copy(group)

            memo[id(group.children)] = {}
            if parent_group:
                memo[id(group.parent_group)] = parent_group
            for attr in type(group).__slots__:
                value = getattr(group, attr)
                value = copy.deepcopy(value, memo)
                object.__setattr__(copied, attr, value)

            proxy = weakref.proxy(copied)

            for child in group.children.values():
                if is_task(child):
                    if child.task_id in dag.task_dict:
                        task = copied.children[child.task_id] = dag.task_dict[child.task_id]
                        task.task_group = proxy
                else:
                    filtered_child = filter_task_group(child, proxy)

                    # Only include this child TaskGroup if it is non-empty.
                    if filtered_child.children:
                        copied.children[child.group_id] = filtered_child

            return copied

        object.__setattr__(dag, "task_group", filter_task_group(self.task_group, None))

        # Removing upstream/downstream references to tasks and TaskGroups that did not make
        # the cut.
        groups = dag.task_group.get_task_group_dict()
        for g in groups.values():
            g.upstream_group_ids.intersection_update(groups)
            g.downstream_group_ids.intersection_update(groups)
            g.upstream_task_ids.intersection_update(dag.task_dict)
            g.downstream_task_ids.intersection_update(dag.task_dict)

        for t in dag.tasks:
            # Removing upstream/downstream references to tasks that did not
            # make the cut
            t.upstream_task_ids.intersection_update(dag.task_dict)
            t.downstream_task_ids.intersection_update(dag.task_dict)

        dag.partial = len(dag.tasks) < len(self.tasks)

        return dag

    @functools.cached_property
    def _time_restriction(self) -> TimeRestriction:
        start_dates = [t.start_date for t in self.tasks if t.start_date]
        if self.start_date is not None:
            start_dates.append(self.start_date)
        earliest = None
        if start_dates:
            earliest = coerce_datetime(min(start_dates))
        latest = coerce_datetime(self.end_date)
        end_dates = [t.end_date for t in self.tasks if t.end_date]
        if len(end_dates) == len(self.tasks):  # not exists null end_date
            if self.end_date is not None:
                end_dates.append(self.end_date)
            if end_dates:
                latest = coerce_datetime(max(end_dates))
        return TimeRestriction(earliest, latest, self.catchup)

    def next_dagrun_info(
        self,
        last_automated_dagrun: None | DataInterval,
        *,
        restricted: bool = True,
    ) -> DagRunInfo | None:
        """
        Get information about the next DagRun of this dag after ``date_last_automated_dagrun``.

        This calculates what time interval the next DagRun should operate on
        (its logical date) and when it can be scheduled, according to the
        dag's timetable, start_date, end_date, etc. This doesn't check max
        active run or any other "max_active_tasks" type limits, but only
        performs calculations based on the various date and interval fields of
        this dag and its tasks.

        :param last_automated_dagrun: The ``max(logical_date)`` of
            existing "automated" DagRuns for this dag (scheduled or backfill,
            but not manual).
        :param restricted: If set to *False* (default is *True*), ignore
            ``start_date``, ``end_date``, and ``catchup`` specified on the DAG
            or tasks.
        :return: DagRunInfo of the next dagrun, or None if a dagrun is not
            going to be scheduled.
        """
        if restricted:
            restriction = self._time_restriction
        else:
            restriction = TimeRestriction(earliest=None, latest=None, catchup=True)
        try:
            info = self.timetable.next_dagrun_info(
                last_automated_data_interval=last_automated_dagrun,
                restriction=restriction,
            )
        except Exception:
            log.exception(
                "Failed to fetch run info after data interval %s for DAG %r",
                last_automated_dagrun,
                self.dag_id,
            )
            info = None
        return info

    def iter_dagrun_infos_between(
        self,
        earliest: datetime.datetime | None,
        latest: datetime.datetime,
        *,
        align: bool = True,
    ) -> Iterable[DagRunInfo]:
        """
        Yield DagRunInfo using this DAG's timetable between given interval.

        DagRunInfo instances yielded if their ``logical_date`` is not earlier
        than ``earliest``, nor later than ``latest``. The instances are ordered
        by their ``logical_date`` from earliest to latest.

        If ``align`` is ``False``, the first run will happen immediately on
        ``earliest``, even if it does not fall on the logical timetable schedule.
        The default is ``True``.

        Example: A DAG is scheduled to run every midnight (``0 0 * * *``). If
        ``earliest`` is ``2021-06-03 23:00:00``, the first DagRunInfo would be
        ``2021-06-03 23:00:00`` if ``align=False``, and ``2021-06-04 00:00:00``
        if ``align=True``.
        """
        if earliest is None:
            earliest = self._time_restriction.earliest
        if earliest is None:
            raise ValueError("earliest was None and we had no value in time_restriction to fallback on")
        earliest = coerce_datetime(earliest)
        latest = coerce_datetime(latest)

        restriction = TimeRestriction(earliest, latest, catchup=True)

        try:
            info = self.timetable.next_dagrun_info(
                last_automated_data_interval=None,
                restriction=restriction,
            )
        except Exception:
            log.exception(
                "Failed to fetch run info after data interval %s for DAG %r",
                None,
                self.dag_id,
            )
            info = None

        if info is None:
            # No runs to be scheduled between the user-supplied timeframe. But
            # if align=False, "invent" a data interval for the timeframe itself.
            if not align:
                yield DagRunInfo.interval(earliest, latest)
            return

        # If align=False and earliest does not fall on the timetable's logical
        # schedule, "invent" a data interval for it.
        if not align and info.logical_date != earliest:
            yield DagRunInfo.interval(earliest, info.data_interval.start)

        # Generate naturally according to schedule.
        while info is not None:
            yield info
            try:
                info = self.timetable.next_dagrun_info(
                    last_automated_data_interval=info.data_interval,
                    restriction=restriction,
                )
            except Exception:
                log.exception(
                    "Failed to fetch run info after data interval %s for DAG %r",
                    info.data_interval if info else "<NONE>",
                    self.dag_id,
                )
                break

    @provide_session
    def get_concurrency_reached(self, session=NEW_SESSION) -> bool:
        """Return a boolean indicating whether the max_active_tasks limit for this DAG has been reached."""
        from airflow.models.taskinstance import TaskInstance

        total_tasks = session.scalar(
            select(func.count(TaskInstance.task_id)).where(
                TaskInstance.dag_id == self.dag_id,
                TaskInstance.state == TaskInstanceState.RUNNING,
            )
        )
        return total_tasks >= self.max_active_tasks

    @provide_session
    def create_dagrun(
        self,
        *,
        run_id: str,
        logical_date: datetime.datetime | None = None,
        data_interval: tuple[datetime.datetime, datetime.datetime] | None = None,
        run_after: datetime.datetime,
        conf: dict | None = None,
        run_type: DagRunType,
        triggered_by: DagRunTriggeredByType,
        triggering_user_name: str | None = None,
        state: DagRunState,
        start_date: datetime.datetime | None = None,
        creating_job_id: int | None = None,
        backfill_id: NonNegativeInt | None = None,
        partition_key: str | None = None,
        session: Session = NEW_SESSION,
    ) -> DagRun:
        """
        Create a run for this DAG to run its tasks.

        :param run_id: ID of the dag_run
        :param logical_date: date of execution
        :param run_after: the datetime before which dag won't run
        :param conf: Dict containing configuration/parameters to pass to the DAG
        :param triggered_by: the entity which triggers the dag_run
        :param triggering_user_name: the user name who triggers the dag_run
        :param start_date: the date this dag run should be evaluated
        :param creating_job_id: ID of the job creating this DagRun
        :param backfill_id: ID of the backfill run if one exists
        :param session: Unused. Only added in compatibility with database isolation mode
        :return: The created DAG run.

        :meta private:
        """
        from airflow.models.dagrun import RUN_ID_REGEX

        logical_date = coerce_datetime(logical_date)
        # For manual runs where logical_date is None, ensure no data_interval is set.
        if logical_date is None and data_interval is not None:
            raise ValueError("data_interval must be None when logical_date is None")

        if data_interval and not isinstance(data_interval, DataInterval):
            data_interval = DataInterval(*map(coerce_datetime, data_interval))

        if isinstance(run_type, DagRunType):
            pass
        elif isinstance(run_type, str):  # Ensure the input value is valid.
            run_type = DagRunType(run_type)
        else:
            raise ValueError(f"run_type should be a DagRunType, not {type(run_type)}")

        if not isinstance(run_id, str):
            raise ValueError(f"`run_id` should be a str, not {type(run_id)}")

        # This is also done on the DagRun model class, but SQLAlchemy column
        # validator does not work well for some reason.
        if not re.match(RUN_ID_REGEX, run_id):
            regex = airflow_conf.get("scheduler", "allowed_run_id_pattern").strip()
            if not regex or not re.match(regex, run_id):
                raise ValueError(
                    f"The run_id provided '{run_id}' does not match regex pattern "
                    f"'{regex}' or '{RUN_ID_REGEX}'"
                )

        # Prevent a manual run from using an ID that looks like a scheduled run.
        if run_type == DagRunType.MANUAL:
            if (inferred_run_type := DagRunType.from_run_id(run_id)) != DagRunType.MANUAL:
                raise ValueError(
                    f"A {run_type.value} DAG run cannot use ID {run_id!r} since it "
                    f"is reserved for {inferred_run_type.value} runs"
                )

        # todo: AIP-78 add verification that if run type is backfill then we have a backfill id
        copied_params = self.params.deep_merge(conf)
        copied_params.validate()
        orm_dagrun = _create_orm_dagrun(
            dag=self,
            run_id=run_id,
            logical_date=logical_date,
            data_interval=data_interval,
            run_after=coerce_datetime(run_after),
            start_date=coerce_datetime(start_date),
            conf=conf,
            state=state,
            run_type=run_type,
            creating_job_id=creating_job_id,
            backfill_id=backfill_id,
            triggered_by=triggered_by,
            triggering_user_name=triggering_user_name,
            partition_key=partition_key,
            session=session,
        )

        if self.deadline:
            for deadline in cast("list", self.deadline):
                if isinstance(deadline.reference, DeadlineReference.TYPES.DAGRUN):
                    deadline_time = deadline.reference.evaluate_with(
                        session=session,
                        interval=deadline.interval,
                        dag_id=self.dag_id,
                        run_id=run_id,
                    )
                    if deadline_time is not None:
                        session.add(
                            Deadline(
                                deadline_time=deadline_time,
                                callback=deadline.callback,
                                dagrun_id=orm_dagrun.id,
                                dag_id=orm_dagrun.dag_id,
                            )
                        )
                        Stats.incr("deadline_alerts.deadline_created", tags={"dag_id": self.dag_id})

        return orm_dagrun

    @provide_session
    def set_task_instance_state(
        self,
        *,
        task_id: str,
        map_indexes: Collection[int] | None = None,
        run_id: str | None = None,
        state: TaskInstanceState,
        upstream: bool = False,
        downstream: bool = False,
        future: bool = False,
        past: bool = False,
        commit: bool = True,
        session=NEW_SESSION,
    ) -> list[TaskInstance]:
        """
        Set the state of a TaskInstance and clear downstream tasks in failed or upstream_failed state.

        :param task_id: Task ID of the TaskInstance
        :param map_indexes: Only set TaskInstance if its map_index matches.
            If None (default), all mapped TaskInstances of the task are set.
        :param run_id: The run_id of the TaskInstance
        :param state: State to set the TaskInstance to
        :param upstream: Include all upstream tasks of the given task_id
        :param downstream: Include all downstream tasks of the given task_id
        :param future: Include all future TaskInstances of the given task_id
        :param commit: Commit changes
        :param past: Include all past TaskInstances of the given task_id
        """
        from airflow.api.common.mark_tasks import set_state

        task = self.get_task(task_id)
        task.dag = self

        tasks_to_set_state: list[SerializedOperator | tuple[SerializedOperator, int]]
        if map_indexes is None:
            tasks_to_set_state = [task]
        else:
            tasks_to_set_state = [(task, map_index) for map_index in map_indexes]

        altered = set_state(
            tasks=tasks_to_set_state,
            run_id=run_id,
            upstream=upstream,
            downstream=downstream,
            future=future,
            past=past,
            state=state,
            commit=commit,
            session=session,
        )

        if not commit:
            return altered

        # Clear downstream tasks that are in failed/upstream_failed state to resume them.
        # Flush the session so that the tasks marked success are reflected in the db.
        session.flush()
        subset = self.partial_subset(
            task_ids={task_id},
            include_downstream=True,
            include_upstream=False,
        )

        # Raises an error if not found
        dr_id, logical_date = session.execute(
            select(DagRun.id, DagRun.logical_date).where(
                DagRun.run_id == run_id, DagRun.dag_id == self.dag_id
            )
        ).one()

        # Now we want to clear downstreams of tasks that had their state set...
        clear_kwargs = {
            "only_failed": True,
            "session": session,
            # Exclude the task itself from being cleared.
            "exclude_task_ids": frozenset((task_id,)),
        }
        if not future and not past:  # Simple case 1: we're only dealing with exactly one run.
            clear_kwargs["run_id"] = run_id
            subset.clear(**clear_kwargs)
        elif future and past:  # Simple case 2: we're clearing ALL runs.
            subset.clear(**clear_kwargs)
        else:  # Complex cases: we may have more than one run, based on a date range.
            # Make 'future' and 'past' make some sense when multiple runs exist
            # for the same logical date. We order runs by their id and only
            # clear runs have larger/smaller ids.
            exclude_run_id_stmt = select(DagRun.run_id).where(DagRun.logical_date == logical_date)
            if future:
                clear_kwargs["start_date"] = logical_date
                exclude_run_id_stmt = exclude_run_id_stmt.where(DagRun.id > dr_id)
            else:
                clear_kwargs["end_date"] = logical_date
                exclude_run_id_stmt = exclude_run_id_stmt.where(DagRun.id < dr_id)
            subset.clear(exclude_run_ids=frozenset(session.scalars(exclude_run_id_stmt)), **clear_kwargs)
        return altered

    @overload
    def _get_task_instances(
        self,
        *,
        task_ids: Collection[str | tuple[str, int]] | None,
        start_date: datetime.datetime | None,
        end_date: datetime.datetime | None,
        run_id: str | None,
        state: TaskInstanceState | Sequence[TaskInstanceState],
        exclude_task_ids: Collection[str | tuple[str, int]] | None,
        exclude_run_ids: frozenset[str] | None,
        session: Session,
    ) -> Iterable[TaskInstance]: ...  # pragma: no cover

    @overload
    def _get_task_instances(
        self,
        *,
        task_ids: Collection[str | tuple[str, int]] | None,
        as_pk_tuple: Literal[True],
        start_date: datetime.datetime | None,
        end_date: datetime.datetime | None,
        run_id: str | None,
        state: TaskInstanceState | Sequence[TaskInstanceState],
        exclude_task_ids: Collection[str | tuple[str, int]] | None,
        exclude_run_ids: frozenset[str] | None,
        session: Session,
    ) -> set[TaskInstanceKey]: ...  # pragma: no cover

    def _get_task_instances(
        self,
        *,
        task_ids: Collection[str | tuple[str, int]] | None,
        as_pk_tuple: Literal[True, None] = None,
        start_date: datetime.datetime | None,
        end_date: datetime.datetime | None,
        run_id: str | None,
        state: TaskInstanceState | Sequence[TaskInstanceState],
        exclude_task_ids: Collection[str | tuple[str, int]] | None,
        exclude_run_ids: frozenset[str] | None,
        session: Session,
    ) -> Iterable[TaskInstance] | set[TaskInstanceKey]:
        from airflow.models.taskinstance import TaskInstance

        # If we are looking at dependent dags we want to avoid UNION calls
        # in SQL (it doesn't play nice with fields that have no equality operator,
        # like JSON types), we instead build our result set separately.
        #
        # This will be empty if we are only looking at one dag, in which case
        # we can return the filtered TI query object directly.
        result: set[TaskInstanceKey] = set()

        # Do we want full objects, or just the primary columns?
        if as_pk_tuple:
            tis_pk = select(
                TaskInstance.dag_id,
                TaskInstance.task_id,
                TaskInstance.run_id,
                TaskInstance.map_index,
            )
            tis_pk = tis_pk.join(TaskInstance.dag_run)
        else:
            tis_full = select(TaskInstance)
            tis_full = tis_full.join(TaskInstance.dag_run)

        # Apply common filters
        def apply_filters(query):
            if self.partial:
                query = query.where(
                    TaskInstance.dag_id == self.dag_id, TaskInstance.task_id.in_(self.task_ids)
                )
            else:
                query = query.where(TaskInstance.dag_id == self.dag_id)
            if run_id:
                query = query.where(TaskInstance.run_id == run_id)
            if start_date:
                query = query.where(DagRun.logical_date >= start_date)
            if task_ids is not None:
                # Use the selector condition directly without intermediate variable
                query = query.where(TaskInstance.ti_selector_condition(task_ids))
            if end_date:
                query = query.where(DagRun.logical_date <= end_date)
            return query

        if as_pk_tuple:
            tis_pk = apply_filters(tis_pk)
        else:
            tis_full = apply_filters(tis_full)

        def apply_state_filter(query):
            if state:
                if isinstance(state, (str, TaskInstanceState)):
                    query = query.where(TaskInstance.state == state)
                elif len(state) == 1:
                    query = query.where(TaskInstance.state == state[0])
                else:
                    # this is required to deal with NULL values
                    if None in state:
                        if all(x is None for x in state):
                            query = query.where(TaskInstance.state.is_(None))
                        else:
                            not_none_state = [s for s in state if s]
                            query = query.where(
                                or_(TaskInstance.state.in_(not_none_state), TaskInstance.state.is_(None))
                            )
                    else:
                        query = query.where(TaskInstance.state.in_(state))

            if exclude_run_ids:
                query = query.where(TaskInstance.run_id.not_in(exclude_run_ids))
            return query

        if as_pk_tuple:
            tis_pk = apply_state_filter(tis_pk)
        else:
            tis_full = apply_state_filter(tis_full)

        if result or as_pk_tuple:
            # Only execute the `ti` query if we have also collected some other results
            if as_pk_tuple:
                tis_query = session.execute(tis_pk).all()
                result.update(TaskInstanceKey(**cols._mapping) for cols in tis_query)
            else:
                result.update(ti.key for ti in session.scalars(tis_full))

            if exclude_task_ids is not None:
                result = {
                    task
                    for task in result
                    if task.task_id not in exclude_task_ids
                    and (task.task_id, task.map_index) not in exclude_task_ids
                }

        if as_pk_tuple:
            return result
        if result:
            # We've been asked for objects, lets combine it all back in to a result set
            ti_filters = TaskInstance.filter_for_tis(result)
            if ti_filters is not None:
                tis_final = select(TaskInstance).where(ti_filters)
                return session.scalars(tis_final)
        elif exclude_task_ids is None:
            pass  # Disable filter if not set.
        elif isinstance(next(iter(exclude_task_ids), None), str):
            tis_full = tis_full.where(TaskInstance.task_id.notin_(exclude_task_ids))
        else:
            tis_full = tis_full.where(
                tuple_(TaskInstance.task_id, TaskInstance.map_index).not_in(exclude_task_ids)
            )

        return session.scalars(tis_full)

    @overload
    def clear(
        self,
        *,
        dry_run: Literal[True],
        task_ids: Collection[str | tuple[str, int]] | None = None,
        run_id: str,
        only_failed: bool = False,
        only_running: bool = False,
        dag_run_state: DagRunState = DagRunState.QUEUED,
        session: Session = NEW_SESSION,
        exclude_task_ids: frozenset[str] | frozenset[tuple[str, int]] | None = frozenset(),
        exclude_run_ids: frozenset[str] | None = frozenset(),
        run_on_latest_version: bool = False,
    ) -> list[TaskInstance]: ...  # pragma: no cover

    @overload
    def clear(
        self,
        *,
        task_ids: Collection[str | tuple[str, int]] | None = None,
        run_id: str,
        only_failed: bool = False,
        only_running: bool = False,
        dag_run_state: DagRunState = DagRunState.QUEUED,
        dry_run: Literal[False] = False,
        session: Session = NEW_SESSION,
        exclude_task_ids: frozenset[str] | frozenset[tuple[str, int]] | None = frozenset(),
        exclude_run_ids: frozenset[str] | None = frozenset(),
        run_on_latest_version: bool = False,
    ) -> int: ...  # pragma: no cover

    @overload
    def clear(
        self,
        *,
        dry_run: Literal[True],
        task_ids: Collection[str | tuple[str, int]] | None = None,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        only_failed: bool = False,
        only_running: bool = False,
        dag_run_state: DagRunState = DagRunState.QUEUED,
        session: Session = NEW_SESSION,
        exclude_task_ids: frozenset[str] | frozenset[tuple[str, int]] | None = frozenset(),
        exclude_run_ids: frozenset[str] | None = frozenset(),
        run_on_latest_version: bool = False,
    ) -> list[TaskInstance]: ...  # pragma: no cover

    @overload
    def clear(
        self,
        *,
        task_ids: Collection[str | tuple[str, int]] | None = None,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        only_failed: bool = False,
        only_running: bool = False,
        dag_run_state: DagRunState = DagRunState.QUEUED,
        dry_run: Literal[False] = False,
        session: Session = NEW_SESSION,
        exclude_task_ids: frozenset[str] | frozenset[tuple[str, int]] | None = frozenset(),
        exclude_run_ids: frozenset[str] | None = frozenset(),
        run_on_latest_version: bool = False,
    ) -> int: ...  # pragma: no cover

    @provide_session
    def clear(
        self,
        task_ids: Collection[str | tuple[str, int]] | None = None,
        *,
        run_id: str | None = None,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        only_failed: bool = False,
        only_running: bool = False,
        dag_run_state: DagRunState = DagRunState.QUEUED,
        dry_run: bool = False,
        session: Session = NEW_SESSION,
        exclude_task_ids: frozenset[str] | frozenset[tuple[str, int]] | None = frozenset(),
        exclude_run_ids: frozenset[str] | None = frozenset(),
        run_on_latest_version: bool = False,
    ) -> int | Iterable[TaskInstance]:
        """
        Clear a set of task instances associated with the current dag for a specified date range.

        :param task_ids: List of task ids or (``task_id``, ``map_index``) tuples to clear
        :param run_id: The run_id for which the tasks should be cleared
        :param start_date: The minimum logical_date to clear
        :param end_date: The maximum logical_date to clear
        :param only_failed: Only clear failed tasks
        :param only_running: Only clear running tasks.
        :param dag_run_state: state to set DagRun to. If set to False, dagrun state will not
            be changed.
        :param dry_run: Find the tasks to clear but don't clear them.
        :param run_on_latest_version: whether to run on latest serialized DAG and Bundle version
        :param session: The sqlalchemy session to use
        :param exclude_task_ids: A set of ``task_id`` or (``task_id``, ``map_index``)
            tuples that should not be cleared
        :param exclude_run_ids: A set of ``run_id`` or (``run_id``)
        """
        from airflow.models.taskinstance import clear_task_instances

        state: list[TaskInstanceState] = []
        if only_failed:
            state += [TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED]
        if only_running:
            # Yes, having `+=` doesn't make sense, but this was the existing behaviour
            state += [TaskInstanceState.RUNNING]

        tis_result = self._get_task_instances(
            task_ids=task_ids,
            start_date=start_date,
            end_date=end_date,
            run_id=run_id,
            state=state,
            session=session,
            exclude_task_ids=exclude_task_ids,
            exclude_run_ids=exclude_run_ids,
        )

        if dry_run:
            return list(tis_result)

        tis = list(tis_result)

        count = len(tis)
        if count == 0:
            return 0

        clear_task_instances(
            list(tis),
            session,
            dag_run_state=dag_run_state,
            run_on_latest_version=run_on_latest_version,
        )

        session.flush()
        return count

    @classmethod
    def clear_dags(
        cls,
        dags: Iterable[SerializedDAG],
        *,
        start_date=None,
        end_date=None,
        only_failed=False,
        only_running=False,
        dag_run_state=DagRunState.QUEUED,
        dry_run: bool = False,
    ):
        if dry_run:
            tis = itertools.chain.from_iterable(
                dag.clear(
                    start_date=start_date,
                    end_date=end_date,
                    only_failed=only_failed,
                    only_running=only_running,
                    dag_run_state=dag_run_state,
                    dry_run=True,
                )
                for dag in dags
            )
            return list(tis)

        return sum(
            dag.clear(
                start_date=start_date,
                end_date=end_date,
                only_failed=only_failed,
                only_running=only_running,
                dag_run_state=dag_run_state,
                dry_run=False,
            )
            for dag in dags
        )

    def get_edge_info(self, upstream_task_id: str, downstream_task_id: str) -> EdgeInfoType:
        """Return edge information for the given pair of tasks or an empty edge if there is no information."""
        # Note - older serialized dags may not have edge_info being a dict at all
        empty = cast("EdgeInfoType", {})
        if self.edge_info:
            return self.edge_info.get(upstream_task_id, {}).get(downstream_task_id, empty)
        return empty


@provide_session
def _create_orm_dagrun(
    *,
    dag: SerializedDAG,
    run_id: str,
    logical_date: datetime.datetime | None,
    data_interval: DataInterval | None,
    run_after: datetime.datetime,
    start_date: datetime.datetime | None,
    conf: Any,
    state: DagRunState | None,
    run_type: DagRunType,
    creating_job_id: int | None,
    backfill_id: NonNegativeInt | None,
    triggered_by: DagRunTriggeredByType,
    triggering_user_name: str | None = None,
    partition_key: str | None = None,
    session: Session = NEW_SESSION,
) -> DagRun:
    bundle_version = None
    if not dag.disable_bundle_versioning:
        bundle_version = session.scalar(
            select(DagModel.bundle_version).where(DagModel.dag_id == dag.dag_id),
        )
    dag_version = DagVersion.get_latest_version(dag.dag_id, session=session)
    if not dag_version:
        raise AirflowException(f"Cannot create DagRun for DAG {dag.dag_id} because the dag is not serialized")

    run = DagRun(
        dag_id=dag.dag_id,
        run_id=run_id,
        logical_date=logical_date,
        start_date=start_date,
        run_after=run_after,
        conf=conf,
        state=state,
        run_type=run_type,
        creating_job_id=creating_job_id,
        data_interval=data_interval,
        triggered_by=triggered_by,
        triggering_user_name=triggering_user_name,
        backfill_id=backfill_id,
        bundle_version=bundle_version,
        partition_key=partition_key,
    )
    # Load defaults into the following two fields to ensure result can be serialized detached
    max_log_template_id = session.scalar(select(func.max(LogTemplate.__table__.c.id)))
    run.log_template_id = int(max_log_template_id) if max_log_template_id is not None else 0
    run.created_dag_version = dag_version
    run.consumed_asset_events = []
    session.add(run)
    session.flush()
    run.dag = dag
    # create the associated task instances
    # state is None at the moment of creation
    run.verify_integrity(session=session, dag_version_id=dag_version.id)
    return run

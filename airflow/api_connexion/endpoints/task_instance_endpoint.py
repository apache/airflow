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

from typing import TYPE_CHECKING, Any, Iterable, Sequence, TypeVar

from flask import g
from marshmallow import ValidationError
from sqlalchemy import and_, or_, select
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.orm import joinedload

from airflow.api_connexion import security
from airflow.api_connexion.endpoints.request_dict import get_json_request_dict
from airflow.api_connexion.exceptions import BadRequest, NotFound, PermissionDenied
from airflow.api_connexion.parameters import format_datetime, format_parameters
from airflow.api_connexion.schemas.task_instance_schema import (
    TaskInstanceCollection,
    TaskInstanceHistoryCollection,
    TaskInstanceReferenceCollection,
    clear_task_instance_form,
    set_single_task_instance_state_form,
    set_task_instance_note_form_schema,
    set_task_instance_state_form,
    task_dependencies_collection_schema,
    task_instance_batch_form,
    task_instance_collection_schema,
    task_instance_history_collection_schema,
    task_instance_history_schema,
    task_instance_reference_collection_schema,
    task_instance_reference_schema,
    task_instance_schema,
)
from airflow.api_connexion.security import get_readable_dags
from airflow.auth.managers.models.resource_details import DagAccessEntity, DagDetails
from airflow.exceptions import TaskNotFound
from airflow.models.dagrun import DagRun as DR
from airflow.models.taskinstance import TaskInstance as TI, clear_task_instances
from airflow.models.taskinstancehistory import TaskInstanceHistory as TIH
from airflow.utils.airflow_flask_app import get_airflow_app
from airflow.utils.db import get_query_count
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.www.decorators import action_logging
from airflow.www.extensions.init_auth_manager import get_auth_manager

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.sql import ClauseElement, Select
    from sqlalchemy.sql.expression import ColumnOperators

    from airflow.api_connexion.types import APIResponse
    from airflow.auth.managers.models.batch_apis import IsAuthorizedDagRequest

T = TypeVar("T")


@security.requires_access_dag("GET", DagAccessEntity.TASK_INSTANCE)
@provide_session
def get_task_instance(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get task instance."""
    query = (
        select(TI)
        .where(TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id)
        .join(TI.dag_run)
        .options(joinedload(TI.rendered_task_instance_fields))
    )

    try:
        task_instance = session.scalar(query)
    except MultipleResultsFound:
        raise NotFound(
            "Task instance not found", detail="Task instance is mapped, add the map_index value to the URL"
        )
    if task_instance is None:
        raise NotFound("Task instance not found")
    if task_instance.map_index != -1:
        raise NotFound(
            "Task instance not found", detail="Task instance is mapped, add the map_index value to the URL"
        )

    return task_instance_schema.dump(task_instance)


@security.requires_access_dag("GET", DagAccessEntity.TASK_INSTANCE)
@provide_session
def get_mapped_task_instance(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    map_index: int,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get task instance."""
    query = (
        select(TI)
        .where(TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id, TI.map_index == map_index)
        .join(TI.dag_run)
        .options(joinedload(TI.rendered_task_instance_fields))
    )
    task_instance = session.scalar(query)

    if task_instance is None:
        raise NotFound("Task instance not found")

    return task_instance_schema.dump(task_instance)


@format_parameters(
    {
        "execution_date_gte": format_datetime,
        "execution_date_lte": format_datetime,
        "start_date_gte": format_datetime,
        "start_date_lte": format_datetime,
        "end_date_gte": format_datetime,
        "end_date_lte": format_datetime,
        "updated_at_gte": format_datetime,
        "updated_at_lte": format_datetime,
    },
)
@security.requires_access_dag("GET", DagAccessEntity.TASK_INSTANCE)
@provide_session
def get_mapped_task_instances(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    execution_date_gte: str | None = None,
    execution_date_lte: str | None = None,
    start_date_gte: str | None = None,
    start_date_lte: str | None = None,
    end_date_gte: str | None = None,
    end_date_lte: str | None = None,
    updated_at_gte: str | None = None,
    updated_at_lte: str | None = None,
    duration_gte: float | None = None,
    duration_lte: float | None = None,
    state: list[str] | None = None,
    pool: list[str] | None = None,
    queue: list[str] | None = None,
    executor: list[str] | None = None,
    limit: int | None = None,
    offset: int | None = None,
    order_by: str | None = None,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get list of task instances."""
    # Because state can be 'none'
    states = _convert_ti_states(state)

    base_query = (
        select(TI)
        .where(TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id, TI.map_index >= 0)
        .join(TI.dag_run)
    )

    # 0 can mean a mapped TI that expanded to an empty list, so it is not an automatic 404
    unfiltered_total_count = get_query_count(base_query, session=session)
    if unfiltered_total_count == 0:
        dag = get_airflow_app().dag_bag.get_dag(dag_id)
        if not dag:
            error_message = f"DAG {dag_id} not found"
            raise NotFound(error_message)
        try:
            task = dag.get_task(task_id)
        except TaskNotFound:
            error_message = f"Task id {task_id} not found"
            raise NotFound(error_message)
        if not task.get_needs_expansion():
            error_message = f"Task id {task_id} is not mapped"
            raise NotFound(error_message)

    # Other search criteria
    base_query = _apply_range_filter(
        base_query,
        key=DR.execution_date,
        value_range=(execution_date_gte, execution_date_lte),
    )
    base_query = _apply_range_filter(
        base_query, key=TI.start_date, value_range=(start_date_gte, start_date_lte)
    )
    base_query = _apply_range_filter(base_query, key=TI.end_date, value_range=(end_date_gte, end_date_lte))
    base_query = _apply_range_filter(base_query, key=TI.duration, value_range=(duration_gte, duration_lte))
    base_query = _apply_range_filter(
        base_query, key=TI.updated_at, value_range=(updated_at_gte, updated_at_lte)
    )
    base_query = _apply_array_filter(base_query, key=TI.state, values=states)
    base_query = _apply_array_filter(base_query, key=TI.pool, values=pool)
    base_query = _apply_array_filter(base_query, key=TI.queue, values=queue)
    base_query = _apply_array_filter(base_query, key=TI.executor, values=executor)

    # Count elements before joining extra columns
    total_entries = get_query_count(base_query, session=session)

    try:
        order_by_params = _get_order_by_params(order_by)
        entry_query = base_query.order_by(*order_by_params)
    except _UnsupportedOrderBy as e:
        raise BadRequest(detail=f"Ordering with {e.order_by!r} is not supported")

    task_instances = session.execute(entry_query.offset(offset).limit(limit)).all()
    return task_instance_collection_schema.dump(
        TaskInstanceCollection(task_instances=task_instances, total_entries=total_entries)
    )


def _convert_ti_states(states: Iterable[str] | None) -> list[TaskInstanceState | None] | None:
    if not states:
        return None
    return [None if s in ("none", None) else TaskInstanceState(s) for s in states]


def _apply_array_filter(query: Select, key: ClauseElement, values: Iterable[Any] | None) -> Select:
    if values is not None:
        cond = ((key == v) for v in values)
        query = query.where(or_(*cond))
    return query


def _apply_range_filter(query: Select, key: ClauseElement, value_range: tuple[T, T]) -> Select:
    gte_value, lte_value = value_range
    if gte_value is not None:
        query = query.where(key >= gte_value)
    if lte_value is not None:
        query = query.where(key <= lte_value)
    return query


class _UnsupportedOrderBy(ValueError):
    def __init__(self, order_by: str) -> None:
        super().__init__(order_by)
        self.order_by = order_by


def _get_order_by_params(order_by: str | None = None) -> tuple[ColumnOperators, ...]:
    """Return a tuple with the order by params to be used in the query."""
    if order_by is None:
        return (TI.map_index.asc(),)
    if order_by == "state":
        return (TI.state.asc(), TI.map_index.asc())
    if order_by == "-state":
        return (TI.state.desc(), TI.map_index.asc())
    if order_by == "duration":
        return (TI.duration.asc(), TI.map_index.asc())
    if order_by == "-duration":
        return (TI.duration.desc(), TI.map_index.asc())
    if order_by == "start_date":
        return (TI.start_date.asc(), TI.map_index.asc())
    if order_by == "-start_date":
        return (TI.start_date.desc(), TI.map_index.asc())
    if order_by == "end_date":
        return (TI.end_date.asc(), TI.map_index.asc())
    if order_by == "-end_date":
        return (TI.end_date.desc(), TI.map_index.asc())
    if order_by == "map_index":
        return (TI.map_index.asc(),)
    if order_by == "-map_index":
        return (TI.map_index.desc(),)
    raise _UnsupportedOrderBy(order_by)


@format_parameters(
    {
        "execution_date_gte": format_datetime,
        "execution_date_lte": format_datetime,
        "start_date_gte": format_datetime,
        "start_date_lte": format_datetime,
        "end_date_gte": format_datetime,
        "end_date_lte": format_datetime,
        "updated_at_gte": format_datetime,
        "updated_at_lte": format_datetime,
    },
)
@security.requires_access_dag("GET", DagAccessEntity.TASK_INSTANCE)
@provide_session
def get_task_instances(
    *,
    limit: int,
    dag_id: str | None = None,
    dag_run_id: str | None = None,
    execution_date_gte: str | None = None,
    execution_date_lte: str | None = None,
    start_date_gte: str | None = None,
    start_date_lte: str | None = None,
    end_date_gte: str | None = None,
    end_date_lte: str | None = None,
    updated_at_gte: str | None = None,
    updated_at_lte: str | None = None,
    duration_gte: float | None = None,
    duration_lte: float | None = None,
    state: list[str] | None = None,
    pool: list[str] | None = None,
    queue: list[str] | None = None,
    executor: list[str] | None = None,
    offset: int | None = None,
    order_by: str | None = None,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get list of task instances."""
    # Because state can be 'none'
    states = _convert_ti_states(state)

    base_query = select(TI).join(TI.dag_run)

    if dag_id != "~":
        base_query = base_query.where(TI.dag_id == dag_id)
    else:
        base_query = base_query.where(TI.dag_id.in_(get_readable_dags()))
    if dag_run_id != "~":
        base_query = base_query.where(TI.run_id == dag_run_id)
    base_query = _apply_range_filter(
        base_query,
        key=DR.execution_date,
        value_range=(execution_date_gte, execution_date_lte),
    )
    base_query = _apply_range_filter(
        base_query, key=TI.start_date, value_range=(start_date_gte, start_date_lte)
    )
    base_query = _apply_range_filter(base_query, key=TI.end_date, value_range=(end_date_gte, end_date_lte))
    base_query = _apply_range_filter(base_query, key=TI.duration, value_range=(duration_gte, duration_lte))
    base_query = _apply_range_filter(
        base_query, key=TI.updated_at, value_range=(updated_at_gte, updated_at_lte)
    )
    base_query = _apply_array_filter(base_query, key=TI.state, values=states)
    base_query = _apply_array_filter(base_query, key=TI.pool, values=pool)
    base_query = _apply_array_filter(base_query, key=TI.queue, values=queue)
    base_query = _apply_array_filter(base_query, key=TI.executor, values=executor)

    # Count elements before joining extra columns
    total_entries = get_query_count(base_query, session=session)

    try:
        order_by_params = _get_order_by_params(order_by)
        entry_query = base_query.order_by(*order_by_params)
    except _UnsupportedOrderBy as e:
        raise BadRequest(detail=f"Ordering with {e.order_by!r} is not supported")

    task_instances = session.scalars(entry_query.offset(offset).limit(limit))
    return task_instance_collection_schema.dump(
        TaskInstanceCollection(task_instances=task_instances, total_entries=total_entries)
    )


@security.requires_access_dag("GET", DagAccessEntity.TASK_INSTANCE)
@provide_session
def get_task_instances_batch(session: Session = NEW_SESSION) -> APIResponse:
    """Get list of task instances."""
    body = get_json_request_dict()
    try:
        data = task_instance_batch_form.load(body)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))
    dag_ids = data["dag_ids"]
    if dag_ids:
        requests: Sequence[IsAuthorizedDagRequest] = [
            {
                "method": "GET",
                "details": DagDetails(id=id),
            }
            for id in dag_ids
        ]
        if not get_auth_manager().batch_is_authorized_dag(requests):
            raise PermissionDenied(detail=f"User not allowed to access some of these DAGs: {list(dag_ids)}")
    else:
        dag_ids = get_auth_manager().get_permitted_dag_ids(user=g.user)

    states = _convert_ti_states(data["state"])
    base_query = select(TI).join(TI.dag_run)

    base_query = _apply_array_filter(base_query, key=TI.dag_id, values=dag_ids)
    base_query = _apply_array_filter(base_query, key=TI.run_id, values=data["dag_run_ids"])
    base_query = _apply_array_filter(base_query, key=TI.task_id, values=data["task_ids"])
    base_query = _apply_range_filter(
        base_query,
        key=DR.execution_date,
        value_range=(data["execution_date_gte"], data["execution_date_lte"]),
    )
    base_query = _apply_range_filter(
        base_query,
        key=TI.start_date,
        value_range=(data["start_date_gte"], data["start_date_lte"]),
    )
    base_query = _apply_range_filter(
        base_query, key=TI.end_date, value_range=(data["end_date_gte"], data["end_date_lte"])
    )
    base_query = _apply_range_filter(
        base_query, key=TI.duration, value_range=(data["duration_gte"], data["duration_lte"])
    )
    base_query = _apply_array_filter(base_query, key=TI.state, values=states)
    base_query = _apply_array_filter(base_query, key=TI.pool, values=data["pool"])
    base_query = _apply_array_filter(base_query, key=TI.queue, values=data["queue"])
    base_query = _apply_array_filter(base_query, key=TI.executor, values=data["executor"])

    # Count elements before joining extra columns
    total_entries = get_query_count(base_query, session=session)

    ti_query = base_query.options(
        joinedload(TI.rendered_task_instance_fields), joinedload(TI.task_instance_note)
    )

    try:
        order_by_params = _get_order_by_params(data["order_by"])
        ti_query = ti_query.order_by(*order_by_params)
    except _UnsupportedOrderBy as e:
        raise BadRequest(detail=f"Ordering with {e.order_by!r} is not supported")

    task_instances = session.scalars(ti_query)

    return task_instance_collection_schema.dump(
        TaskInstanceCollection(task_instances=task_instances, total_entries=total_entries)
    )


@security.requires_access_dag("PUT", DagAccessEntity.TASK_INSTANCE)
@action_logging
@provide_session
def post_clear_task_instances(*, dag_id: str, session: Session = NEW_SESSION) -> APIResponse:
    """Clear task instances."""
    body = get_json_request_dict()
    try:
        data = clear_task_instance_form.load(body)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))

    dag = get_airflow_app().dag_bag.get_dag(dag_id)
    if not dag:
        error_message = f"Dag id {dag_id} not found"
        raise NotFound(error_message)
    reset_dag_runs = data.pop("reset_dag_runs")
    dry_run = data.pop("dry_run")
    # We always pass dry_run here, otherwise this would try to confirm on the terminal!
    dag_run_id = data.pop("dag_run_id", None)
    future = data.pop("include_future", False)
    past = data.pop("include_past", False)
    downstream = data.pop("include_downstream", False)
    upstream = data.pop("include_upstream", False)
    if dag_run_id is not None:
        dag_run: DR | None = session.scalar(select(DR).where(DR.dag_id == dag_id, DR.run_id == dag_run_id))
        if dag_run is None:
            error_message = f"Dag Run id {dag_run_id} not found in dag {dag_id}"
            raise NotFound(error_message)
        data["start_date"] = dag_run.logical_date
        data["end_date"] = dag_run.logical_date
    if past:
        data["start_date"] = None
    if future:
        data["end_date"] = None
    task_ids = data.pop("task_ids", None)
    if task_ids is not None:
        task_id = [task[0] if isinstance(task, tuple) else task for task in task_ids]
        dag = dag.partial_subset(
            task_ids_or_regex=task_id,
            include_downstream=downstream,
            include_upstream=upstream,
        )

        if len(dag.task_dict) > 1:
            # If we had upstream/downstream etc then also include those!
            task_ids.extend(tid for tid in dag.task_dict if tid != task_id)
    task_instances = dag.clear(dry_run=True, dag_bag=get_airflow_app().dag_bag, task_ids=task_ids, **data)

    if not dry_run:
        clear_task_instances(
            task_instances,
            session,
            dag=dag,
            dag_run_state=DagRunState.QUEUED if reset_dag_runs else False,
        )

    return task_instance_reference_collection_schema.dump(
        TaskInstanceReferenceCollection(task_instances=task_instances)
    )


@security.requires_access_dag("PUT", DagAccessEntity.TASK_INSTANCE)
@action_logging
@provide_session
def post_set_task_instances_state(*, dag_id: str, session: Session = NEW_SESSION) -> APIResponse:
    """Set a state of task instances."""
    body = get_json_request_dict()
    try:
        data = set_task_instance_state_form.load(body)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))

    error_message = f"Dag ID {dag_id} not found"
    dag = get_airflow_app().dag_bag.get_dag(dag_id)
    if not dag:
        raise NotFound(error_message)

    task_id = data["task_id"]
    task = dag.task_dict.get(task_id)

    if not task:
        error_message = f"Task ID {task_id} not found"
        raise NotFound(error_message)

    execution_date = data.get("execution_date")
    run_id = data.get("dag_run_id")
    if (
        execution_date
        and (
            session.scalars(
                select(TI).where(
                    TI.task_id == task_id, TI.dag_id == dag_id, TI.execution_date == execution_date
                )
            ).one_or_none()
        )
        is None
    ):
        raise NotFound(
            detail=f"Task instance not found for task {task_id!r} on execution_date {execution_date}"
        )

    if run_id and not session.get(
        TI, {"task_id": task_id, "dag_id": dag_id, "run_id": run_id, "map_index": -1}
    ):
        error_message = f"Task instance not found for task {task_id!r} on DAG run with ID {run_id!r}"
        raise NotFound(detail=error_message)

    tis = dag.set_task_instance_state(
        task_id=task_id,
        run_id=run_id,
        execution_date=execution_date,
        state=data["new_state"],
        upstream=data["include_upstream"],
        downstream=data["include_downstream"],
        future=data["include_future"],
        past=data["include_past"],
        commit=not data["dry_run"],
        session=session,
    )
    return task_instance_reference_collection_schema.dump(TaskInstanceReferenceCollection(task_instances=tis))


def set_mapped_task_instance_note(
    *, dag_id: str, dag_run_id: str, task_id: str, map_index: int
) -> APIResponse:
    """Set the note for a Mapped Task instance."""
    return set_task_instance_note(dag_id=dag_id, dag_run_id=dag_run_id, task_id=task_id, map_index=map_index)


@security.requires_access_dag("PUT", DagAccessEntity.TASK_INSTANCE)
@action_logging
@provide_session
def patch_task_instance(
    *, dag_id: str, dag_run_id: str, task_id: str, map_index: int = -1, session: Session = NEW_SESSION
) -> APIResponse:
    """Update the state of a task instance."""
    body = get_json_request_dict()
    try:
        data = set_single_task_instance_state_form.load(body)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))

    dag = get_airflow_app().dag_bag.get_dag(dag_id)
    if not dag:
        raise NotFound("DAG not found", detail=f"DAG {dag_id!r} not found")

    if not dag.has_task(task_id):
        raise NotFound("Task not found", detail=f"Task {task_id!r} not found in DAG {dag_id!r}")

    ti: TI | None = session.get(
        TI, {"task_id": task_id, "dag_id": dag_id, "run_id": dag_run_id, "map_index": map_index}
    )

    if not ti:
        error_message = f"Task instance not found for task {task_id!r} on DAG run with ID {dag_run_id!r}"
        raise NotFound(detail=error_message)

    if not data["dry_run"]:
        ti = dag.set_task_instance_state(
            task_id=task_id,
            run_id=dag_run_id,
            map_indexes=[map_index],
            state=data["new_state"],
            commit=True,
            session=session,
        )

    return task_instance_reference_schema.dump(ti)


@security.requires_access_dag("PUT", DagAccessEntity.TASK_INSTANCE)
@action_logging
@provide_session
def patch_mapped_task_instance(
    *, dag_id: str, dag_run_id: str, task_id: str, map_index: int, session: Session = NEW_SESSION
) -> APIResponse:
    """Update the state of a mapped task instance."""
    return patch_task_instance(
        dag_id=dag_id, dag_run_id=dag_run_id, task_id=task_id, map_index=map_index, session=session
    )


@security.requires_access_dag("PUT", DagAccessEntity.TASK_INSTANCE)
@action_logging
@provide_session
def set_task_instance_note(
    *, dag_id: str, dag_run_id: str, task_id: str, map_index: int = -1, session: Session = NEW_SESSION
) -> APIResponse:
    """Set the note for a Task instance. This supports both Mapped and non-Mapped Task instances."""
    try:
        post_body = set_task_instance_note_form_schema.load(get_json_request_dict())
        new_note = post_body["note"]
    except ValidationError as err:
        raise BadRequest(detail=str(err))

    query = (
        select(TI)
        .where(TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id)
        .join(TI.dag_run)
        .options(joinedload(TI.rendered_task_instance_fields))
    )
    if map_index == -1:
        query = query.where(or_(TI.map_index == -1, TI.map_index is None))
    else:
        query = query.where(TI.map_index == map_index)

    try:
        ti = session.scalar(query)
    except MultipleResultsFound:
        raise NotFound(
            "Task instance not found", detail="Task instance is mapped, add the map_index value to the URL"
        )
    if ti is None:
        error_message = f"Task Instance not found for dag_id={dag_id}, run_id={dag_run_id}, task_id={task_id}"
        raise NotFound(error_message)

    current_user_id = get_auth_manager().get_user_id()
    if ti.task_instance_note is None:
        ti.note = (new_note, current_user_id)
    else:
        ti.task_instance_note.content = new_note
        ti.task_instance_note.user_id = current_user_id
    session.commit()
    return task_instance_schema.dump(ti)


@security.requires_access_dag("GET", DagAccessEntity.TASK_INSTANCE)
@provide_session
def get_task_instance_dependencies(
    *, dag_id: str, dag_run_id: str, task_id: str, map_index: int = -1, session: Session = NEW_SESSION
) -> APIResponse:
    """Get dependencies blocking task from getting scheduled."""
    from airflow.exceptions import TaskNotFound
    from airflow.ti_deps.dep_context import DepContext
    from airflow.ti_deps.dependencies_deps import SCHEDULER_QUEUED_DEPS
    from airflow.utils.airflow_flask_app import get_airflow_app

    query = select(TI).where(TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id)

    if map_index == -1:
        query = query.where(or_(TI.map_index == -1, TI.map_index is None))
    else:
        query = query.where(TI.map_index == map_index)

    try:
        result = session.execute(query).one_or_none()
    except MultipleResultsFound:
        raise NotFound(
            "Task instance not found", detail="Task instance is mapped, add the map_index value to the URL"
        )

    if result is None:
        error_message = f"Task Instance not found for dag_id={dag_id}, run_id={dag_run_id}, task_id={task_id}"
        raise NotFound(error_message)

    ti = result[0]
    deps = []

    if ti.state in [None, TaskInstanceState.SCHEDULED]:
        dag = get_airflow_app().dag_bag.get_dag(ti.dag_id)

        if dag:
            try:
                ti.task = dag.get_task(ti.task_id)
            except TaskNotFound:
                pass
            else:
                dep_context = DepContext(SCHEDULER_QUEUED_DEPS)
                deps = sorted(
                    [
                        {"name": dep.dep_name, "reason": dep.reason}
                        for dep in ti.get_failed_dep_statuses(dep_context=dep_context, session=session)
                    ],
                    key=lambda x: x["name"],
                )

    return task_dependencies_collection_schema.dump({"dependencies": deps})


def get_mapped_task_instance_dependencies(
    *, dag_id: str, dag_run_id: str, task_id: str, map_index: int
) -> APIResponse:
    """Get dependencies blocking mapped task instance from getting scheduled."""
    return get_task_instance_dependencies(
        dag_id=dag_id, dag_run_id=dag_run_id, task_id=task_id, map_index=map_index
    )


@security.requires_access_dag("GET", DagAccessEntity.TASK_INSTANCE)
@provide_session
def get_task_instance_try_details(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    task_try_number: int,
    map_index: int = -1,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get details of a task instance try."""

    def _query(orm_object):
        query = select(orm_object).where(
            orm_object.dag_id == dag_id,
            orm_object.run_id == dag_run_id,
            orm_object.task_id == task_id,
            orm_object.try_number == task_try_number,
            orm_object.map_index == map_index,
        )
        try:
            result = session.execute(query).one_or_none()
        except MultipleResultsFound:
            raise NotFound(
                "Task instance not found",
                detail="Task instance is mapped, add the map_index value to the URL",
            )
        return result

    result = _query(TI) or _query(TIH)
    if result is None:
        error_message = f"Task Instance not found for dag_id={dag_id}, run_id={dag_run_id}, task_id={task_id}, map_index={map_index}, try_number={task_try_number}."
        raise NotFound("Task instance not found", detail=error_message)
    return task_instance_history_schema.dump(result[0])


@provide_session
def get_mapped_task_instance_try_details(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    task_try_number: int,
    map_index: int,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get details of a mapped task instance try."""
    return get_task_instance_try_details(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        task_try_number=task_try_number,
        map_index=map_index,
        session=session,
    )


@security.requires_access_dag("GET", DagAccessEntity.TASK_INSTANCE)
@provide_session
def get_task_instance_tries(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    map_index: int = -1,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get list of task instances history."""

    def _query(orm_object):
        query = select(orm_object).where(
            orm_object.dag_id == dag_id,
            orm_object.run_id == dag_run_id,
            orm_object.task_id == task_id,
            orm_object.map_index == map_index,
        )
        return query

    task_instances = session.scalars(_query(TIH)).all() + session.scalars(_query(TI)).all()
    return task_instance_history_collection_schema.dump(
        TaskInstanceHistoryCollection(task_instances=task_instances, total_entries=len(task_instances))
    )


@security.requires_access_dag("GET", DagAccessEntity.TASK_INSTANCE)
@provide_session
def get_mapped_task_instance_tries(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    map_index: int,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get list of mapped task instances history."""
    return get_task_instance_tries(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        map_index=map_index,
        session=session,
    )

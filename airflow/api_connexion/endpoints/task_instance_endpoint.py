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

from typing import Any, Iterable, TypeVar

from marshmallow import ValidationError
from sqlalchemy import and_, func, or_
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.orm.query import Query
from sqlalchemy.sql import ClauseElement

from airflow.api_connexion import security
from airflow.api_connexion.endpoints.request_dict import get_json_request_dict
from airflow.api_connexion.exceptions import BadRequest, NotFound
from airflow.api_connexion.parameters import format_datetime, format_parameters
from airflow.api_connexion.schemas.task_instance_schema import (
    TaskInstanceCollection,
    TaskInstanceReferenceCollection,
    clear_task_instance_form,
    set_single_task_instance_state_form,
    set_task_instance_note_form_schema,
    set_task_instance_state_form,
    task_instance_batch_form,
    task_instance_collection_schema,
    task_instance_reference_collection_schema,
    task_instance_reference_schema,
    task_instance_schema,
)
from airflow.api_connexion.types import APIResponse
from airflow.models import SlaMiss
from airflow.models.dagrun import DagRun as DR
from airflow.models.operator import needs_expansion
from airflow.models.taskinstance import TaskInstance as TI, clear_task_instances
from airflow.security import permissions
from airflow.utils.airflow_flask_app import get_airflow_app
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import DagRunState, State

T = TypeVar("T")


@security.requires_access(
    [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
    ],
)
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
        session.query(TI)
        .filter(TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id)
        .join(TI.dag_run)
        .outerjoin(
            SlaMiss,
            and_(
                SlaMiss.dag_id == TI.dag_id,
                SlaMiss.execution_date == DR.execution_date,
                SlaMiss.task_id == TI.task_id,
            ),
        )
        .add_entity(SlaMiss)
        .options(joinedload(TI.rendered_task_instance_fields))
    )

    try:
        task_instance = query.one_or_none()
    except MultipleResultsFound:
        raise NotFound(
            "Task instance not found", detail="Task instance is mapped, add the map_index value to the URL"
        )
    if task_instance is None:
        raise NotFound("Task instance not found")
    if task_instance[0].map_index != -1:
        raise NotFound(
            "Task instance not found", detail="Task instance is mapped, add the map_index value to the URL"
        )

    return task_instance_schema.dump(task_instance)


@security.requires_access(
    [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
    ],
)
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
        session.query(TI)
        .filter(
            TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id, TI.map_index == map_index
        )
        .join(TI.dag_run)
        .outerjoin(
            SlaMiss,
            and_(
                SlaMiss.dag_id == TI.dag_id,
                SlaMiss.execution_date == DR.execution_date,
                SlaMiss.task_id == TI.task_id,
            ),
        )
        .add_entity(SlaMiss)
        .options(joinedload(TI.rendered_task_instance_fields))
    )
    task_instance = query.one_or_none()
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
    },
)
@security.requires_access(
    [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
    ],
)
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
    duration_gte: float | None = None,
    duration_lte: float | None = None,
    state: list[str] | None = None,
    pool: list[str] | None = None,
    queue: list[str] | None = None,
    limit: int | None = None,
    offset: int | None = None,
    order_by: str | None = None,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get list of task instances."""
    # Because state can be 'none'
    states = _convert_state(state)

    base_query = (
        session.query(TI)
        .filter(TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id, TI.map_index >= 0)
        .join(TI.dag_run)
    )

    # 0 can mean a mapped TI that expanded to an empty list, so it is not an automatic 404
    if base_query.with_entities(func.count("*")).scalar() == 0:
        dag = get_airflow_app().dag_bag.get_dag(dag_id)
        if not dag:
            error_message = f"DAG {dag_id} not found"
            raise NotFound(error_message)
        task = dag.get_task(task_id)
        if not task:
            error_message = f"Task id {task_id} not found"
            raise NotFound(error_message)
        if not needs_expansion(task):
            error_message = f"Task id {task_id} is not mapped"
            raise NotFound(error_message)

    # Other search criteria
    query = _apply_range_filter(
        base_query,
        key=DR.execution_date,
        value_range=(execution_date_gte, execution_date_lte),
    )
    query = _apply_range_filter(query, key=TI.start_date, value_range=(start_date_gte, start_date_lte))
    query = _apply_range_filter(query, key=TI.end_date, value_range=(end_date_gte, end_date_lte))
    query = _apply_range_filter(query, key=TI.duration, value_range=(duration_gte, duration_lte))
    query = _apply_array_filter(query, key=TI.state, values=states)
    query = _apply_array_filter(query, key=TI.pool, values=pool)
    query = _apply_array_filter(query, key=TI.queue, values=queue)

    # Count elements before joining extra columns
    total_entries = query.with_entities(func.count("*")).scalar()

    # Add SLA miss
    query = (
        query.join(
            SlaMiss,
            and_(
                SlaMiss.dag_id == TI.dag_id,
                SlaMiss.task_id == TI.task_id,
                SlaMiss.execution_date == DR.execution_date,
            ),
            isouter=True,
        )
        .add_entity(SlaMiss)
        .options(joinedload(TI.rendered_task_instance_fields))
    )

    if order_by:
        if order_by == "state":
            query = query.order_by(TI.state.asc(), TI.map_index.asc())
        elif order_by == "-state":
            query = query.order_by(TI.state.desc(), TI.map_index.asc())
        elif order_by == "-map_index":
            query = query.order_by(TI.map_index.desc())
        else:
            raise BadRequest(detail=f"Ordering with '{order_by}' is not supported")
    else:
        query = query.order_by(TI.map_index.asc())

    task_instances = query.offset(offset).limit(limit).all()
    return task_instance_collection_schema.dump(
        TaskInstanceCollection(task_instances=task_instances, total_entries=total_entries)
    )


def _convert_state(states: Iterable[str] | None) -> list[str | None] | None:
    if not states:
        return None
    return [State.NONE if s == "none" else s for s in states]


def _apply_array_filter(query: Query, key: ClauseElement, values: Iterable[Any] | None) -> Query:
    if values is not None:
        cond = ((key == v) for v in values)
        query = query.filter(or_(*cond))
    return query


def _apply_range_filter(query: Query, key: ClauseElement, value_range: tuple[T, T]) -> Query:
    gte_value, lte_value = value_range
    if gte_value is not None:
        query = query.filter(key >= gte_value)
    if lte_value is not None:
        query = query.filter(key <= lte_value)
    return query


@format_parameters(
    {
        "execution_date_gte": format_datetime,
        "execution_date_lte": format_datetime,
        "start_date_gte": format_datetime,
        "start_date_lte": format_datetime,
        "end_date_gte": format_datetime,
        "end_date_lte": format_datetime,
    },
)
@security.requires_access(
    [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
    ],
)
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
    duration_gte: float | None = None,
    duration_lte: float | None = None,
    state: list[str] | None = None,
    pool: list[str] | None = None,
    queue: list[str] | None = None,
    offset: int | None = None,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get list of task instances."""
    # Because state can be 'none'
    states = _convert_state(state)

    base_query = session.query(TI).join(TI.dag_run)

    if dag_id != "~":
        base_query = base_query.filter(TI.dag_id == dag_id)
    if dag_run_id != "~":
        base_query = base_query.filter(TI.run_id == dag_run_id)
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
    base_query = _apply_array_filter(base_query, key=TI.state, values=states)
    base_query = _apply_array_filter(base_query, key=TI.pool, values=pool)
    base_query = _apply_array_filter(base_query, key=TI.queue, values=queue)

    # Count elements before joining extra columns
    total_entries = base_query.with_entities(func.count("*")).scalar()
    # Add join
    query = (
        base_query.join(
            SlaMiss,
            and_(
                SlaMiss.dag_id == TI.dag_id,
                SlaMiss.task_id == TI.task_id,
                SlaMiss.execution_date == DR.execution_date,
            ),
            isouter=True,
        )
        .add_entity(SlaMiss)
        .options(joinedload(TI.rendered_task_instance_fields))
    )
    task_instances = query.offset(offset).limit(limit).all()
    return task_instance_collection_schema.dump(
        TaskInstanceCollection(task_instances=task_instances, total_entries=total_entries)
    )


@security.requires_access(
    [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
    ],
)
@provide_session
def get_task_instances_batch(session: Session = NEW_SESSION) -> APIResponse:
    """Get list of task instances."""
    body = get_json_request_dict()
    try:
        data = task_instance_batch_form.load(body)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))
    states = _convert_state(data["state"])
    base_query = session.query(TI).join(TI.dag_run)

    base_query = _apply_array_filter(base_query, key=TI.dag_id, values=data["dag_ids"])
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

    # Count elements before joining extra columns
    total_entries = base_query.with_entities(func.count("*")).scalar()
    # Add join
    base_query = base_query.join(
        SlaMiss,
        and_(
            SlaMiss.dag_id == TI.dag_id,
            SlaMiss.task_id == TI.task_id,
            SlaMiss.execution_date == DR.execution_date,
        ),
        isouter=True,
    ).add_entity(SlaMiss)
    ti_query = base_query.options(joinedload(TI.rendered_task_instance_fields))
    task_instances = ti_query.all()

    return task_instance_collection_schema.dump(
        TaskInstanceCollection(task_instances=task_instances, total_entries=total_entries)
    )


@security.requires_access(
    [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
    ],
)
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
        dag_run: DR | None = (
            session.query(DR).filter(DR.dag_id == dag_id, DR.run_id == dag_run_id).one_or_none()
        )
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
            task_instances.all(),
            session,
            dag=dag,
            dag_run_state=DagRunState.QUEUED if reset_dag_runs else False,
        )

    return task_instance_reference_collection_schema.dump(
        TaskInstanceReferenceCollection(task_instances=task_instances.all())
    )


@security.requires_access(
    [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
    ],
)
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
            session.query(TI)
            .filter(TI.task_id == task_id, TI.dag_id == dag_id, TI.execution_date == execution_date)
            .one_or_none()
        )
        is None
    ):
        raise NotFound(
            detail=f"Task instance not found for task {task_id!r} on execution_date {execution_date}"
        )

    if run_id and not session.query(TI).get(
        {"task_id": task_id, "dag_id": dag_id, "run_id": run_id, "map_index": -1}
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


@security.requires_access(
    [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
    ],
)
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

    ti: TI | None = session.query(TI).get(
        {"task_id": task_id, "dag_id": dag_id, "run_id": dag_run_id, "map_index": map_index}
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


@security.requires_access(
    [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
    ],
)
@provide_session
def patch_mapped_task_instance(
    *, dag_id: str, dag_run_id: str, task_id: str, map_index: int, session: Session = NEW_SESSION
) -> APIResponse:
    """Update the state of a mapped task instance."""
    return patch_task_instance(
        dag_id=dag_id, dag_run_id=dag_run_id, task_id=task_id, map_index=map_index, session=session
    )


@security.requires_access(
    [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
    ],
)
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
        session.query(TI)
        .filter(TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id)
        .join(TI.dag_run)
        .outerjoin(
            SlaMiss,
            and_(
                SlaMiss.dag_id == TI.dag_id,
                SlaMiss.execution_date == DR.execution_date,
                SlaMiss.task_id == TI.task_id,
            ),
        )
        .add_entity(SlaMiss)
        .options(joinedload(TI.rendered_task_instance_fields))
    )
    if map_index == -1:
        query = query.filter(or_(TI.map_index == -1, TI.map_index is None))
    else:
        query = query.filter(TI.map_index == map_index)

    try:
        result = query.one_or_none()
    except MultipleResultsFound:
        raise NotFound(
            "Task instance not found", detail="Task instance is mapped, add the map_index value to the URL"
        )
    if result is None:
        error_message = f"Task Instance not found for dag_id={dag_id}, run_id={dag_run_id}, task_id={task_id}"
        raise NotFound(error_message)

    ti, sla_miss = result
    from flask_login import current_user

    current_user_id = getattr(current_user, "id", None)
    if ti.task_instance_note is None:
        ti.note = (new_note, current_user_id)
    else:
        ti.task_instance_note.content = new_note
        ti.task_instance_note.user_id = current_user_id
    session.commit()
    return task_instance_schema.dump((ti, sla_miss))

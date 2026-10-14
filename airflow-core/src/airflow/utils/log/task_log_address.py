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

from collections.abc import Collection, Mapping
from functools import cache
from pathlib import PurePosixPath
from string import Formatter
from typing import TYPE_CHECKING
from uuid import UUID

import attrs
import jinja2
from jinja2.meta import find_undeclared_variables
from sqlalchemy import inspect, select, tuple_
from sqlalchemy.orm.attributes import NO_VALUE

from airflow.models.dagbag import DBDagBag
from airflow.models.dagrun import DagRun
from airflow.models.dynamic_region import SENTINEL_REGION_ID, DynamicRegion
from airflow.models.tasklog import LogTemplate
from airflow.serialization.definitions.taskgroup import SerializedLoopTaskGroup
from airflow.utils.helpers import render_template
from airflow.utils.session import create_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.taskinstance import TaskInstance


@attrs.define(frozen=True)
class TaskLogContext:
    """Prepared, immutable inputs for query-free task log filename rendering."""

    filename_template: str
    logical_date: str
    data_interval_start: str
    data_interval_end: str
    log_position: str = ""
    map_index: int = -1


def region_log_position(
    region_id: UUID,
    region_index: int,
    *,
    regions: Mapping[UUID, DynamicRegion],
    node_kinds: Mapping[str, str],
    task_id: str | None = None,
) -> str:
    """
    Render the loop nesting and fork position of one coordinate, empty outside any loop.

    A node the pinned definition no longer lists is classified from the stored region: the task's own
    region is its mapped expansion and any other region encloses it as a loop.
    """
    frames = []
    inside_loop = False
    while region_id != SENTINEL_REGION_ID:
        region = regions[region_id]
        kind = node_kinds.get(region.node_id) or ("map" if region.node_id == task_id else "loop")
        if kind not in {"loop", "map"}:
            raise ValueError(f"Unsupported log-address region kind: {kind}")
        position = 1
        predecessor = region
        while predecessor.forked_from_region_id is not None:
            predecessor = regions[predecessor.forked_from_region_id]
            position += 1
        suffix = f".{position}" if position > 1 else ""
        label = "pass" if kind == "loop" else "map"
        inside_loop = inside_loop or kind == "loop"
        frames.append(f"{label}={region_index}{suffix}")
        if region.parent_region_id is None:
            break
        if TYPE_CHECKING:
            assert region.parent_region_index is not None
        region_id, region_index = region.parent_region_id, region.parent_region_index
    return "/".join(reversed(frames)) if inside_loop else ""


class _LogTaskView:
    def __init__(self, ti, map_index: int):
        self._ti = ti
        self.map_index = map_index

    def __getattr__(self, name: str) -> object:
        return getattr(self._ti, name)


@cache
def _compile_template(template: str) -> tuple[jinja2.Template | str, bool]:
    if "{{" in template:
        env = jinja2.Environment(autoescape=False)  # nosec B701: Render a filesystem path, not HTML.
        return env.from_string(template), "log_position" in find_undeclared_variables(env.parse(template))
    fields = {field for _, field, _, _ in Formatter().parse(template)}
    return template, "log_position" in fields


def render_task_log_filename(ti, try_number: int, *, context: TaskLogContext) -> str:
    """Render a pinned template and apply its regional address exactly once."""
    template, consumes_position = _compile_template(context.filename_template)
    task_view = _LogTaskView(ti, context.map_index)
    if isinstance(template, jinja2.Template):
        filename = render_template(
            template,
            {
                "ti": task_view,
                "ts": context.logical_date,
                "try_number": try_number,
                "log_position": context.log_position,
            },
            native=False,
        )
    else:
        filename = template.format(
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            run_id=ti.run_id,
            data_interval_start=context.data_interval_start,
            data_interval_end=context.data_interval_end,
            logical_date=context.logical_date,
            try_number=try_number,
            log_position=context.log_position,
        )
    if context.log_position and not consumes_position:
        path = PurePosixPath(filename)
        return str(path.parent / context.log_position / path.name)
    return filename


def _load_node_kinds(
    tis: Collection[TaskInstance],
    runs: Mapping[tuple[str, str], DagRun],
    session: Session,
    dag_bag: DBDagBag | None,
) -> dict[UUID, dict[str, str]]:
    """Classify the loop groups and mapped tasks of each pinned Dag version, skipping versions that are gone."""
    version_ids = {
        version_id
        for ti in tis
        if (version_id := ti.dag_version_id or runs[ti.dag_id, ti.run_id].created_dag_version_id)
    }
    attached = {
        dag.dag_version_id: dag
        for ti in tis
        if (dag := getattr(ti.task, "dag", None)) is not None and dag.dag_version_id is not None
    }
    if dag_bag is None:
        dag_bag = DBDagBag(load_op_links=False)
    kinds: dict[UUID, dict[str, str]] = {}
    for version_id in version_ids:
        dag = attached.get(version_id) or dag_bag.get_dag(version_id, session=session)
        if dag is None:
            continue
        kinds[version_id] = {
            group_id: "loop"
            for group_id, group in dag.task_group.get_task_group_dict().items()
            if group_id is not None and isinstance(group, SerializedLoopTaskGroup)
        } | {task.task_id: "map" for task in dag.tasks if task.get_needs_expansion()}
    return kinds


def prepare_task_log_contexts(
    tis: Collection[TaskInstance],
    *,
    session: Session | None = None,
    dag_bag: DBDagBag | None = None,
    filename_template: str | None = None,
) -> dict[UUID, TaskLogContext]:
    """
    Load shared address inputs in batches before rendering workloads or reading logs.

    Pass the caller's ``dag_bag`` so the loop and map nodes of a Dag version come from its cache instead of
    deserializing the Dag again. Writers pass the configured ``filename_template``; readers omit it to use
    each run's pinned template.
    """
    if not tis:
        return {}
    if session is None:
        with create_session(scoped=False) as session:
            return prepare_task_log_contexts(
                tis, session=session, dag_bag=dag_bag, filename_template=filename_template
            )
    runs: dict[tuple[str, str], DagRun] = {}
    for ti in tis:
        loaded_run: DagRun | None = inspect(ti).attrs.dag_run.loaded_value
        if loaded_run is not NO_VALUE and loaded_run is not None:
            runs[ti.dag_id, ti.run_id] = loaded_run
    missing_run_keys = {(ti.dag_id, ti.run_id) for ti in tis} - runs.keys()
    if missing_run_keys:
        runs.update(
            ((run.dag_id, run.run_id), run)
            for run in session.scalars(
                select(DagRun).where(tuple_(DagRun.dag_id, DagRun.run_id).in_(missing_run_keys))
            )
        )
    templates: dict[int | None, str | None] = {}
    if filename_template is None:
        template_ids = {run.log_template_id for run in runs.values() if run.log_template_id is not None}
        templates = {
            template.id: template.filename
            for template in session.scalars(select(LogTemplate).where(LogTemplate.id.in_(template_ids)))
        }
        if any(run.log_template_id is None for run in runs.values()):
            templates[None] = session.scalar(select(LogTemplate.filename).order_by(LogTemplate.id).limit(1))
    regional = [ti for ti in tis if ti.region_id != SENTINEL_REGION_ID]
    regions: dict[UUID, DynamicRegion] = {}
    node_kinds: dict[UUID, dict[str, str]] = {}
    if regional:
        frontier = {ti.region_id for ti in regional}
        while frontier:
            rows = session.scalars(select(DynamicRegion).where(DynamicRegion.id.in_(frontier))).all()
            regions.update((row.id, row) for row in rows)
            frontier = {
                ancestor_id
                for row in rows
                for ancestor_id in (row.parent_region_id, row.forked_from_region_id)
                if ancestor_id is not None and ancestor_id not in regions
            }
        # A task's own top-level region is already known from its stored row to be a mapped expansion
        # or a loop; only a region nested inside another needs the pinned definition to be told apart.
        nested = [ti for ti in regional if ti.region_id in regions and regions[ti.region_id].parent_region_id]
        node_kinds = _load_node_kinds(nested, runs, session, dag_bag)
    contexts = {}
    for ti in tis:
        run = runs[ti.dag_id, ti.run_id]
        template = filename_template if filename_template is not None else templates.get(run.log_template_id)
        if template is None:
            raise ValueError(
                f"No log_template entry found for ID {run.log_template_id!r}. "
                "Please make sure you set up the metadatabase correctly."
            )
        position = ""
        map_index = ti.region_index
        if ti.region_id != SENTINEL_REGION_ID:
            version_id = ti.dag_version_id or run.created_dag_version_id
            position = region_log_position(
                ti.region_id,
                ti.region_index,
                regions=regions,
                node_kinds=node_kinds.get(version_id, {}) if version_id else {},
                task_id=ti.task_id,
            )
            map_index = ti.region_index if regions[ti.region_id].node_id == ti.task_id else -1
        contexts[ti.id] = TaskLogContext(
            template,
            (run.logical_date or run.run_after).isoformat(),
            run.data_interval_start.isoformat() if run.data_interval_start else "",
            run.data_interval_end.isoformat() if run.data_interval_end else "",
            position,
            map_index,
        )
    return contexts

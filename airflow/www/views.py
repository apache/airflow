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

import collections.abc
import contextlib
import copy
import datetime
import itertools
import json
import logging
import math
import operator
import os
import sys
import traceback
import warnings
from bisect import insort_left
from collections import defaultdict
from functools import cached_property
from json import JSONDecodeError
from pathlib import Path
from typing import TYPE_CHECKING, Any, Collection, Iterator, Mapping, MutableMapping, Sequence
from urllib.parse import unquote, urlencode, urljoin, urlparse, urlsplit

import configupdater
import flask.json
import lazy_object_proxy
import re2
import sqlalchemy as sqla
from croniter import croniter
from flask import (
    Response,
    abort,
    before_render_template,
    current_app,
    flash,
    g,
    has_request_context,
    make_response,
    redirect,
    render_template,
    request,
    send_from_directory,
    session as flask_session,
    url_for,
)
from flask_appbuilder import BaseView, ModelView, expose
from flask_appbuilder._compat import as_unicode
from flask_appbuilder.actions import action
from flask_appbuilder.const import FLAMSG_ERR_SEC_ACCESS_DENIED
from flask_appbuilder.models.sqla.filters import BaseFilter
from flask_appbuilder.urltools import get_order_args, get_page_args, get_page_size_args
from flask_appbuilder.widgets import FormWidget
from flask_babel import lazy_gettext
from itsdangerous import URLSafeSerializer
from jinja2.utils import htmlsafe_json_dumps, pformat  # type: ignore
from markupsafe import Markup, escape
from pendulum.datetime import DateTime
from pendulum.parsing.exceptions import ParserError
from sqlalchemy import and_, case, desc, func, inspect, or_, select, union_all
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload
from wtforms import BooleanField, validators

import airflow
from airflow import models, plugins_manager, settings
from airflow.api.common.airflow_health import get_airflow_health
from airflow.api.common.mark_tasks import (
    set_dag_run_state_to_failed,
    set_dag_run_state_to_queued,
    set_dag_run_state_to_success,
    set_state,
)
from airflow.auth.managers.models.resource_details import AccessView, DagAccessEntity, DagDetails
from airflow.compat.functools import cache
from airflow.configuration import AIRFLOW_CONFIG, conf
from airflow.datasets import Dataset, DatasetAlias
from airflow.exceptions import (
    AirflowConfigException,
    AirflowException,
    AirflowNotFoundException,
    ParamValidationError,
    RemovedInAirflow3Warning,
)
from airflow.executors.executor_loader import ExecutorLoader
from airflow.hooks.base import BaseHook
from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.jobs.triggerer_job_runner import TriggererJobRunner
from airflow.models import Connection, DagModel, DagTag, Log, SlaMiss, Trigger, XCom
from airflow.models.dag import get_dataset_triggered_next_run_info
from airflow.models.dagrun import RUN_ID_REGEX, DagRun, DagRunType
from airflow.models.dataset import DagScheduleDatasetReference, DatasetDagRunQueue, DatasetEvent, DatasetModel
from airflow.models.errors import ParseImportError
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance, TaskInstanceNote
from airflow.plugins_manager import PLUGINS_ATTRIBUTES_TO_DUMP
from airflow.providers_manager import ProvidersManager
from airflow.security import permissions
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_deps import SCHEDULER_QUEUED_DEPS
from airflow.timetables._cron import CronMixin
from airflow.timetables.base import DataInterval, TimeRestriction
from airflow.timetables.simple import ContinuousTimetable
from airflow.utils import json as utils_json, timezone, yaml
from airflow.utils.airflow_flask_app import get_airflow_app
from airflow.utils.dag_edges import dag_edges
from airflow.utils.db import get_query_count
from airflow.utils.docs import get_doc_url_for_provider, get_docs_url
from airflow.utils.helpers import exactly_one
from airflow.utils.log import secrets_masker
from airflow.utils.log.log_reader import TaskLogReader
from airflow.utils.net import get_hostname
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.utils.strings import to_boolean
from airflow.utils.task_group import TaskGroup, task_group_to_dict
from airflow.utils.timezone import td_format, utcnow
from airflow.utils.types import NOTSET
from airflow.version import version
from airflow.www import auth, utils as wwwutils
from airflow.www.decorators import action_logging, gzipped
from airflow.www.extensions.init_auth_manager import get_auth_manager, is_auth_manager_initialized
from airflow.www.forms import (
    DagRunEditForm,
    DateTimeForm,
    TaskInstanceEditForm,
    create_connection_form_class,
)
from airflow.www.widgets import AirflowModelListWidget, AirflowVariableShowWidget

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.auth.managers.models.batch_apis import IsAuthorizedDagRequest
    from airflow.models.dag import DAG
    from airflow.models.operator import Operator

PAGE_SIZE = conf.getint("webserver", "page_size")
FILTER_TAGS_COOKIE = "tags_filter"
FILTER_STATUS_COOKIE = "dag_status_filter"
FILTER_LASTRUN_COOKIE = "last_run_filter"
LINECHART_X_AXIS_TICKFORMAT = (
    "function (d, i) { let xLabel;"
    "if (i === undefined) {xLabel = d3.time.format('%H:%M, %d %b %Y')(new Date(parseInt(d)));"
    "} else {xLabel = d3.time.format('%H:%M, %d %b')(new Date(parseInt(d)));} return xLabel;}"
)

SENSITIVE_FIELD_PLACEHOLDER = "RATHER_LONG_SENSITIVE_FIELD_PLACEHOLDER"

logger = logging.getLogger(__name__)


def sanitize_args(args: dict[str, Any]) -> dict[str, Any]:
    """
    Remove all parameters starting with `_`.

    :param args: arguments of request
    :return: copy of the dictionary passed as input with args starting with `_` removed.
    """
    return {key: value for key, value in args.items() if not key.startswith("_")}


# Following the release of https://github.com/python/cpython/issues/102153 in Python 3.8.17 and 3.9.17 on
# June 6, 2023, we are adding extra sanitization of the urls passed to get_safe_url method to make it works
# the same way regardless if the user uses latest Python patchlevel versions or not. This also follows
# a recommended solution by the Python core team.
#
# From: https://github.com/python/cpython/commit/d28bafa2d3e424b6fdcfd7ae7cde8e71d7177369
#
#   We recommend that users of these APIs where the values may be used anywhere
#   with security implications code defensively. Do some verification within your
#   code before trusting a returned component part.  Does that ``scheme`` make
#   sense?  Is that a sensible ``path``?  Is there anything strange about that
#   ``hostname``?  etc.
#
# C0 control and space to be stripped per WHATWG spec.
# == "".join([chr(i) for i in range(0, 0x20 + 1)])
_WHATWG_C0_CONTROL_OR_SPACE = (
    "\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c"
    "\r\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f "
)


def get_safe_url(url):
    """Given a user-supplied URL, ensure it points to our web server."""
    if not url:
        return url_for("Airflow.index")

    # If the url contains semicolon, redirect it to homepage to avoid
    # potential XSS. (Similar to https://github.com/python/cpython/pull/24297/files (bpo-42967))
    if ";" in unquote(url):
        return url_for("Airflow.index")

    url = url.lstrip(_WHATWG_C0_CONTROL_OR_SPACE)

    host_url = urlsplit(request.host_url)
    redirect_url = urlsplit(urljoin(request.host_url, url))
    if not (redirect_url.scheme in ("http", "https") and host_url.netloc == redirect_url.netloc):
        return url_for("Airflow.index")

    # This will ensure we only redirect to the right scheme/netloc
    return redirect_url.geturl()


def get_date_time_num_runs_dag_runs_form_data(www_request, session, dag):
    """Get Execution Data, Base Date & Number of runs from a Request."""
    date_time = www_request.args.get("execution_date")
    run_id = www_request.args.get("run_id")
    # First check run id, then check execution date, if not fall back on the latest dagrun
    if run_id:
        dagrun = dag.get_dagrun(run_id=run_id, session=session)
        date_time = dagrun.execution_date
    elif date_time:
        date_time = _safe_parse_datetime(date_time)
    else:
        date_time = dag.get_latest_execution_date(session=session) or timezone.utcnow()

    base_date = www_request.args.get("base_date")
    if base_date:
        base_date = _safe_parse_datetime(base_date)
    else:
        # The DateTimeField widget truncates milliseconds and would loose
        # the first dag run. Round to next second.
        base_date = (date_time + datetime.timedelta(seconds=1)).replace(microsecond=0)

    default_dag_run = conf.getint("webserver", "default_dag_run_display_number")
    num_runs = www_request.args.get("num_runs", default=default_dag_run, type=int)

    # When base_date has been rounded up because of the DateTimeField widget, we want
    # to use the execution_date as the starting point for our query just to ensure a
    # link targeting a specific dag run actually loads that dag run.  If there are
    # more than num_runs dag runs in the "rounded period" then those dagruns would get
    # loaded and the actual requested run would be excluded by the limit().  Once
    # the user has changed base date to be anything else we want to use that instead.
    query_date = base_date
    if date_time < base_date <= date_time + datetime.timedelta(seconds=1):
        query_date = date_time

    drs = session.scalars(
        select(DagRun)
        .where(DagRun.dag_id == dag.dag_id, DagRun.execution_date <= query_date)
        .order_by(desc(DagRun.execution_date))
        .limit(num_runs)
    ).all()
    dr_choices = []
    dr_state = None
    for dr in drs:
        dr_choices.append((dr.execution_date.isoformat(), dr.run_id))
        if date_time == dr.execution_date:
            dr_state = dr.state

    # Happens if base_date was changed and the selected dag run is not in result
    if not dr_state and drs:
        dr = drs[0]
        date_time = dr.execution_date
        dr_state = dr.state

    return {
        "dttm": date_time,
        "base_date": base_date,
        "num_runs": num_runs,
        "execution_date": date_time.isoformat(),
        "dr_choices": dr_choices,
        "dr_state": dr_state,
    }


def _safe_parse_datetime(v, *, allow_empty=False, strict=True) -> datetime.datetime | None:
    """
    Parse datetime and return error message for invalid dates.

    :param v: the string value to be parsed
    :param allow_empty: Set True to return none if empty str or None
    :param strict: if False, it will fall back on the dateutil parser if unable to parse with pendulum
    """
    if allow_empty is True and not v:
        return None
    try:
        return timezone.parse(v, strict=strict)
    except (TypeError, ParserError):
        abort(400, f"Invalid datetime: {v!r}")


def node_dict(node_id, label, node_class):
    return {
        "id": node_id,
        "value": {"label": label, "rx": 5, "ry": 5, "class": node_class},
    }


def dag_to_grid(dag: DagModel, dag_runs: Sequence[DagRun], session: Session) -> dict[str, Any]:
    """
    Create a nested dict representation of the DAG's TaskGroup and its children.

    Used to construct the Graph and Grid views.
    """
    query = session.execute(
        select(
            TaskInstance.task_id,
            TaskInstance.run_id,
            TaskInstance.state,
            case(
                (TaskInstance.map_index == -1, TaskInstance.try_number),
                else_=None,
            ).label("try_number"),
            func.min(TaskInstanceNote.content).label("note"),
            func.count(func.coalesce(TaskInstance.state, sqla.literal("no_status"))).label("state_count"),
            func.min(TaskInstance.queued_dttm).label("queued_dttm"),
            func.min(TaskInstance.start_date).label("start_date"),
            func.max(TaskInstance.end_date).label("end_date"),
        )
        .join(TaskInstance.task_instance_note, isouter=True)
        .where(
            TaskInstance.dag_id == dag.dag_id,
            TaskInstance.run_id.in_([dag_run.run_id for dag_run in dag_runs]),
        )
        .group_by(
            TaskInstance.task_id,
            TaskInstance.run_id,
            TaskInstance.state,
            case(
                (TaskInstance.map_index == -1, TaskInstance.try_number),
                else_=None,
            ),
        )
        .order_by(TaskInstance.task_id, TaskInstance.run_id)
    )

    grouped_tis: dict[str, list[TaskInstance]] = collections.defaultdict(
        list,
        ((task_id, list(tis)) for task_id, tis in itertools.groupby(query, key=lambda ti: ti.task_id)),
    )

    @cache
    def get_task_group_children_getter() -> operator.methodcaller:
        sort_order = conf.get("webserver", "grid_view_sorting_order", fallback="topological")
        if sort_order == "topological":
            return operator.methodcaller("topological_sort")
        if sort_order == "hierarchical_alphabetical":
            return operator.methodcaller("hierarchical_alphabetical_sort")
        raise AirflowConfigException(f"Unsupported grid_view_sorting_order: {sort_order}")

    def task_group_to_grid(item: Operator | TaskGroup) -> dict[str, Any]:
        if not isinstance(item, TaskGroup):

            def _mapped_summary(ti_summaries: list[TaskInstance]) -> Iterator[dict[str, Any]]:
                run_id = ""
                record: dict[str, Any] = {}

                def set_overall_state(record):
                    for state in wwwutils.priority:
                        if state in record["mapped_states"]:
                            record["state"] = state
                            break
                    # When turning the dict into JSON we can't have None as a key,
                    # so use the string that the UI does.
                    with contextlib.suppress(KeyError):
                        record["mapped_states"]["no_status"] = record["mapped_states"].pop(None)

                for ti_summary in ti_summaries:
                    if run_id != ti_summary.run_id:
                        run_id = ti_summary.run_id
                        if record:
                            set_overall_state(record)
                            yield record
                        record = {
                            "task_id": ti_summary.task_id,
                            "run_id": run_id,
                            "queued_dttm": ti_summary.queued_dttm,
                            "start_date": ti_summary.start_date,
                            "end_date": ti_summary.end_date,
                            "mapped_states": {ti_summary.state: ti_summary.state_count},
                            "state": None,  # We change this before yielding
                        }
                        continue
                    record["queued_dttm"] = min(
                        filter(None, [record["queued_dttm"], ti_summary.queued_dttm]), default=None
                    )
                    record["start_date"] = min(
                        filter(None, [record["start_date"], ti_summary.start_date]), default=None
                    )
                    # Sometimes the start date of a group might be before the queued date of the group
                    if (
                        record["queued_dttm"]
                        and record["start_date"]
                        and record["queued_dttm"] > record["start_date"]
                    ):
                        record["queued_dttm"] = None
                    record["end_date"] = max(
                        filter(None, [record["end_date"], ti_summary.end_date]), default=None
                    )
                    record["mapped_states"][ti_summary.state] = ti_summary.state_count
                if record:
                    set_overall_state(record)
                    yield record

            if item_is_mapped := item.get_needs_expansion():
                instances = list(_mapped_summary(grouped_tis[item.task_id]))
            else:
                instances = [
                    {
                        "task_id": task_instance.task_id,
                        "run_id": task_instance.run_id,
                        "state": task_instance.state,
                        "queued_dttm": task_instance.queued_dttm,
                        "start_date": task_instance.start_date,
                        "end_date": task_instance.end_date,
                        "try_number": task_instance.try_number,
                        "note": task_instance.note,
                    }
                    for task_instance in grouped_tis[item.task_id]
                ]

            setup_teardown_type = {}
            if item.is_setup is True:
                setup_teardown_type["setupTeardownType"] = "setup"
            elif item.is_teardown is True:
                setup_teardown_type["setupTeardownType"] = "teardown"

            return {
                "id": item.task_id,
                "instances": instances,
                "label": item.label,
                "extra_links": item.extra_links,
                "is_mapped": item_is_mapped,
                "has_outlet_datasets": any(
                    isinstance(i, (Dataset, DatasetAlias)) for i in (item.outlets or [])
                ),
                "operator": item.operator_name,
                "trigger_rule": item.trigger_rule,
                **setup_teardown_type,
            }

        # Task Group
        task_group = item
        children = [task_group_to_grid(child) for child in get_task_group_children_getter()(item)]

        def get_summary(dag_run: DagRun):
            child_instances = [
                item
                for sublist in (child["instances"] for child in children if "instances" in child)
                for item in sublist
                if item["run_id"] == dag_run.run_id
                if item
            ]

            children_queued_dttms = (item["queued_dttm"] for item in child_instances)
            children_start_dates = (item["start_date"] for item in child_instances)
            children_end_dates = (item["end_date"] for item in child_instances)
            children_states = {item["state"] for item in child_instances}

            group_state = next((state for state in wwwutils.priority if state in children_states), None)
            group_queued_dttm = min(filter(None, children_queued_dttms), default=None)
            group_start_date = min(filter(None, children_start_dates), default=None)
            group_end_date = max(filter(None, children_end_dates), default=None)
            # Sometimes the start date of a group might be before the queued date of the group
            if group_queued_dttm and group_start_date and group_queued_dttm > group_start_date:
                group_queued_dttm = None

            return {
                "task_id": task_group.group_id,
                "run_id": dag_run.run_id,
                "state": group_state,
                "queued_dttm": group_queued_dttm,
                "start_date": group_start_date,
                "end_date": group_end_date,
            }

        def get_mapped_group_summaries():
            mapped_ti_query = session.execute(
                select(TaskInstance.task_id, TaskInstance.state, TaskInstance.run_id, TaskInstance.map_index)
                .where(
                    TaskInstance.dag_id == dag.dag_id,
                    TaskInstance.task_id.in_(child["id"] for child in children),
                    TaskInstance.run_id.in_(r.run_id for r in dag_runs),
                )
                .order_by(TaskInstance.task_id, TaskInstance.run_id)
            )
            # Group tis by run_id, and then map_index.
            mapped_tis: Mapping[str, Mapping[int, list[TaskInstance]]] = defaultdict(
                lambda: defaultdict(list)
            )
            for ti in mapped_ti_query:
                mapped_tis[ti.run_id][ti.map_index].append(ti)

            def get_mapped_group_summary(run_id: str, mapped_instances: Mapping[int, list[TaskInstance]]):
                child_instances = [
                    item
                    for sublist in (child["instances"] for child in children if "instances" in child)
                    for item in sublist
                    if item and item["run_id"] == run_id
                ]

                children_queued_dttms = (item["queued_dttm"] for item in child_instances)
                children_start_dates = (item["start_date"] for item in child_instances)
                children_end_dates = (item["end_date"] for item in child_instances)
                children_states = {item["state"] for item in child_instances}

                # TODO: This assumes TI map index has a one-to-one mapping to
                # its parent mapped task group, which will not be true when we
                # allow nested mapping in the future.
                mapped_states: MutableMapping[str, int] = defaultdict(int)
                for mi_values in mapped_instances.values():
                    child_states = {mi.state for mi in mi_values}
                    state = next(s for s in wwwutils.priority if s in child_states)
                    value = state.value if state is not None else "no_status"
                    mapped_states[value] += 1

                group_state = next((state for state in wwwutils.priority if state in children_states), None)
                group_queued_dttm = min(filter(None, children_queued_dttms), default=None)
                group_start_date = min(filter(None, children_start_dates), default=None)
                group_end_date = max(filter(None, children_end_dates), default=None)

                return {
                    "task_id": task_group.group_id,
                    "run_id": run_id,
                    "state": group_state,
                    "queued_dttm": group_queued_dttm,
                    "start_date": group_start_date,
                    "end_date": group_end_date,
                    "mapped_states": mapped_states,
                }

            return [get_mapped_group_summary(run_id, tis) for run_id, tis in mapped_tis.items()]

        # We don't need to calculate summaries for the root
        if task_group.group_id is None:
            return {
                "id": task_group.group_id,
                "label": task_group.label,
                "children": children,
                "instances": [],
            }

        if next(task_group.iter_mapped_task_groups(), None) is not None:
            return {
                "id": task_group.group_id,
                "label": task_group.label,
                "children": children,
                "tooltip": task_group.tooltip,
                "instances": get_mapped_group_summaries(),
                "is_mapped": True,
            }

        group_summaries = [get_summary(dr) for dr in dag_runs]

        return {
            "id": task_group.group_id,
            "label": task_group.label,
            "children": children,
            "tooltip": task_group.tooltip,
            "instances": group_summaries,
        }

    return task_group_to_grid(dag.task_group)


def get_key_paths(input_dict):
    """Return a list of dot-separated dictionary paths."""
    for key, value in input_dict.items():
        if isinstance(value, dict):
            for sub_key in get_key_paths(value):
                yield f"{key}.{sub_key}"
        else:
            yield key


def get_value_from_path(key_path, content):
    """Return the value from a dictionary based on dot-separated path of keys."""
    elem = content
    for x in key_path.strip(".").split("."):
        try:
            x = int(x)
            elem = elem[x]
        except ValueError:
            elem = elem.get(x)

    return elem


def get_task_stats_from_query(qry):
    """
    Return a dict of the task quantity, grouped by dag id and task status.

    :param qry: The data in the format (<dag id>, <task state>, <is dag running>, <task count>),
        ordered by <dag id> and <is dag running>
    """
    data = {}
    last_dag_id = None
    has_running_dags = False
    for dag_id, state, is_dag_running, count in qry:
        if last_dag_id != dag_id:
            last_dag_id = dag_id
            has_running_dags = False
        elif not is_dag_running and has_running_dags:
            continue

        if is_dag_running:
            has_running_dags = True
        if dag_id not in data:
            data[dag_id] = {}
        data[dag_id][state] = count
    return data


def redirect_or_json(origin, msg, status="", status_code=200):
    """
    Return json which allows us to more elegantly handle side effects in-page.

    This is useful because some endpoints are called by javascript.
    """
    if request.headers.get("Accept") == "application/json":
        if status == "error" and status_code == 200:
            status_code = 500
        return Response(response=msg, status=status_code, mimetype="application/json")
    else:
        if status:
            flash(msg, status)
        else:
            flash(msg)
        return redirect(origin)


######################################################################################
#                                    Error handlers
######################################################################################


def not_found(error):
    """Show Not Found on screen for any error in the Webserver."""
    return (
        render_template(
            "airflow/error.html",
            hostname=get_hostname() if conf.getboolean("webserver", "EXPOSE_HOSTNAME") else "",
            status_code=404,
            error_message="Page cannot be found.",
        ),
        404,
    )


def method_not_allowed(error):
    """Show Method Not Allowed on screen for any error in the Webserver."""
    return (
        render_template(
            "airflow/error.html",
            hostname=get_hostname() if conf.getboolean("webserver", "EXPOSE_HOSTNAME") else "",
            status_code=405,
            error_message="Received an invalid request.",
        ),
        405,
    )


def show_traceback(error):
    """Show Traceback for a given error."""
    if not is_auth_manager_initialized():
        # this is the case where internal API component is used and auth manager is not used/initialized
        return ("Error calling the API", 500)
    is_logged_in = get_auth_manager().is_logged_in()
    return (
        render_template(
            "airflow/traceback.html",
            python_version=sys.version.split(" ")[0] if is_logged_in else "redacted",
            airflow_version=version if is_logged_in else "redacted",
            hostname=(
                get_hostname()
                if conf.getboolean("webserver", "EXPOSE_HOSTNAME") and is_logged_in
                else "redacted"
            ),
            info=(
                traceback.format_exc()
                if conf.getboolean("webserver", "EXPOSE_STACKTRACE") and is_logged_in
                else "Error! Please contact server admin."
            ),
        ),
        500,
    )


######################################################################################
#                                    BaseViews
######################################################################################


class AirflowBaseView(BaseView):
    """Base View to set Airflow related properties."""

    from airflow import macros

    route_base = ""

    extra_args = {
        # Make our macros available to our UI templates too.
        "macros": macros,
        "get_docs_url": get_docs_url,
    }

    if not conf.getboolean("core", "unit_test_mode"):
        executor, _ = ExecutorLoader.import_default_executor_cls()
        extra_args["sqlite_warning"] = settings.engine and (settings.engine.dialect.name == "sqlite")
        if not executor.is_production:
            extra_args["production_executor_warning"] = executor.__name__
        extra_args["otel_metrics_on"] = conf.getboolean("metrics", "otel_on")
        extra_args["otel_traces_on"] = conf.getboolean("traces", "otel_on")

    line_chart_attr = {
        "legend.maxKeyLength": 200,
    }

    def render_template(self, *args, **kwargs):
        # Add triggerer_job only if we need it
        if TriggererJobRunner.is_needed():
            kwargs["triggerer_job"] = lazy_object_proxy.Proxy(TriggererJobRunner.most_recent_job)

        if "dag" in kwargs:
            kwargs["can_edit_dag"] = get_auth_manager().is_authorized_dag(
                method="PUT", details=DagDetails(id=kwargs["dag"].dag_id)
            )
            url_serializer = URLSafeSerializer(current_app.config["SECRET_KEY"])
            kwargs["dag_file_token"] = url_serializer.dumps(kwargs["dag"].fileloc)

        return super().render_template(
            *args,
            # Cache this at most once per request, not for the lifetime of the view instance
            scheduler_job=lazy_object_proxy.Proxy(SchedulerJobRunner.most_recent_job),
            **kwargs,
        )


class Airflow(AirflowBaseView):
    """Main Airflow application."""

    @expose("/health")
    def health(self):
        """
        Check the health status of the Airflow instance.

        Includes metadatabase, scheduler and triggerer.
        """
        airflow_health_status = get_airflow_health()

        return flask.json.jsonify(airflow_health_status)

    @expose("/home")
    @auth.has_access_view()
    def index(self):
        """Home view."""
        from airflow.models.dag import DagOwnerAttributes

        hide_paused_dags_by_default = conf.getboolean("webserver", "hide_paused_dags_by_default")
        default_dag_run = conf.getint("webserver", "default_dag_run_display_number")

        num_runs = request.args.get("num_runs", default=default_dag_run, type=int)
        current_page = request.args.get("page", default=0, type=int)
        arg_search_query = request.args.get("search")
        arg_tags_filter = request.args.getlist("tags")
        arg_status_filter = request.args.get("status")
        arg_lastrun_filter = request.args.get("lastrun")
        arg_sorting_key = request.args.get("sorting_key", "dag_id")
        arg_sorting_direction = request.args.get("sorting_direction", default="asc")

        if request.args.get("reset_tags") is not None:
            flask_session[FILTER_TAGS_COOKIE] = None
            # Remove the reset_tags=reset from the URL
            return redirect(url_for("Airflow.index"))

        if arg_lastrun_filter == "reset_filter":
            flask_session[FILTER_LASTRUN_COOKIE] = None
            return redirect(url_for("Airflow.index"))

        filter_tags_cookie_val = flask_session.get(FILTER_TAGS_COOKIE)
        filter_lastrun_cookie_val = flask_session.get(FILTER_LASTRUN_COOKIE)

        # update filter args in url from session values if needed
        if (not arg_tags_filter and filter_tags_cookie_val) or (
            not arg_lastrun_filter and filter_lastrun_cookie_val
        ):
            tags = arg_tags_filter or (filter_tags_cookie_val and filter_tags_cookie_val.split(","))
            lastrun = arg_lastrun_filter or filter_lastrun_cookie_val
            return redirect(url_for("Airflow.index", tags=tags, lastrun=lastrun))

        if arg_tags_filter:
            flask_session[FILTER_TAGS_COOKIE] = ",".join(arg_tags_filter)

        if arg_lastrun_filter:
            arg_lastrun_filter = arg_lastrun_filter.strip().lower()
            flask_session[FILTER_LASTRUN_COOKIE] = arg_lastrun_filter

        if arg_status_filter is None:
            filter_status_cookie_val = flask_session.get(FILTER_STATUS_COOKIE)
            if filter_status_cookie_val:
                arg_status_filter = filter_status_cookie_val
            else:
                arg_status_filter = "active" if hide_paused_dags_by_default else "all"
                flask_session[FILTER_STATUS_COOKIE] = arg_status_filter
        else:
            status = arg_status_filter.strip().lower()
            flask_session[FILTER_STATUS_COOKIE] = status
            arg_status_filter = status

        dags_per_page = PAGE_SIZE

        start = current_page * dags_per_page
        end = start + dags_per_page

        # Get all the dag id the user could access
        filter_dag_ids = get_auth_manager().get_permitted_dag_ids(user=g.user)

        with create_session() as session:
            # read orm_dags from the db
            dags_query = select(DagModel).where(~DagModel.is_subdag, DagModel.is_active)

            if arg_search_query:
                escaped_arg_search_query = arg_search_query.replace("_", r"\_")
                dags_query = dags_query.where(
                    DagModel.dag_id.ilike("%" + escaped_arg_search_query + "%", escape="\\")
                    | DagModel._dag_display_property_value.ilike(
                        "%" + escaped_arg_search_query + "%", escape="\\"
                    )
                    | DagModel.owners.ilike("%" + escaped_arg_search_query + "%", escape="\\")
                )

            if arg_tags_filter:
                dags_query = dags_query.where(DagModel.tags.any(DagTag.name.in_(arg_tags_filter)))

            dags_query = dags_query.where(DagModel.dag_id.in_(filter_dag_ids))
            filtered_dag_count = get_query_count(dags_query, session=session)
            if filtered_dag_count == 0 and len(arg_tags_filter):
                flash(
                    "No matching DAG tags found.",
                    "warning",
                )
                flask_session[FILTER_TAGS_COOKIE] = None
                return redirect(url_for("Airflow.index"))

            # find DAGs which have a RUNNING DagRun
            running_dags = dags_query.join(DagRun, DagModel.dag_id == DagRun.dag_id).where(
                (DagRun.state == DagRunState.RUNNING) | (DagRun.state == DagRunState.QUEUED)
            )

            lastrun_running_is_paused = session.execute(
                running_dags.with_only_columns(DagModel.dag_id, DagModel.is_paused).distinct(DagModel.dag_id)
            ).all()

            lastrun_running_count_active = len(
                list(filter(lambda x: not x.is_paused, lastrun_running_is_paused))
            )
            lastrun_running_count_paused = len(list(filter(lambda x: x.is_paused, lastrun_running_is_paused)))

            # find DAGs for which the latest DagRun is FAILED
            subq_all = (
                select(DagRun.dag_id, func.max(DagRun.start_date).label("start_date"))
                .group_by(DagRun.dag_id)
                .subquery()
            )
            subq_failed = (
                select(DagRun.dag_id, func.max(DagRun.start_date).label("start_date"))
                .where(DagRun.state == DagRunState.FAILED)
                .group_by(DagRun.dag_id)
                .subquery()
            )
            subq_join = (
                select(subq_all.c.dag_id, subq_all.c.start_date)
                .join(
                    subq_failed,
                    and_(
                        subq_all.c.dag_id == subq_failed.c.dag_id,
                        subq_all.c.start_date == subq_failed.c.start_date,
                    ),
                )
                .subquery()
            )
            failed_dags = dags_query.join(subq_join, DagModel.dag_id == subq_join.c.dag_id)

            lastrun_failed_is_paused_count = dict(
                session.execute(
                    failed_dags.with_only_columns(DagModel.is_paused, func.count()).group_by(
                        DagModel.is_paused
                    )
                ).all()
            )

            lastrun_failed_count_active = lastrun_failed_is_paused_count.get(False, 0)
            lastrun_failed_count_paused = lastrun_failed_is_paused_count.get(True, 0)

            if arg_lastrun_filter == "running":
                dags_query = running_dags
            elif arg_lastrun_filter == "failed":
                dags_query = failed_dags

            all_dags = dags_query
            active_dags = dags_query.where(~DagModel.is_paused)
            paused_dags = dags_query.where(DagModel.is_paused)

            status_is_paused = session.execute(
                all_dags.with_only_columns(DagModel.dag_id, DagModel.is_paused).distinct(DagModel.dag_id)
            ).all()

            status_count_active = len(list(filter(lambda x: not x.is_paused, status_is_paused)))
            status_count_paused = len(list(filter(lambda x: x.is_paused, status_is_paused)))
            all_dags_count = status_count_active + status_count_paused

            if arg_status_filter == "active":
                current_dags = active_dags
                num_of_all_dags = status_count_active
                lastrun_count_running = lastrun_running_count_active
                lastrun_count_failed = lastrun_failed_count_active
            elif arg_status_filter == "paused":
                current_dags = paused_dags
                num_of_all_dags = status_count_paused
                lastrun_count_running = lastrun_running_count_paused
                lastrun_count_failed = lastrun_failed_count_paused
            else:
                current_dags = all_dags
                num_of_all_dags = all_dags_count
                lastrun_count_running = lastrun_running_count_active + lastrun_running_count_paused
                lastrun_count_failed = lastrun_failed_count_active + lastrun_failed_count_paused

            if arg_sorting_key == "dag_id":
                if arg_sorting_direction == "desc":
                    current_dags = current_dags.order_by(
                        func.coalesce(DagModel.dag_display_name, DagModel.dag_id).desc()
                    )
                else:
                    current_dags = current_dags.order_by(
                        func.coalesce(DagModel.dag_display_name, DagModel.dag_id)
                    )
            elif arg_sorting_key == "last_dagrun":
                dag_run_subquery = (
                    select(
                        DagRun.dag_id,
                        sqla.func.max(DagRun.execution_date).label("max_execution_date"),
                    )
                    .group_by(DagRun.dag_id)
                    .subquery()
                )
                current_dags = current_dags.outerjoin(
                    dag_run_subquery, and_(dag_run_subquery.c.dag_id == DagModel.dag_id)
                )
                null_case = case((dag_run_subquery.c.max_execution_date.is_(None), 1), else_=0)
                if arg_sorting_direction == "desc":
                    current_dags = current_dags.order_by(
                        null_case, dag_run_subquery.c.max_execution_date.desc()
                    )
                else:
                    current_dags = current_dags.order_by(null_case, dag_run_subquery.c.max_execution_date)
            else:
                sort_column = DagModel.__table__.c.get(arg_sorting_key)
                if sort_column is not None:
                    null_case = case((sort_column.is_(None), 1), else_=0)
                    if arg_sorting_direction == "desc":
                        current_dags = current_dags.order_by(null_case, sort_column.desc())
                    else:
                        current_dags = current_dags.order_by(null_case, sort_column)

            dags = (
                session.scalars(
                    current_dags.options(joinedload(DagModel.tags)).offset(start).limit(dags_per_page)
                )
                .unique()
                .all()
            )

            dataset_triggered_dag_ids = {dag.dag_id for dag in dags if dag.schedule_interval == "Dataset"}
            if dataset_triggered_dag_ids:
                dataset_triggered_next_run_info = get_dataset_triggered_next_run_info(
                    dataset_triggered_dag_ids, session=session
                )
            else:
                dataset_triggered_next_run_info = {}

            file_tokens = {}
            for dag in dags:
                dag.can_edit = get_auth_manager().is_authorized_dag(
                    method="PUT", details=DagDetails(id=dag.dag_id), user=g.user
                )
                can_create_dag_run = get_auth_manager().is_authorized_dag(
                    method="POST",
                    access_entity=DagAccessEntity.RUN,
                    details=DagDetails(id=dag.dag_id),
                    user=g.user,
                )
                dag.can_trigger = dag.can_edit and can_create_dag_run
                dag.can_delete = get_auth_manager().is_authorized_dag(
                    method="DELETE", details=DagDetails(id=dag.dag_id), user=g.user
                )
                url_serializer = URLSafeSerializer(current_app.config["SECRET_KEY"])
                file_tokens[dag.dag_id] = url_serializer.dumps(dag.fileloc)

            dagtags = session.execute(select(func.distinct(DagTag.name)).order_by(DagTag.name)).all()
            tags = [
                {"name": name, "selected": bool(arg_tags_filter and name in arg_tags_filter)}
                for (name,) in dagtags
            ]

            owner_links_dict = DagOwnerAttributes.get_all(session)

            if get_auth_manager().is_authorized_view(access_view=AccessView.IMPORT_ERRORS):
                import_errors = select(ParseImportError).order_by(ParseImportError.id)

                can_read_all_dags = get_auth_manager().is_authorized_dag(method="GET")
                if not can_read_all_dags:
                    # if the user doesn't have access to all DAGs, only display errors from visible DAGs
                    import_errors = import_errors.where(
                        ParseImportError.filename.in_(
                            select(DagModel.fileloc).distinct().where(DagModel.dag_id.in_(filter_dag_ids))
                        )
                    )

                import_errors = session.scalars(import_errors)
                for import_error in import_errors:
                    stacktrace = import_error.stacktrace
                    if not can_read_all_dags:
                        # Check if user has read access to all the DAGs defined in the file
                        file_dag_ids = (
                            session.query(DagModel.dag_id)
                            .filter(DagModel.fileloc == import_error.filename)
                            .all()
                        )
                        requests: Sequence[IsAuthorizedDagRequest] = [
                            {
                                "method": "GET",
                                "details": DagDetails(id=dag_id[0]),
                            }
                            for dag_id in file_dag_ids
                        ]
                        if not get_auth_manager().batch_is_authorized_dag(requests):
                            stacktrace = "REDACTED - you do not have read permission on all DAGs in the file"
                    flash(
                        f"Broken DAG: [{import_error.filename}]\r{stacktrace}",
                        "dag_import_error",
                    )

        from airflow.plugins_manager import import_errors as plugin_import_errors

        for filename, stacktrace in plugin_import_errors.items():
            flash(
                f"Broken plugin: [{filename}] {stacktrace}",
                "error",
            )

        num_of_pages = math.ceil(num_of_all_dags / dags_per_page)

        state_color_mapping = State.state_color.copy()
        state_color_mapping["null"] = state_color_mapping.pop(None)

        page_title = conf.get(section="webserver", key="instance_name", fallback="DAGs")
        page_title_has_markup = conf.getboolean(
            section="webserver", key="instance_name_has_markup", fallback=False
        )

        dashboard_alerts = [
            fm for fm in settings.DASHBOARD_UIALERTS if fm.should_show(get_airflow_app().appbuilder)
        ]

        def _iter_parsed_moved_data_table_names():
            for table_name in inspect(session.get_bind()).get_table_names():
                segments = table_name.split("__", 3)
                if len(segments) >= 3:
                    if segments[0] == settings.AIRFLOW_MOVED_TABLE_PREFIX:
                        # Second segment is a version marker that we don't need to show.
                        yield segments[-1], table_name

        if get_auth_manager().is_authorized_configuration(method="GET", user=g.user) and conf.getboolean(
            "webserver", "warn_deployment_exposure"
        ):
            robots_file_access_count = (
                select(Log)
                .where(Log.event == "robots")
                .where(Log.dttm > (utcnow() - datetime.timedelta(days=7)))
            )
            robots_file_access_count = get_query_count(robots_file_access_count, session=session)
            if robots_file_access_count > 0:
                flash(
                    Markup(
                        "Recent requests have been made to /robots.txt. "
                        "This indicates that this deployment may be accessible to the public internet. "
                        "This warning can be disabled by setting webserver.warn_deployment_exposure=False in "
                        "airflow.cfg. Read more about web deployment security <a href="
                        f'"{get_docs_url("security/webserver.html")}">'
                        "here</a>"
                    ),
                    "warning",
                )

        return self.render_template(
            "airflow/dags.html",
            dags=dags,
            show_trigger_form_if_no_params=conf.getboolean("webserver", "show_trigger_form_if_no_params"),
            dashboard_alerts=dashboard_alerts,
            migration_moved_data_alerts=sorted(set(_iter_parsed_moved_data_table_names())),
            current_page=current_page,
            search_query=arg_search_query or "",
            page_title=Markup(page_title) if page_title_has_markup else page_title,
            page_size=dags_per_page,
            num_of_pages=num_of_pages,
            num_dag_from=min(start + 1, num_of_all_dags),
            num_dag_to=min(end, num_of_all_dags),
            num_of_all_dags=num_of_all_dags,
            paging=wwwutils.generate_pages(
                current_page,
                num_of_pages,
                search=escape(arg_search_query) if arg_search_query else None,
                status=arg_status_filter or None,
                tags=arg_tags_filter or None,
                sorting_key=arg_sorting_key or None,
                sorting_direction=arg_sorting_direction or None,
            ),
            num_runs=num_runs,
            tags=tags,
            owner_links=owner_links_dict,
            state_color=state_color_mapping,
            status_filter=arg_status_filter,
            status_count_all=all_dags_count,
            status_count_active=status_count_active,
            status_count_paused=status_count_paused,
            lastrun_filter=arg_lastrun_filter,
            lastrun_count_running=lastrun_count_running,
            lastrun_count_failed=lastrun_count_failed,
            tags_filter=arg_tags_filter,
            sorting_key=arg_sorting_key,
            sorting_direction=arg_sorting_direction,
            auto_refresh_interval=conf.getint("webserver", "auto_refresh_interval"),
            dataset_triggered_next_run_info=dataset_triggered_next_run_info,
            file_tokens=file_tokens,
        )

    @expose("/datasets")
    @auth.has_access_dataset("GET")
    def datasets(self):
        """Datasets view."""
        state_color_mapping = State.state_color.copy()
        state_color_mapping["null"] = state_color_mapping.pop(None)
        return self.render_template(
            "airflow/datasets.html",
            auto_refresh_interval=conf.getint("webserver", "auto_refresh_interval"),
            state_color_mapping=state_color_mapping,
        )

    @expose("/cluster_activity")
    @auth.has_access_view(AccessView.CLUSTER_ACTIVITY)
    def cluster_activity(self):
        """Cluster Activity view."""
        state_color_mapping = State.state_color.copy()
        state_color_mapping["no_status"] = state_color_mapping.pop(None)
        standalone_dag_processor = conf.getboolean("scheduler", "standalone_dag_processor")
        return self.render_template(
            "airflow/cluster_activity.html",
            auto_refresh_interval=conf.getint("webserver", "auto_refresh_interval"),
            state_color_mapping=state_color_mapping,
            standalone_dag_processor=standalone_dag_processor,
        )

    @expose("/next_run_datasets_summary", methods=["POST"])
    @provide_session
    def next_run_datasets_summary(self, session: Session = NEW_SESSION):
        """Next run info for dataset triggered DAGs."""
        allowed_dag_ids = get_auth_manager().get_permitted_dag_ids(user=g.user)

        if not allowed_dag_ids:
            return flask.json.jsonify({})

        # Filter by post parameters
        selected_dag_ids = {unquote(dag_id) for dag_id in request.form.getlist("dag_ids") if dag_id}

        if selected_dag_ids:
            filter_dag_ids = selected_dag_ids.intersection(allowed_dag_ids)
        else:
            filter_dag_ids = allowed_dag_ids

        dataset_triggered_dag_ids = [
            dag_id
            for dag_id in (
                session.scalars(
                    select(DagModel.dag_id)
                    .where(DagModel.dag_id.in_(filter_dag_ids))
                    .where(DagModel.schedule_interval == "Dataset")
                )
            )
        ]

        dataset_triggered_next_run_info = get_dataset_triggered_next_run_info(
            dataset_triggered_dag_ids, session=session
        )

        return flask.json.jsonify(dataset_triggered_next_run_info)

    @expose("/dag_stats", methods=["POST"])
    @auth.has_access_dag("GET", DagAccessEntity.RUN)
    @provide_session
    def dag_stats(self, session: Session = NEW_SESSION):
        """Dag statistics."""
        allowed_dag_ids = get_auth_manager().get_permitted_dag_ids(user=g.user)

        # Filter by post parameters
        selected_dag_ids = {unquote(dag_id) for dag_id in request.form.getlist("dag_ids") if dag_id}
        if selected_dag_ids:
            filter_dag_ids = selected_dag_ids.intersection(allowed_dag_ids)
        else:
            filter_dag_ids = allowed_dag_ids
        if not filter_dag_ids:
            return flask.json.jsonify({})

        dag_state_stats = session.execute(
            select(DagRun.dag_id, DagRun.state, sqla.func.count(DagRun.state))
            .group_by(DagRun.dag_id, DagRun.state)
            .where(DagRun.dag_id.in_(filter_dag_ids))
        )
        dag_state_data = {(dag_id, state): count for dag_id, state, count in dag_state_stats}

        payload = {
            dag_id: [
                {"state": state, "count": dag_state_data.get((dag_id, state), 0)}
                for state in State.dag_states
            ]
            for dag_id in filter_dag_ids
        }
        return flask.json.jsonify(payload)

    @expose("/task_stats", methods=["POST"])
    @auth.has_access_dag("GET", DagAccessEntity.TASK_INSTANCE)
    @provide_session
    def task_stats(self, session: Session = NEW_SESSION):
        """Task Statistics."""
        allowed_dag_ids = get_auth_manager().get_permitted_dag_ids(user=g.user)

        if not allowed_dag_ids:
            return flask.json.jsonify({})

        # Filter by post parameters
        selected_dag_ids = {unquote(dag_id) for dag_id in request.form.getlist("dag_ids") if dag_id}

        if selected_dag_ids:
            filter_dag_ids = selected_dag_ids.intersection(allowed_dag_ids)
        else:
            filter_dag_ids = allowed_dag_ids

        running_dag_run_query_result = (
            select(DagRun.dag_id, DagRun.run_id)
            .join(DagModel, DagModel.dag_id == DagRun.dag_id)
            .where(DagRun.state == DagRunState.RUNNING, DagModel.is_active)
        )

        running_dag_run_query_result = running_dag_run_query_result.where(DagRun.dag_id.in_(filter_dag_ids))

        running_dag_run_query_result = running_dag_run_query_result.subquery("running_dag_run")

        # Select all task_instances from active dag_runs.
        running_task_instance_query_result = select(
            TaskInstance.dag_id.label("dag_id"),
            TaskInstance.state.label("state"),
            sqla.literal(True).label("is_dag_running"),
        ).join(
            running_dag_run_query_result,
            and_(
                running_dag_run_query_result.c.dag_id == TaskInstance.dag_id,
                running_dag_run_query_result.c.run_id == TaskInstance.run_id,
            ),
        )

        if conf.getboolean("webserver", "SHOW_RECENT_STATS_FOR_COMPLETED_RUNS", fallback=True):
            last_dag_run = (
                select(DagRun.dag_id, sqla.func.max(DagRun.execution_date).label("execution_date"))
                .join(DagModel, DagModel.dag_id == DagRun.dag_id)
                .where(DagRun.state != DagRunState.RUNNING, DagModel.is_active)
                .group_by(DagRun.dag_id)
            )

            last_dag_run = last_dag_run.where(DagRun.dag_id.in_(filter_dag_ids))
            last_dag_run = last_dag_run.subquery("last_dag_run")

            # Select all task_instances from active dag_runs.
            # If no dag_run is active, return task instances from most recent dag_run.
            last_task_instance_query_result = (
                select(
                    TaskInstance.dag_id.label("dag_id"),
                    TaskInstance.state.label("state"),
                    sqla.literal(False).label("is_dag_running"),
                )
                .join(TaskInstance.dag_run)
                .join(
                    last_dag_run,
                    and_(
                        last_dag_run.c.dag_id == TaskInstance.dag_id,
                        last_dag_run.c.execution_date == DagRun.execution_date,
                    ),
                )
            )

            final_task_instance_query_result = union_all(
                last_task_instance_query_result, running_task_instance_query_result
            ).alias("final_ti")
        else:
            final_task_instance_query_result = running_task_instance_query_result.subquery("final_ti")

        qry = session.execute(
            select(
                final_task_instance_query_result.c.dag_id,
                final_task_instance_query_result.c.state,
                final_task_instance_query_result.c.is_dag_running,
                sqla.func.count(),
            )
            .group_by(
                final_task_instance_query_result.c.dag_id,
                final_task_instance_query_result.c.state,
                final_task_instance_query_result.c.is_dag_running,
            )
            .order_by(
                final_task_instance_query_result.c.dag_id,
                final_task_instance_query_result.c.is_dag_running.desc(),
            )
        )
        data = get_task_stats_from_query(qry)
        payload: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for dag_id, state in itertools.product(filter_dag_ids, State.task_states):
            payload[dag_id].append({"state": state, "count": data.get(dag_id, {}).get(state, 0)})
        return flask.json.jsonify(payload)

    @expose("/last_dagruns", methods=["POST"])
    @auth.has_access_dag("GET", DagAccessEntity.RUN)
    @provide_session
    def last_dagruns(self, session: Session = NEW_SESSION):
        """Last DAG runs."""
        allowed_dag_ids = get_auth_manager().get_permitted_dag_ids(user=g.user)

        # Filter by post parameters
        selected_dag_ids = {unquote(dag_id) for dag_id in request.form.getlist("dag_ids") if dag_id}

        if selected_dag_ids:
            filter_dag_ids = selected_dag_ids.intersection(allowed_dag_ids)
        else:
            filter_dag_ids = allowed_dag_ids

        if not filter_dag_ids:
            return flask.json.jsonify({})

        last_runs_subquery = (
            select(
                DagRun.dag_id,
                sqla.func.max(DagRun.execution_date).label("max_execution_date"),
            )
            .group_by(DagRun.dag_id)
            .where(DagRun.dag_id.in_(filter_dag_ids))  # Only include accessible/selected DAGs.
            .subquery("last_runs")
        )

        query = session.execute(
            select(
                DagRun.dag_id,
                DagRun.start_date,
                DagRun.end_date,
                DagRun.state,
                DagRun.execution_date,
                DagRun.data_interval_start,
                DagRun.data_interval_end,
            ).join(
                last_runs_subquery,
                and_(
                    last_runs_subquery.c.dag_id == DagRun.dag_id,
                    last_runs_subquery.c.max_execution_date == DagRun.execution_date,
                ),
            )
        )

        resp = {
            r.dag_id.replace(".", "__dot__"): {
                "dag_id": r.dag_id,
                "state": r.state,
                "execution_date": wwwutils.datetime_to_string(r.execution_date),
                "start_date": wwwutils.datetime_to_string(r.start_date),
                "end_date": wwwutils.datetime_to_string(r.end_date),
                "data_interval_start": wwwutils.datetime_to_string(r.data_interval_start),
                "data_interval_end": wwwutils.datetime_to_string(r.data_interval_end),
            }
            for r in query
        }
        return flask.json.jsonify(resp)

    @expose("/code")
    def legacy_code(self):
        """Redirect from url param."""
        return redirect(url_for("Airflow.code", **sanitize_args(request.args)))

    @expose("/dags/<string:dag_id>/code")
    @auth.has_access_dag("GET", DagAccessEntity.CODE)
    def code(self, dag_id):
        """Dag Code."""
        kwargs = {
            **sanitize_args(request.args),
            "dag_id": dag_id,
            "tab": "code",
        }

        return redirect(url_for("Airflow.grid", **kwargs))

    @expose("/dag_details")
    def legacy_dag_details(self):
        """Redirect from url param."""
        return redirect(url_for("Airflow.dag_details", **sanitize_args(request.args)))

    @expose("/dags/<string:dag_id>/details")
    def dag_details(self, dag_id):
        """Get Dag details."""
        return redirect(url_for("Airflow.grid", dag_id=dag_id))

    @expose("/rendered-templates")
    @auth.has_access_dag("GET", DagAccessEntity.TASK_INSTANCE)
    @provide_session
    def rendered_templates(self, session):
        """Get rendered Dag."""
        dag_id = request.args.get("dag_id")
        task_id = request.args.get("task_id")
        map_index = request.args.get("map_index", -1, type=int)
        execution_date = request.args.get("execution_date")
        dttm = _safe_parse_datetime(execution_date)
        form = DateTimeForm(data={"execution_date": dttm})
        root = request.args.get("root", "")

        logger.info("Retrieving rendered templates.")
        dag: DAG = get_airflow_app().dag_bag.get_dag(dag_id)
        dag_run = dag.get_dagrun(execution_date=dttm)
        raw_task = dag.get_task(task_id).prepare_for_execution()

        no_dagrun = False
        url_serializer = URLSafeSerializer(current_app.config["SECRET_KEY"])

        title = "Rendered Template"
        html_dict = {}

        ti: TaskInstance
        if dag_run is None:
            # No DAG run matching given logical date. This usually means this
            # DAG has never been run. Task instance rendering does not really
            # make sense in this situation, but "works" prior to AIP-39. This
            # "fakes" a temporary DagRun-TaskInstance association (not saved to
            # database) for presentation only.
            ti = TaskInstance(raw_task, map_index=map_index)
            ti.dag_run = DagRun(dag_id=dag_id, execution_date=dttm)
            no_dagrun = True
        else:
            ti = dag_run.get_task_instance(task_id=task_id, map_index=map_index, session=session)
            if ti:
                ti.refresh_from_task(raw_task)
            else:
                flash(f"there is no task instance with the provided map_index {map_index}", "error")
                return self.render_template(
                    "airflow/ti_code.html",
                    show_trigger_form_if_no_params=conf.getboolean(
                        "webserver", "show_trigger_form_if_no_params"
                    ),
                    dag_run_id=dag_run.run_id if dag_run else "",
                    html_dict=html_dict,
                    dag=dag,
                    task_id=task_id,
                    execution_date=execution_date,
                    map_index=map_index,
                    form=form,
                    root=root,
                    title=title,
                )

        try:
            ti.get_rendered_template_fields(session=session)
        except AirflowException as e:
            if not e.__cause__:
                flash(f"Error rendering template: {e}", "error")
            else:
                msg = Markup("Error rendering template: {0}<br><br>OriginalError: {0.__cause__}").format(e)
                flash(msg, "error")
        except Exception as e:
            flash(f"Error rendering template: {e}", "error")

        # Ensure we are rendering the unmapped operator. Unmapping should be
        # done automatically if template fields are rendered successfully; this
        # only matters if get_rendered_template_fields() raised an exception.
        # The following rendering won't show useful values in this case anyway,
        # but we'll display some quasi-meaingful field names.
        task = ti.task.unmap(None)

        renderers = wwwutils.get_attr_renderer()

        for template_field in task.template_fields:
            content = getattr(task, template_field)
            renderer = task.template_fields_renderers.get(template_field, template_field)
            if renderer in renderers:
                html_dict[template_field] = renderers[renderer](content) if not no_dagrun else ""
            else:
                html_dict[template_field] = Markup("<pre><code>{}</pre></code>").format(
                    pformat(content) if not no_dagrun else ""
                )

            if isinstance(content, dict):
                if template_field == "op_kwargs":
                    for key, value in content.items():
                        renderer = task.template_fields_renderers.get(key, key)
                        if renderer in renderers:
                            html_dict[f"{template_field}.{key}"] = (
                                renderers[renderer](value) if not no_dagrun else ""
                            )
                        else:
                            html_dict[f"{template_field}.{key}"] = Markup(
                                "<pre><code>{}</pre></code>"
                            ).format(pformat(value) if not no_dagrun else "")
                else:
                    for dict_keys in get_key_paths(content):
                        template_path = f"{template_field}.{dict_keys}"
                        renderer = task.template_fields_renderers.get(template_path, template_path)
                        if renderer in renderers:
                            content_value = get_value_from_path(dict_keys, content)
                            html_dict[template_path] = (
                                renderers[renderer](content_value) if not no_dagrun else ""
                            )
        return self.render_template(
            "airflow/ti_code.html",
            show_trigger_form_if_no_params=conf.getboolean("webserver", "show_trigger_form_if_no_params"),
            html_dict=html_dict,
            dag_run_id=dag_run.run_id if dag_run else "",
            dag=dag,
            task_id=task_id,
            task_display_name=task.task_display_name,
            execution_date=execution_date,
            map_index=map_index,
            form=form,
            root=root,
            title=title,
            dag_file_token=url_serializer.dumps(dag.fileloc),
        )

    @expose("/rendered-k8s")
    @auth.has_access_dag("GET", DagAccessEntity.TASK_INSTANCE)
    @provide_session
    def rendered_k8s(self, *, session: Session = NEW_SESSION):
        """Get rendered k8s yaml."""
        if not settings.IS_K8S_OR_K8SCELERY_EXECUTOR:
            abort(404)
        # This part is only used for k8s executor so providers.cncf.kubernetes must be installed
        # with the get_rendered_k8s_spec method
        from airflow.providers.cncf.kubernetes.template_rendering import get_rendered_k8s_spec

        dag_id = request.args.get("dag_id")
        task_id = request.args.get("task_id")
        if task_id is None:
            logger.warning("Task id not passed in the request")
            abort(400)
        execution_date = request.args.get("execution_date")
        dttm = _safe_parse_datetime(execution_date)
        form = DateTimeForm(data={"execution_date": dttm})
        root = request.args.get("root", "")
        map_index = request.args.get("map_index", -1, type=int)
        logger.info("Retrieving rendered k8s.")

        dag: DAG = get_airflow_app().dag_bag.get_dag(dag_id)
        task = dag.get_task(task_id)
        dag_run = dag.get_dagrun(execution_date=dttm, session=session)
        ti = dag_run.get_task_instance(task_id=task.task_id, map_index=map_index, session=session)

        if not ti:
            raise AirflowException(f"Task instance {task.task_id} not found.")

        pod_spec = None
        if not isinstance(ti, TaskInstance):
            raise ValueError("not a TaskInstance")
        try:
            pod_spec = get_rendered_k8s_spec(ti, session=session)
        except AirflowException as e:
            if not e.__cause__:
                flash(f"Error rendering Kubernetes POD Spec: {e}", "error")
            else:
                tmp = Markup("Error rendering Kubernetes POD Spec: {0}<br><br>Original error: {0.__cause__}")
                flash(tmp.format(e), "error")
        except Exception as e:
            flash(f"Error rendering Kubernetes Pod Spec: {e}", "error")
        title = "Rendered K8s Pod Spec"

        if pod_spec:
            content = wwwutils.get_attr_renderer()["yaml"](yaml.dump(pod_spec))
        else:
            content = Markup("<pre><code>Error rendering Kubernetes POD Spec</pre></code>")

        return self.render_template(
            "airflow/ti_code.html",
            show_trigger_form_if_no_params=conf.getboolean("webserver", "show_trigger_form_if_no_params"),
            dag_run_id=dag_run.run_id if dag_run else "",
            html_dict={"k8s": content},
            dag=dag,
            task_id=task_id,
            task_display_name=task.task_display_name,
            execution_date=execution_date,
            map_index=map_index,
            form=form,
            root=root,
            title=title,
        )

    @expose("/object/rendered-k8s")
    @auth.has_access_dag("GET", DagAccessEntity.TASK_INSTANCE)
    @provide_session
    def rendered_k8s_data(self, *, session: Session = NEW_SESSION):
        """Get rendered k8s yaml."""
        if not settings.IS_K8S_OR_K8SCELERY_EXECUTOR:
            return {"error": "Not a k8s or k8s_celery executor"}, 404
        # This part is only used for k8s executor so providers.cncf.kubernetes must be installed
        # with the get_rendered_k8s_spec method
        from airflow.providers.cncf.kubernetes.template_rendering import get_rendered_k8s_spec

        dag_id = request.args.get("dag_id")
        task_id = request.args.get("task_id")
        if task_id is None:
            return {"error": "Task id not passed in the request"}, 404
        run_id = request.args.get("run_id")
        map_index = request.args.get("map_index", -1, type=int)
        logger.info("Retrieving rendered k8s data.")

        dag: DAG = get_airflow_app().dag_bag.get_dag(dag_id)
        task = dag.get_task(task_id)
        dag_run = dag.get_dagrun(run_id=run_id, session=session)
        ti = dag_run.get_task_instance(task_id=task.task_id, map_index=map_index, session=session)

        if not ti:
            return {"error": f"can't find task instance {task.task_id}"}, 404
        pod_spec = None
        if not isinstance(ti, TaskInstance):
            return {"error": f"{task.task_id} is not a task instance"}, 500
        try:
            pod_spec = get_rendered_k8s_spec(ti, session=session)
        except AirflowException as e:
            if not e.__cause__:
                return {"error": f"Error rendering Kubernetes POD Spec: {e}"}, 500
            else:
                tmp = Markup("Error rendering Kubernetes POD Spec: {0}<br><br>Original error: {0.__cause__}")
                return {"error": tmp.format(e)}, 500
        except Exception as e:
            return {"error": f"Error rendering Kubernetes Pod Spec: {e}"}, 500

        return pod_spec

    @expose("/get_logs_with_metadata")
    @auth.has_access_dag("GET", DagAccessEntity.TASK_INSTANCE)
    @auth.has_access_dag("GET", DagAccessEntity.TASK_LOGS)
    @provide_session
    def get_logs_with_metadata(self, session: Session = NEW_SESSION):
        """Retrieve logs including metadata."""
        dag_id = request.args.get("dag_id")
        task_id = request.args.get("task_id")
        execution_date_str = request.args["execution_date"]
        map_index = request.args.get("map_index", -1, type=int)
        try_number = request.args.get("try_number", type=int)
        metadata_str = request.args.get("metadata", "{}")
        response_format = request.args.get("format", "json")

        # Validate JSON metadata
        try:
            metadata: dict = json.loads(metadata_str) or {}
        except json.decoder.JSONDecodeError:
            return {"error": "Invalid JSON metadata"}, 400

        # Convert string datetime into actual datetime
        try:
            execution_date = timezone.parse(execution_date_str, strict=True)
        except ValueError:
            error_message = (
                f"Given execution date {execution_date_str!r} could not be identified as a date. "
                "Example date format: 2015-11-16T14:34:15+00:00"
            )
            return {"error": error_message}, 400

        task_log_reader = TaskLogReader()
        if not task_log_reader.supports_read:
            return {
                "message": "Task log handler does not support read logs.",
                "error": True,
                "metadata": {"end_of_log": True},
            }

        ti = session.scalar(
            select(models.TaskInstance)
            .where(
                TaskInstance.task_id == task_id,
                TaskInstance.dag_id == dag_id,
                TaskInstance.execution_date == execution_date,
                TaskInstance.map_index == map_index,
            )
            .join(TaskInstance.dag_run)
            .options(joinedload(TaskInstance.trigger).joinedload(Trigger.triggerer_job))
            .limit(1)
        )

        if ti is None:
            return {
                "message": "*** Task instance did not exist in the DB\n",
                "error": True,
                "metadata": {"end_of_log": True},
            }

        try:
            dag = get_airflow_app().dag_bag.get_dag(dag_id)
            if dag:
                ti.task = dag.get_task(ti.task_id)

            if response_format == "json":
                logs, metadata = task_log_reader.read_log_chunks(ti, try_number, metadata)
                message = logs[0] if try_number is not None else logs
                return {"message": message, "metadata": metadata}

            metadata["download_logs"] = True
            attachment_filename = task_log_reader.render_log_filename(ti, try_number, session=session)
            log_stream = task_log_reader.read_log_stream(ti, try_number, metadata)
            return Response(
                response=log_stream,
                mimetype="text/plain",
                headers={"Content-Disposition": f"attachment; filename={attachment_filename}"},
            )
        except AttributeError as e:
            error_messages = [f"Task log handler does not support read logs.\n{e}\n"]
            metadata["end_of_log"] = True
            return {"message": error_messages, "error": True, "metadata": metadata}

    @expose("/log")
    @auth.has_access_dag("GET", DagAccessEntity.TASK_LOGS)
    @provide_session
    def log(self, session: Session = NEW_SESSION):
        """Retrieve log."""
        dag_id = request.args["dag_id"]
        task_id = request.args.get("task_id")
        map_index = request.args.get("map_index", -1, type=int)
        execution_date = request.args.get("execution_date")

        if execution_date:
            dttm = _safe_parse_datetime(execution_date)
        else:
            dttm = None

        form = DateTimeForm(data={"execution_date": dttm})
        dag_model = DagModel.get_dagmodel(dag_id)

        ti: TaskInstance = session.scalar(
            select(models.TaskInstance)
            .filter_by(dag_id=dag_id, task_id=task_id, execution_date=dttm, map_index=map_index)
            .limit(1)
        )

        num_logs = 0
        if ti is not None:
            num_logs = ti.try_number
        logs = [""] * num_logs
        root = request.args.get("root", "")
        return self.render_template(
            "airflow/ti_log.html",
            show_trigger_form_if_no_params=conf.getboolean("webserver", "show_trigger_form_if_no_params"),
            logs=logs,
            dag=dag_model,
            dag_run_id=ti.run_id if ti else "",
            title="Log by attempts",
            dag_id=dag_id,
            task_id=task_id,
            task_display_name=ti.task_display_name,
            execution_date=execution_date,
            map_index=map_index,
            form=form,
            root=root,
            wrapped=conf.getboolean("webserver", "default_wrap"),
        )

    @expose("/redirect_to_external_log")
    @auth.has_access_dag("GET", DagAccessEntity.TASK_LOGS)
    @provide_session
    def redirect_to_external_log(self, session: Session = NEW_SESSION):
        """Redirects to external log."""
        dag_id = request.args.get("dag_id")
        task_id = request.args.get("task_id")
        execution_date = request.args.get("execution_date")
        dttm = _safe_parse_datetime(execution_date)
        map_index = request.args.get("map_index", -1, type=int)
        try_number = request.args.get("try_number", 1)

        ti = session.scalar(
            select(models.TaskInstance)
            .filter_by(dag_id=dag_id, task_id=task_id, execution_date=dttm, map_index=map_index)
            .limit(1)
        )

        if not ti:
            flash(f"Task [{dag_id}.{task_id}] does not exist", "error")
            return redirect(url_for("Airflow.index"))

        task_log_reader = TaskLogReader()
        if not task_log_reader.supports_external_link:
            flash("Task log handler does not support external links", "error")
            return redirect(url_for("Airflow.index"))

        handler = task_log_reader.log_handler
        url = handler.get_external_log_url(ti, try_number)
        return redirect(url)

    @expose("/task")
    @auth.has_access_dag("GET", DagAccessEntity.TASK_INSTANCE)
    @provide_session
    def task(self, session: Session = NEW_SESSION):
        """Retrieve task."""
        dag_id = request.args.get("dag_id")
        task_id = request.args.get("task_id")
        execution_date = request.args.get("execution_date")
        dttm = _safe_parse_datetime(execution_date)
        map_index = request.args.get("map_index", -1, type=int)
        form = DateTimeForm(data={"execution_date": dttm})
        root = request.args.get("root", "")
        dag = get_airflow_app().dag_bag.get_dag(dag_id)

        if not dag or task_id not in dag.task_ids:
            flash(f"Task [{dag_id}.{task_id}] doesn't seem to exist at the moment", "error")
            return redirect(url_for("Airflow.index"))
        task = copy.copy(dag.get_task(task_id))
        task.resolve_template_files()

        ti: TaskInstance | None = session.scalar(
            select(TaskInstance)
            .options(
                # HACK: Eager-load relationships. This is needed because
                # multiple properties mis-use provide_session() that destroys
                # the session object ti is bounded to.
                joinedload(TaskInstance.queued_by_job, innerjoin=False),
                joinedload(TaskInstance.trigger, innerjoin=False),
            )
            .filter_by(execution_date=dttm, dag_id=dag_id, task_id=task_id, map_index=map_index)
        )
        if ti is None:
            ti_attrs: list[tuple[str, Any]] | None = None
        else:
            ti.refresh_from_task(task)
            ti_attrs_to_skip = [
                "dag_id",
                "key",
                "mark_success_url",
                "log",
                "log_url",
                "task",
                "trigger",
                "triggerer_job",
            ]
            # Some fields on TI are deprecated, but we don't want those warnings here.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RemovedInAirflow3Warning)
                all_ti_attrs = (
                    # fetching the value of _try_number to be shown under name try_number in UI
                    (name, getattr(ti, name))
                    for name in dir(ti)
                    if not name.startswith("_") and name not in ti_attrs_to_skip
                )
            ti_attrs = sorted((name, attr) for name, attr in all_ti_attrs if not callable(attr))

        attr_renderers = wwwutils.get_attr_renderer()

        attrs_to_skip: frozenset[str] = getattr(task, "HIDE_ATTRS_FROM_UI", frozenset())

        def include_task_attrs(attr_name):
            return not (
                attr_name == "HIDE_ATTRS_FROM_UI"
                or attr_name.startswith("_")
                or attr_name in attr_renderers
                or attr_name in attrs_to_skip
            )

        task_attrs = [
            (attr_name, secrets_masker.redact(attr, attr_name))
            for attr_name, attr in (
                (attr_name, getattr(task, attr_name)) for attr_name in filter(include_task_attrs, dir(task))
            )
            if not callable(attr)
        ]

        # Color coding the special attributes that are code
        special_attrs_rendered = {
            attr_name: renderer(getattr(task, attr_name))
            for attr_name, renderer in attr_renderers.items()
            if hasattr(task, attr_name)
        }

        no_failed_deps_result = [
            (
                "Unknown",
                "All dependencies are met but the task instance is not running. In most "
                "cases this just means that the task will probably be scheduled soon "
                "unless:<br>\n- The scheduler is down or under heavy load<br>\n{}\n"
                "<br>\nIf this task instance does not start soon please contact your "
                "Airflow administrator for assistance.".format(
                    "- This task instance already ran and had it's state changed manually "
                    "(e.g. cleared in the UI)<br>"
                    if ti and ti.state is None
                    else ""
                ),
            )
        ]

        # Use the scheduler's context to figure out which dependencies are not met
        if ti is None:
            failed_dep_reasons: list[tuple[str, str]] = []
        else:
            dep_context = DepContext(SCHEDULER_QUEUED_DEPS)
            failed_dep_reasons = [
                (dep.dep_name, dep.reason) for dep in ti.get_failed_dep_statuses(dep_context=dep_context)
            ]

        title = "Task Instance Details"
        return self.render_template(
            "airflow/task.html",
            show_trigger_form_if_no_params=conf.getboolean("webserver", "show_trigger_form_if_no_params"),
            task_attrs=task_attrs,
            ti_attrs=ti_attrs,
            dag_run_id=ti.run_id if ti else "",
            failed_dep_reasons=failed_dep_reasons or no_failed_deps_result,
            task_id=task_id,
            execution_date=execution_date,
            map_index=map_index,
            special_attrs_rendered=special_attrs_rendered,
            form=form,
            root=root,
            dag=dag,
            title=title,
            task_display_name=task.task_display_name,
        )

    @expose("/xcom")
    @auth.has_access_dag("GET", DagAccessEntity.XCOM)
    @provide_session
    def xcom(self, session: Session = NEW_SESSION):
        """Retrieve XCOM."""
        dag_id = request.args["dag_id"]
        task_id = request.args.get("task_id")
        map_index = request.args.get("map_index", -1, type=int)
        # Carrying execution_date through, even though it's irrelevant for
        # this context
        execution_date = request.args.get("execution_date")
        dttm = _safe_parse_datetime(execution_date)

        form = DateTimeForm(data={"execution_date": dttm})
        root = request.args.get("root", "")
        dag = DagModel.get_dagmodel(dag_id)
        ti: TaskInstance = session.scalar(
            select(TaskInstance).filter_by(dag_id=dag_id, task_id=task_id).limit(1)
        )

        if not ti:
            flash(f"Task [{dag_id}.{task_id}] doesn't seem to exist at the moment", "error")
            return redirect(url_for("Airflow.index"))

        xcom_query = session.scalars(
            select(XCom).where(
                XCom.dag_id == dag_id,
                XCom.task_id == task_id,
                XCom.execution_date == dttm,
                XCom.map_index == map_index,
            )
        )
        attributes = [(xcom.key, xcom.value) for xcom in xcom_query if not xcom.key.startswith("_")]

        title = "XCom"
        return self.render_template(
            "airflow/xcom.html",
            show_trigger_form_if_no_params=conf.getboolean("webserver", "show_trigger_form_if_no_params"),
            attributes=attributes,
            task_id=task_id,
            dag_run_id=ti.run_id if ti else "",
            task_display_name=ti.task_display_name,
            execution_date=execution_date,
            map_index=map_index,
            form=form,
            root=root,
            dag=dag,
            title=title,
        )

    @expose("/delete", methods=["POST"])
    @auth.has_access_dag("DELETE")
    @action_logging
    def delete(self):
        """Delete DAG."""
        from airflow.api.common import delete_dag
        from airflow.exceptions import DagNotFound

        dag_id = request.values.get("dag_id")
        origin = get_safe_url(request.values.get("origin"))
        redirect_url = get_safe_url(request.values.get("redirect_url"))

        try:
            delete_dag.delete_dag(dag_id)
        except DagNotFound:
            flash(f"DAG with id {dag_id} not found. Cannot delete", "error")
            return redirect(redirect_url)
        except AirflowException:
            flash(
                f"Cannot delete DAG with id {dag_id} because some task instances of the DAG "
                "are still running. Please mark the  task instances as "
                "failed/succeeded before deleting the DAG",
                "error",
            )
            return redirect(redirect_url)

        flash(f"Deleting DAG with id {dag_id}. May take a couple minutes to fully disappear.")

        # Upon success return to origin.
        return redirect(origin)

    @expose("/dags/<string:dag_id>/trigger", methods=["POST", "GET"])
    @auth.has_access_dag("POST", DagAccessEntity.RUN)
    @action_logging
    @provide_session
    def trigger(self, dag_id: str, session: Session = NEW_SESSION):
        """Triggers DAG Run."""
        run_id = request.values.get("run_id", "")
        origin = get_safe_url(request.values.get("origin"))
        unpause = request.values.get("unpause")
        request_conf = request.values.get("conf")
        request_execution_date = request.values.get("execution_date", default=timezone.utcnow().isoformat())
        is_dag_run_conf_overrides_params = conf.getboolean("core", "dag_run_conf_overrides_params")
        dag = get_airflow_app().dag_bag.get_dag(dag_id)
        dag_orm: DagModel = session.scalar(select(DagModel).where(DagModel.dag_id == dag_id).limit(1))

        # Prepare form fields with param struct details to render a proper form with schema information
        form_fields = {}
        allow_raw_html_descriptions = conf.getboolean("webserver", "allow_raw_html_descriptions")
        form_trust_problems = []
        for k, v in dag.params.items():
            form_fields[k] = v.dump()
            form_field: dict = form_fields[k]
            # If no schema is provided, auto-detect on default values
            if "schema" not in form_field:
                form_field["schema"] = {}
            form_field_schema: dict = form_field["schema"]
            if "type" not in form_field_schema:
                form_field_value = form_field["value"]
                if isinstance(form_field_value, bool):
                    form_field_schema["type"] = "boolean"
                elif isinstance(form_field_value, int):
                    form_field_schema["type"] = ["integer", "null"]
                elif isinstance(form_field_value, list):
                    form_field_schema["type"] = ["array", "null"]
                elif isinstance(form_field_value, dict):
                    form_field_schema["type"] = ["object", "null"]
            # Mark HTML fields as safe if allowed
            if allow_raw_html_descriptions:
                if "description_html" in form_field_schema:
                    form_field["description"] = Markup(form_field_schema["description_html"])
                if "custom_html_form" in form_field_schema:
                    form_field_schema["custom_html_form"] = Markup(form_field_schema["custom_html_form"])
            else:
                if "description_html" in form_field_schema and "description_md" not in form_field_schema:
                    form_trust_problems.append(f"Field {k} uses HTML description")
                    form_field["description"] = form_field_schema.pop("description_html")
                if "custom_html_form" in form_field_schema:
                    form_trust_problems.append(f"Field {k} uses custom HTML form definition")
                    form_field_schema.pop("custom_html_form")
            if "description_md" in form_field_schema:
                form_field["description"] = wwwutils.wrapped_markdown(form_field_schema["description_md"])
            # Check for default values and pre-populate
            if k in request.values:
                if form_field_schema.get("type", None) in [
                    "boolean",
                    "array",
                    ["array", "null"],
                    "object",
                    ["object", "null"],
                ]:
                    try:
                        form_field["value"] = json.loads(request.values.get(k, ""))
                    except JSONDecodeError:
                        flash(
                            f'Could not pre-populate field "{k}" due to parsing error of value "{request.values.get(k)}"'
                        )
                else:
                    form_field["value"] = request.values.get(k)
        if form_trust_problems:
            flash(
                Markup(
                    "At least one field in the trigger form uses a raw HTML form definition. This is not allowed for "
                    "security. Please switch to markdown description via <code>description_md</code>. "
                    "Raw HTML is deprecated and must be enabled via "
                    "<code>webserver.allow_raw_html_descriptions</code> configuration parameter. Using plain text "
                    "as fallback for these fields. "
                    f"<ul><li>{'</li><li>'.join(form_trust_problems)}</li></ul>"
                ),
                "warning",
            )
        if allow_raw_html_descriptions and any("description_html" in p.schema for p in dag.params.values()):
            flash(
                Markup(
                    "The form params use raw HTML in <code>description_html</code> which is deprecated. "
                    "Please migrate to <code>description_md</code>."
                ),
                "warning",
            )
        if allow_raw_html_descriptions and any("custom_html_form" in p.schema for p in dag.params.values()):
            flash(
                Markup(
                    "The form params use <code>custom_html_form</code> definition. "
                    "This is deprecated with Airflow 2.8.0 and will be removed in a future release."
                ),
                "warning",
            )

        ui_fields_defined = any("const" not in f["schema"] for f in form_fields.values())
        show_trigger_form_if_no_params = conf.getboolean("webserver", "show_trigger_form_if_no_params")

        if not dag_orm:
            flash(f"Cannot find dag {dag_id}")
            return redirect(origin)

        if dag_orm.has_import_errors:
            flash(f"Cannot create dagruns because the dag {dag_id} has import errors", "error")
            return redirect(origin)

        num_recent_confs = conf.getint("webserver", "num_recent_configurations_for_trigger")
        recent_runs = session.execute(
            select(DagRun.conf, func.max(DagRun.run_id).label("run_id"), func.max(DagRun.execution_date))
            .where(
                DagRun.dag_id == dag_id,
                DagRun.run_type == DagRunType.MANUAL,
                DagRun.conf.isnot(None),
            )
            .group_by(DagRun.conf)
            .order_by(func.max(DagRun.execution_date).desc())
            .limit(num_recent_confs)
        )
        recent_confs = {
            run_id: json.dumps(run_conf, cls=utils_json.WebEncoder)
            for run_id, run_conf in ((run.run_id, run.conf) for run in recent_runs)
            if isinstance(run_conf, dict) and any(run_conf)
        }
        render_params = {
            "dag": dag,
            "dag_id": dag_id,
            "run_id": run_id,
            "origin": origin,
            "doc_md": wwwutils.wrapped_markdown(getattr(dag, "doc_md", None)),
            "recent_confs": recent_confs,
            "is_dag_run_conf_overrides_params": is_dag_run_conf_overrides_params,
        }

        if request.method == "GET" or (
            not request_conf and (ui_fields_defined or show_trigger_form_if_no_params)
        ):
            # Populate conf textarea with conf requests parameter, or dag.params
            default_conf = ""

            form = DateTimeForm(data={"execution_date": request_execution_date})

            if request_conf:
                default_conf = request_conf
            else:
                try:
                    default_conf = json.dumps(
                        {
                            str(k): v.resolve(
                                value=request.values.get(k, default=NOTSET), suppress_exception=True
                            )
                            for k, v in dag.params.items()
                        },
                        indent=4,
                        ensure_ascii=False,
                        cls=utils_json.WebEncoder,
                    )
                except TypeError:
                    flash("Could not pre-populate conf field due to non-JSON-serializable data-types")
            return self.render_template(
                "airflow/trigger.html",
                form_fields=form_fields,
                **render_params,
                conf=default_conf,
                form=form,
            )

        try:
            execution_date = timezone.parse(request_execution_date, strict=True)
        except ParserError:
            flash("Invalid execution date", "error")
            form = DateTimeForm(data={"execution_date": timezone.utcnow().isoformat()})
            return self.render_template(
                "airflow/trigger.html",
                form_fields=form_fields,
                **render_params,
                conf=request_conf or {},
                form=form,
            )

        dr = DagRun.find_duplicate(dag_id=dag_id, run_id=run_id, execution_date=execution_date)
        if dr:
            if dr.run_id == run_id:
                message = f"The run ID {run_id} already exists"
            else:
                message = f"The logical date {execution_date} already exists"
            flash(message, "error")
            return redirect(origin)

        regex = conf.get("scheduler", "allowed_run_id_pattern")
        if run_id and not re2.match(RUN_ID_REGEX, run_id):
            if not regex.strip() or not re2.match(regex.strip(), run_id):
                flash(
                    f"The provided run ID '{run_id}' is invalid. It does not match either "
                    f"the configured pattern: '{regex}' or the built-in pattern: '{RUN_ID_REGEX}'",
                    "error",
                )

                form = DateTimeForm(data={"execution_date": execution_date})
                return self.render_template(
                    "airflow/trigger.html",
                    form_fields=form_fields,
                    **render_params,
                    conf=request_conf,
                    form=form,
                )

        run_conf = {}
        if request_conf:
            try:
                run_conf = json.loads(request_conf)
                if not isinstance(run_conf, dict):
                    flash("Invalid JSON configuration, must be a dict", "error")
                    form = DateTimeForm(data={"execution_date": execution_date})
                    return self.render_template(
                        "airflow/trigger.html",
                        form_fields=form_fields,
                        **render_params,
                        conf=request_conf,
                        form=form,
                    )
            except json.decoder.JSONDecodeError:
                flash("Invalid JSON configuration, not parseable", "error")
                form = DateTimeForm(data={"execution_date": execution_date})
                return self.render_template(
                    "airflow/trigger.html",
                    form_fields=form_fields,
                    **render_params,
                    conf=request_conf,
                    form=form,
                )

        if dag.get_is_paused():
            if unpause or not ui_fields_defined:
                flash(f"Unpaused DAG {dag_id}.")
                dag_model = models.DagModel.get_dagmodel(dag_id)
                if dag_model is not None:
                    dag_model.set_is_paused(is_paused=False)
            else:
                flash(
                    f"DAG {dag_id} is paused, unpause if you want to have the triggered run being executed.",
                    "warning",
                )

        try:
            dag_run = dag.create_dagrun(
                run_type=DagRunType.MANUAL,
                execution_date=execution_date,
                data_interval=dag.timetable.infer_manual_data_interval(run_after=execution_date),
                state=DagRunState.QUEUED,
                conf=run_conf,
                external_trigger=True,
                dag_hash=get_airflow_app().dag_bag.dags_hash.get(dag_id),
                run_id=run_id,
            )
        except (ValueError, ParamValidationError) as ve:
            flash(f"{ve}", "error")
            form = DateTimeForm(data={"execution_date": execution_date})
            # Take over "bad" submitted fields for new form display
            for k in form_fields:
                if k in run_conf:
                    form_fields[k]["value"] = run_conf[k]
            return self.render_template(
                "airflow/trigger.html",
                form_fields=form_fields,
                **render_params,
                conf=request_conf,
                form=form,
            )

        flash(f"Triggered {dag_id} with new Run ID {dag_run.run_id}, it should start any moment now.")
        if "/grid?" in origin:
            path, query = origin.split("?", 1)
            params = {param.split("=")[0]: param.split("=")[1] for param in query.split("&")}
            params["dag_run_id"] = dag_run.run_id
            origin = f"{path}?{urlencode(params)}"
        elif origin.endswith("/grid"):
            origin += f"?{urlencode({'dag_run_id': dag_run.run_id})}"
        return redirect(origin)

    def _clear_dag_tis(
        self,
        dag: DAG,
        start_date: datetime.datetime | None,
        end_date: datetime.datetime | None,
        *,
        origin: str | None,
        task_ids: Collection[str | tuple[str, int]] | None = None,
        recursive: bool = False,
        confirmed: bool = False,
        only_failed: bool = False,
        session: Session,
    ):
        if confirmed:
            count = dag.clear(
                start_date=start_date,
                end_date=end_date,
                task_ids=task_ids,
                include_subdags=recursive,
                include_parentdag=recursive,
                only_failed=only_failed,
                session=session,
            )

            msg = f"{count} task instances have been cleared"
            return redirect_or_json(origin, msg)

        try:
            tis = dag.clear(
                start_date=start_date,
                end_date=end_date,
                task_ids=task_ids,
                include_subdags=recursive,
                include_parentdag=recursive,
                only_failed=only_failed,
                dry_run=True,
                session=session,
            )
        except AirflowException as ex:
            return redirect_or_json(origin, msg=str(ex), status="error", status_code=500)

        if not isinstance(tis, collections.abc.Iterable):
            raise AssertionError(
                f"Expected dag.clear() to return an iterable for dry runs, got {tis} instead."
            )

        details = [str(t) for t in tis]

        if not details:
            return redirect_or_json(origin, "No task instances to clear", status="error", status_code=404)
        elif request.headers.get("Accept") == "application/json":
            if confirmed:
                return htmlsafe_json_dumps(details, separators=(",", ":"))
            return htmlsafe_json_dumps(
                [{"task_id": ti.task_id, "map_index": ti.map_index, "run_id": ti.run_id} for ti in tis],
                separators=(",", ":"),
            )
        return self.render_template(
            "airflow/confirm.html",
            endpoint=None,
            message="Task instances you are about to clear:",
            details="\n".join(details),
        )

    @expose("/clear", methods=["POST"])
    @auth.has_access_dag("PUT", DagAccessEntity.TASK_INSTANCE)
    @action_logging
    @provide_session
    def clear(self, *, session: Session = NEW_SESSION):
        """Clear DAG tasks."""
        dag_id = request.form.get("dag_id")
        task_id = request.form.get("task_id")
        origin = get_safe_url(request.form.get("origin"))
        dag = get_airflow_app().dag_bag.get_dag(dag_id)
        group_id = request.form.get("group_id")

        if "map_index" not in request.form:
            map_indexes: list[int] | None = None
        else:
            map_indexes = request.form.getlist("map_index", type=int)

        execution_date_str = request.form.get("execution_date")
        execution_date = _safe_parse_datetime(execution_date_str)
        confirmed = request.form.get("confirmed") == "true"
        upstream = request.form.get("upstream") == "true"
        downstream = request.form.get("downstream") == "true"
        future = request.form.get("future") == "true"
        past = request.form.get("past") == "true"
        recursive = request.form.get("recursive") == "true"
        only_failed = request.form.get("only_failed") == "true"

        task_ids: list[str | tuple[str, int]] = []

        end_date = execution_date if not future else None
        start_date = execution_date if not past else None

        locked_dag_run_ids: list[int] = []

        if group_id is not None:
            task_group_dict = dag.task_group.get_task_group_dict()
            task_group = task_group_dict.get(group_id)
            if task_group is None:
                return redirect_or_json(
                    origin, msg=f"TaskGroup {group_id} could not be found", status="error", status_code=404
                )
            task_ids = task_ids_or_regex = [t.task_id for t in task_group.iter_tasks()]

            # Lock the related dag runs to prevent from possible dead lock.
            # https://github.com/apache/airflow/pull/26658
            dag_runs_query = select(DagRun.id).where(DagRun.dag_id == dag_id).with_for_update()

            if start_date is None and end_date is None:
                dag_runs_query = dag_runs_query.where(DagRun.execution_date == start_date)
            else:
                if start_date is not None:
                    dag_runs_query = dag_runs_query.where(DagRun.execution_date >= start_date)

                if end_date is not None:
                    dag_runs_query = dag_runs_query.where(DagRun.execution_date <= end_date)

            locked_dag_run_ids = session.scalars(dag_runs_query).all()
        elif task_id:
            if map_indexes is None:
                task_ids = [task_id]
            else:
                task_ids = [(task_id, map_index) for map_index in map_indexes]
            task_ids_or_regex = [task_id]

        dag = dag.partial_subset(
            task_ids_or_regex=task_ids_or_regex,
            include_downstream=downstream,
            include_upstream=upstream,
        )

        if len(dag.task_dict) > 1:
            # If we had upstream/downstream etc then also include those!
            task_ids.extend(tid for tid in dag.task_dict if tid != task_id)

        response = self._clear_dag_tis(
            dag,
            start_date,
            end_date,
            origin=origin,
            task_ids=task_ids,
            recursive=recursive,
            confirmed=confirmed,
            only_failed=only_failed,
            session=session,
        )

        del locked_dag_run_ids

        return response

    @expose("/dagrun_clear", methods=["POST"])
    @auth.has_access_dag("PUT", DagAccessEntity.TASK_INSTANCE)
    @action_logging
    @provide_session
    def dagrun_clear(self, *, session: Session = NEW_SESSION):
        """Clear the DagRun."""
        dag_id = request.form.get("dag_id")
        dag_run_id = request.form.get("dag_run_id")
        confirmed = request.form.get("confirmed") == "true"
        only_failed = request.form.get("only_failed") == "true"

        dag = get_airflow_app().dag_bag.get_dag(dag_id)
        dr = dag.get_dagrun(run_id=dag_run_id)
        start_date = dr.logical_date
        end_date = dr.logical_date

        return self._clear_dag_tis(
            dag,
            start_date,
            end_date,
            origin=None,
            recursive=True,
            confirmed=confirmed,
            only_failed=only_failed,
            session=session,
        )

    @expose("/blocked", methods=["POST"])
    @auth.has_access_dag("GET", DagAccessEntity.RUN)
    @provide_session
    def blocked(self, session: Session = NEW_SESSION):
        """Retrieve active_dag_runs and max_active_runs information for running Dags."""
        allowed_dag_ids = get_auth_manager().get_permitted_dag_ids(user=g.user)

        # Filter by post parameters
        selected_dag_ids = {unquote(dag_id) for dag_id in request.form.getlist("dag_ids") if dag_id}

        if selected_dag_ids:
            filter_dag_ids = selected_dag_ids.intersection(allowed_dag_ids)
        else:
            filter_dag_ids = allowed_dag_ids

        if not filter_dag_ids:
            return flask.json.jsonify([])

        dags = session.execute(
            select(DagRun.dag_id, sqla.func.count(DagRun.id))
            .where(DagRun.state == DagRunState.RUNNING)
            .where(DagRun.dag_id.in_(filter_dag_ids))
            .group_by(DagRun.dag_id)
        )

        payload = []
        for dag_id, active_dag_runs in dags:
            max_active_runs = 0
            dag = get_airflow_app().dag_bag.get_dag(dag_id)
            if dag:
                # TODO: Make max_active_runs a column so we can query for it directly
                max_active_runs = dag.max_active_runs
            payload.append(
                {
                    "dag_id": dag_id,
                    "active_dag_run": active_dag_runs,
                    "max_active_runs": max_active_runs,
                }
            )
        return flask.json.jsonify(payload)

    def _mark_dagrun_state_as_failed(self, dag_id, dag_run_id, confirmed):
        if not dag_run_id:
            return {"status": "error", "message": "Invalid dag_run_id"}

        dag = get_airflow_app().dag_bag.get_dag(dag_id)

        if not dag:
            return {"status": "error", "message": f"Cannot find DAG: {dag_id}"}

        new_dag_state = set_dag_run_state_to_failed(dag=dag, run_id=dag_run_id, commit=confirmed)

        if confirmed:
            return {"status": "success", "message": f"Marked failed on {len(new_dag_state)} task instances"}
        else:
            details = [str(t) for t in new_dag_state]

            return htmlsafe_json_dumps(details, separators=(",", ":"))

    def _mark_dagrun_state_as_success(self, dag_id, dag_run_id, confirmed):
        if not dag_run_id:
            return {"status": "error", "message": "Invalid dag_run_id"}

        dag = get_airflow_app().dag_bag.get_dag(dag_id)

        if not dag:
            return {"status": "error", "message": f"Cannot find DAG: {dag_id}"}

        new_dag_state = set_dag_run_state_to_success(dag=dag, run_id=dag_run_id, commit=confirmed)

        if confirmed:
            return {"status": "success", "message": f"Marked success on {len(new_dag_state)} task instances"}
        else:
            details = [str(t) for t in new_dag_state]

            return htmlsafe_json_dumps(details, separators=(",", ":"))

    @provide_session
    def _mark_dagrun_state_as_queued(
        self,
        dag_id: str,
        dag_run_id: str,
        confirmed: bool,
        session: Session = NEW_SESSION,
    ):
        if not dag_run_id:
            return {"status": "error", "message": "Invalid dag_run_id"}

        dag = get_airflow_app().dag_bag.get_dag(dag_id)

        if not dag:
            return {"status": "error", "message": f"Cannot find DAG: {dag_id}"}

        set_dag_run_state_to_queued(dag=dag, run_id=dag_run_id, commit=confirmed)

        if confirmed:
            return {"status": "success", "message": "Marked the DagRun as queued."}

        else:
            # Identify tasks that will be queued up to run when confirmed
            all_task_ids = [task.task_id for task in dag.tasks]

            existing_tis = session.execute(
                select(TaskInstance.task_id).where(
                    TaskInstance.dag_id == dag.dag_id,
                    TaskInstance.run_id == dag_run_id,
                )
            )

            completed_tis_ids = [task_id for (task_id,) in existing_tis]
            tasks_with_no_state = list(set(all_task_ids) - set(completed_tis_ids))
            details = [str(t) for t in tasks_with_no_state]

            return htmlsafe_json_dumps(details, separators=(",", ":"))

    @expose("/dagrun_failed", methods=["POST"])
    @auth.has_access_dag("PUT", DagAccessEntity.RUN)
    @action_logging
    def dagrun_failed(self):
        """Mark DagRun failed."""
        dag_id = request.form.get("dag_id")
        dag_run_id = request.form.get("dag_run_id")
        confirmed = request.form.get("confirmed") == "true"
        return self._mark_dagrun_state_as_failed(dag_id, dag_run_id, confirmed)

    @expose("/dagrun_success", methods=["POST"])
    @auth.has_access_dag("PUT", DagAccessEntity.RUN)
    @action_logging
    def dagrun_success(self):
        """Mark DagRun success."""
        dag_id = request.form.get("dag_id")
        dag_run_id = request.form.get("dag_run_id")
        confirmed = request.form.get("confirmed") == "true"
        return self._mark_dagrun_state_as_success(dag_id, dag_run_id, confirmed)

    @expose("/dagrun_queued", methods=["POST"])
    @auth.has_access_dag("PUT", DagAccessEntity.RUN)
    @action_logging
    def dagrun_queued(self):
        """Queue DagRun so tasks that haven't run yet can be started."""
        dag_id = request.form.get("dag_id")
        dag_run_id = request.form.get("dag_run_id")
        confirmed = request.form.get("confirmed") == "true"
        return self._mark_dagrun_state_as_queued(dag_id, dag_run_id, confirmed)

    @expose("/dagrun_details")
    def dagrun_details(self):
        """Redirect to the Grid DagRun page. This is avoids breaking links."""
        dag_id = request.args.get("dag_id")
        run_id = request.args.get("run_id")
        return redirect(url_for("Airflow.grid", dag_id=dag_id, dag_run_id=run_id))

    def _mark_task_instance_state(
        self,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        map_indexes: list[int] | None,
        origin: str,
        upstream: bool,
        downstream: bool,
        future: bool,
        past: bool,
        state: TaskInstanceState,
    ):
        dag: DAG = get_airflow_app().dag_bag.get_dag(dag_id)

        if not run_id:
            flash(f"Cannot mark tasks as {state}, seem that DAG {dag_id} has never run", "error")
            return redirect(origin)

        altered = dag.set_task_instance_state(
            task_id=task_id,
            map_indexes=map_indexes,
            run_id=run_id,
            state=state,
            upstream=upstream,
            downstream=downstream,
            future=future,
            past=past,
        )

        flash(f"Marked {state} on {len(altered)} task instances")
        return redirect(origin)

    def _mark_task_group_state(
        self,
        *,
        dag_id: str,
        run_id: str,
        group_id: str,
        origin: str,
        upstream: bool,
        downstream: bool,
        future: bool,
        past: bool,
        state: TaskInstanceState,
    ):
        dag: DAG = get_airflow_app().dag_bag.get_dag(dag_id)

        if not run_id:
            flash(f"Cannot mark tasks as {state}, as DAG {dag_id} has never run", "error")
            return redirect(origin)

        altered = dag.set_task_group_state(
            group_id=group_id,
            run_id=run_id,
            state=state,
            upstream=upstream,
            downstream=downstream,
            future=future,
            past=past,
        )

        flash(f"Marked {state} on {len(altered)} task instances")
        return redirect(origin)

    @expose("/confirm", methods=["GET"])
    @auth.has_access_dag("PUT", DagAccessEntity.TASK_INSTANCE)
    @action_logging
    def confirm(self):
        """Show confirmation page for marking tasks as success or failed."""
        args = request.args
        dag_id = args.get("dag_id")
        task_id = args.get("task_id")
        dag_run_id = args.get("dag_run_id")
        state = args.get("state")
        origin = get_safe_url(args.get("origin"))
        group_id = args.get("group_id")

        if "map_index" not in args:
            map_indexes: list[int] | None = None
        else:
            map_indexes = args.getlist("map_index", type=int)

        upstream = to_boolean(args.get("upstream"))
        downstream = to_boolean(args.get("downstream"))
        future = to_boolean(args.get("future"))
        past = to_boolean(args.get("past"))
        origin = origin or url_for("Airflow.index")

        if not exactly_one(task_id, group_id):
            raise ValueError("Exactly one of task_id or group_id must be provided")

        dag = get_airflow_app().dag_bag.get_dag(dag_id)
        if not dag:
            msg = f"DAG {dag_id} not found"
            return redirect_or_json(origin, msg, status="error", status_code=404)

        if state not in (
            "success",
            "failed",
        ):
            msg = f"Invalid state {state}, must be either 'success' or 'failed'"
            return redirect_or_json(origin, msg, status="error", status_code=400)

        latest_execution_date = dag.get_latest_execution_date()
        if not latest_execution_date:
            msg = f"Cannot mark tasks as {state}, seem that dag {dag_id} has never run"
            return redirect_or_json(origin, msg, status="error", status_code=400)

        tasks: list[Operator | tuple[Operator, int]] = []

        if group_id:
            task_group_dict = dag.task_group.get_task_group_dict()
            task_group = task_group_dict.get(group_id)
            if task_group is None:
                return redirect_or_json(
                    origin, msg=f"TaskGroup {group_id} could not be found", status="error", status_code=404
                )
            tasks = list(task_group.iter_tasks())
        elif task_id:
            try:
                task = dag.get_task(task_id)
            except airflow.exceptions.TaskNotFound:
                msg = f"Task {task_id} not found"
                return redirect_or_json(origin, msg, status="error", status_code=404)
            task.dag = dag
            if map_indexes is None:
                tasks = [task]
            else:
                tasks = [(task, map_index) for map_index in map_indexes]

        to_be_altered = set_state(
            tasks=tasks,
            run_id=dag_run_id,
            upstream=upstream,
            downstream=downstream,
            future=future,
            past=past,
            state=state,
            commit=False,
        )

        if request.headers.get("Accept") == "application/json":
            return htmlsafe_json_dumps(
                [
                    {"task_id": ti.task_id, "map_index": ti.map_index, "run_id": ti.run_id}
                    for ti in to_be_altered
                ],
                separators=(",", ":"),
            )

        details = "\n".join(str(t) for t in to_be_altered)

        response = self.render_template(
            "airflow/confirm.html",
            endpoint=url_for(f"Airflow.{state}"),
            message=f"Task instances you are about to mark as {state}:",
            details=details,
        )

        return response

    @expose("/failed", methods=["POST"])
    @auth.has_access_dag("PUT", DagAccessEntity.TASK_INSTANCE)
    @action_logging
    def failed(self):
        """Mark task or task_group as failed."""
        args = request.form
        dag_id = args.get("dag_id")
        task_id = args.get("task_id")
        run_id = args.get("dag_run_id")
        group_id = args.get("group_id")

        if not exactly_one(task_id, group_id):
            raise ValueError("Exactly one of task_id or group_id must be provided")

        if "map_index" not in args:
            map_indexes: list[int] | None = None
        else:
            map_indexes = args.getlist("map_index", type=int)

        origin = get_safe_url(args.get("origin"))
        upstream = to_boolean(args.get("upstream"))
        downstream = to_boolean(args.get("downstream"))
        future = to_boolean(args.get("future"))
        past = to_boolean(args.get("past"))

        if task_id:
            return self._mark_task_instance_state(
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                map_indexes=map_indexes,
                origin=origin,
                upstream=upstream,
                downstream=downstream,
                future=future,
                past=past,
                state=TaskInstanceState.FAILED,
            )
        elif group_id:
            return self._mark_task_group_state(
                dag_id=dag_id,
                run_id=run_id,
                group_id=group_id,
                origin=origin,
                upstream=upstream,
                downstream=downstream,
                future=future,
                past=past,
                state=TaskInstanceState.FAILED,
            )

    @expose("/success", methods=["POST"])
    @auth.has_access_dag("PUT", DagAccessEntity.TASK_INSTANCE)
    @action_logging
    def success(self):
        """Mark task or task_group as success."""
        args = request.form
        dag_id = args.get("dag_id")
        task_id = args.get("task_id")
        run_id = args.get("dag_run_id")
        group_id = args.get("group_id")

        if not exactly_one(task_id, group_id):
            raise ValueError("Exactly one of task_id or group_id must be provided")

        if "map_index" not in args:
            map_indexes: list[int] | None = None
        else:
            map_indexes = args.getlist("map_index", type=int)

        origin = get_safe_url(args.get("origin"))
        upstream = to_boolean(args.get("upstream"))
        downstream = to_boolean(args.get("downstream"))
        future = to_boolean(args.get("future"))
        past = to_boolean(args.get("past"))

        if task_id:
            return self._mark_task_instance_state(
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                map_indexes=map_indexes,
                origin=origin,
                upstream=upstream,
                downstream=downstream,
                future=future,
                past=past,
                state=TaskInstanceState.SUCCESS,
            )
        elif group_id:
            return self._mark_task_group_state(
                dag_id=dag_id,
                run_id=run_id,
                group_id=group_id,
                origin=origin,
                upstream=upstream,
                downstream=downstream,
                future=future,
                past=past,
                state=TaskInstanceState.SUCCESS,
            )

    @expose("/dags/<string:dag_id>")
    def dag(self, dag_id):
        """Redirect to default DAG view."""
        kwargs = {**sanitize_args(request.args), "dag_id": dag_id}
        return redirect(url_for("Airflow.grid", **kwargs))

    @expose("/tree")
    def legacy_tree(self):
        """Redirect to the replacement - grid view. Kept for backwards compatibility."""
        return redirect(url_for("Airflow.grid", **sanitize_args(request.args)))

    @expose("/dags/<string:dag_id>/grid")
    @auth.has_access_dag("GET", DagAccessEntity.TASK_INSTANCE)
    @gzipped
    @provide_session
    def grid(self, dag_id: str, session: Session = NEW_SESSION):
        """Get Dag's grid view."""
        color_log_error_keywords = conf.get("logging", "color_log_error_keywords", fallback="")
        color_log_warning_keywords = conf.get("logging", "color_log_warning_keywords", fallback="")

        dag = get_airflow_app().dag_bag.get_dag(dag_id, session=session)
        url_serializer = URLSafeSerializer(current_app.config["SECRET_KEY"])
        dag_model = DagModel.get_dagmodel(dag_id, session=session)
        if not dag:
            flash(f'DAG "{dag_id}" seems to be missing from DagBag.', "error")
            return redirect(url_for("Airflow.index"))
        wwwutils.check_import_errors(dag.fileloc, session)
        wwwutils.check_dag_warnings(dag.dag_id, session)

        included_events_raw = conf.get("webserver", "audit_view_included_events", fallback="")
        excluded_events_raw = conf.get("webserver", "audit_view_excluded_events", fallback="")

        root = request.args.get("root")
        if root:
            dag = dag.partial_subset(task_ids_or_regex=root, include_downstream=False, include_upstream=True)

        num_runs = request.args.get("num_runs", type=int)
        if num_runs is None:
            num_runs = conf.getint("webserver", "default_dag_run_display_number")

        doc_md = wwwutils.wrapped_markdown(getattr(dag, "doc_md", None))

        task_log_reader = TaskLogReader()
        if task_log_reader.supports_external_link:
            external_log_name = task_log_reader.log_handler.log_name
        else:
            external_log_name = None

        default_dag_run_display_number = conf.getint("webserver", "default_dag_run_display_number")

        num_runs_options = [5, 25, 50, 100, 365]

        if default_dag_run_display_number not in num_runs_options:
            insort_left(num_runs_options, default_dag_run_display_number)

        can_edit_taskinstance = get_auth_manager().is_authorized_dag(
            method="PUT",
            access_entity=DagAccessEntity.TASK_INSTANCE,
        )

        return self.render_template(
            "airflow/grid.html",
            show_trigger_form_if_no_params=conf.getboolean("webserver", "show_trigger_form_if_no_params"),
            root=root,
            dag=dag,
            doc_md=doc_md,
            num_runs=num_runs,
            can_edit_taskinstance=can_edit_taskinstance,
            show_external_log_redirect=task_log_reader.supports_external_link,
            external_log_name=external_log_name,
            dag_model=dag_model,
            auto_refresh_interval=conf.getint("webserver", "auto_refresh_interval"),
            default_dag_run_display_number=default_dag_run_display_number,
            default_wrap=conf.getboolean("webserver", "default_wrap"),
            filters_drop_down_values=htmlsafe_json_dumps(
                {
                    "taskStates": [state.value for state in TaskInstanceState],
                    "dagStates": [state.value for state in State.dag_states],
                    "runTypes": [run_type.value for run_type in DagRunType],
                    "numRuns": num_runs_options,
                }
            ),
            included_events_raw=included_events_raw,
            excluded_events_raw=excluded_events_raw,
            color_log_error_keywords=color_log_error_keywords,
            color_log_warning_keywords=color_log_warning_keywords,
            dag_file_token=url_serializer.dumps(dag.fileloc),
        )

    @expose("/calendar")
    def legacy_calendar(self):
        """Redirect from url param."""
        return redirect(url_for("Airflow.calendar", **sanitize_args(request.args)))

    @expose("/dags/<string:dag_id>/calendar")
    def calendar(self, dag_id: str):
        """Redirect to the replacement - grid + calendar. Kept for backwards compatibility."""
        kwargs = {**sanitize_args(request.args), "dag_id": dag_id, "tab": "calendar"}

        return redirect(url_for("Airflow.grid", **kwargs))

    @expose("/object/calendar_data")
    @auth.has_access_dag("GET", DagAccessEntity.RUN)
    @gzipped
    @provide_session
    def calendar_data(self, session: Session = NEW_SESSION):
        """Get DAG runs as calendar."""
        dag_id = request.args.get("dag_id")
        dag = get_airflow_app().dag_bag.get_dag(dag_id, session=session)
        if not dag:
            return {"error": f"can't find dag {dag_id}"}, 404

        dag_states = session.execute(
            select(
                func.date(DagRun.execution_date).label("date"),
                DagRun.state,
                func.max(DagRun.data_interval_start).label("data_interval_start"),
                func.max(DagRun.data_interval_end).label("data_interval_end"),
                func.count("*").label("count"),
            )
            .where(DagRun.dag_id == dag.dag_id)
            .group_by(func.date(DagRun.execution_date), DagRun.state)
            .order_by(func.date(DagRun.execution_date).asc())
        ).all()

        data_dag_states = [
            {
                # DATE() in SQLite and MySQL behave differently:
                # SQLite returns a string, MySQL returns a date.
                "date": dr.date if isinstance(dr.date, str) else dr.date.isoformat(),
                "state": dr.state,
                "count": dr.count,
            }
            for dr in dag_states
        ]

        # Upper limit of how many planned runs we should iterate through
        max_planned_runs = 2000
        total_planned = 0

        # Interpret the schedule and show planned dag runs in calendar
        if (
            dag_states
            and dag_states[-1].data_interval_start
            and dag_states[-1].data_interval_end
            and not isinstance(dag.timetable, ContinuousTimetable)
        ):
            last_automated_data_interval = DataInterval(
                timezone.coerce_datetime(dag_states[-1].data_interval_start),
                timezone.coerce_datetime(dag_states[-1].data_interval_end),
            )

            year = last_automated_data_interval.end.year
            restriction = TimeRestriction(dag.start_date, dag.end_date, False)
            dates: dict[datetime.date, int] = collections.Counter()

            if isinstance(dag.timetable, CronMixin):
                # Optimized calendar generation for timetables based on a cron expression.
                dates_iter: Iterator[datetime.datetime | None] = croniter(
                    dag.timetable._expression,
                    start_time=last_automated_data_interval.end,
                    ret_type=datetime.datetime,
                )
                for dt in dates_iter:
                    if dt is None:
                        break
                    if dt.year != year:
                        break
                    if dag.end_date and dt > dag.end_date:
                        break
                    dates[dt.date()] += 1
            else:
                prev_logical_date = DateTime.min
                while True:
                    curr_info = dag.timetable.next_dagrun_info(
                        last_automated_data_interval=last_automated_data_interval,
                        restriction=restriction,
                    )
                    if curr_info is None:
                        break  # Reached the end.
                    if curr_info.logical_date <= prev_logical_date:
                        break  # We're not progressing. Maybe a malformed timetable? Give up.
                    if curr_info.logical_date.year != year:
                        break  # Crossed the year boundary.
                    last_automated_data_interval = curr_info.data_interval
                    dates[curr_info.logical_date.date()] += 1
                    prev_logical_date = curr_info.logical_date
                    total_planned += 1
                    if total_planned > max_planned_runs:
                        break

            data_dag_states.extend(
                {"date": date.isoformat(), "state": "planned", "count": count}
                for (date, count) in dates.items()
            )

        data = {
            "dag_states": data_dag_states,
        }

        return (
            htmlsafe_json_dumps(data, separators=(",", ":"), dumps=flask.json.dumps),
            {"Content-Type": "application/json; charset=utf-8"},
        )

    @expose("/graph")
    def legacy_graph(self):
        """Redirect from url param."""
        return redirect(url_for("Airflow.graph", **sanitize_args(request.args)))

    @expose("/dags/<string:dag_id>/graph")
    @gzipped
    @provide_session
    def graph(self, dag_id: str, session: Session = NEW_SESSION):
        """Redirect to the replacement - grid + graph. Kept for backwards compatibility."""
        dag = get_airflow_app().dag_bag.get_dag(dag_id, session=session)
        if not dag:
            flash(f'DAG "{dag_id}" seems to be missing from DagBag.', "error")
            return redirect(url_for("Airflow.index"))
        dt_nr_dr_data = get_date_time_num_runs_dag_runs_form_data(request, session, dag)
        dttm = dt_nr_dr_data["dttm"]
        dag_run = dag.get_dagrun(execution_date=dttm)
        dag_run_id = dag_run.run_id if dag_run else None

        kwargs = {
            **sanitize_args(request.args),
            "dag_id": dag_id,
            "tab": "graph",
            "dag_run_id": dag_run_id,
        }

        return redirect(url_for("Airflow.grid", **kwargs))

    @expose("/duration")
    def legacy_duration(self):
        """Redirect from url param."""
        return redirect(url_for("Airflow.duration", **sanitize_args(request.args)))

    @expose("/dags/<string:dag_id>/duration")
    def duration(self, dag_id: str):
        """Redirect to Grid view."""
        return redirect(url_for("Airflow.grid", dag_id=dag_id))

    @expose("/tries")
    def legacy_tries(self):
        """Redirect from url param."""
        return redirect(url_for("Airflow.tries", **sanitize_args(request.args)))

    @expose("/dags/<string:dag_id>/tries")
    def tries(self, dag_id: str):
        """Redirect to grid view."""
        kwargs = {
            **sanitize_args(request.args),
            "dag_id": dag_id,
        }
        return redirect(url_for("Airflow.grid", **kwargs))

    @expose("/landing_times")
    def legacy_landing_times(self):
        """Redirect from url param."""
        return redirect(url_for("Airflow.landing_times", **sanitize_args(request.args)))

    @expose("/dags/<string:dag_id>/landing-times")
    def landing_times(self, dag_id: str):
        """Redirect to run duration page."""
        kwargs = {
            **sanitize_args(request.args),
            "dag_id": dag_id,
            "tab": "run_duration",
        }

        return redirect(url_for("Airflow.grid", **kwargs))

    @expose("/paused", methods=["POST"])
    @auth.has_access_dag("PUT")
    @action_logging
    def paused(self):
        """Toggle paused."""
        dag_id = request.args.get("dag_id")
        is_paused = request.args.get("is_paused") == "false"
        models.DagModel.get_dagmodel(dag_id).set_is_paused(is_paused=is_paused)
        return "OK"

    @expose("/gantt")
    def legacy_gantt(self):
        """Redirect from url param."""
        return redirect(url_for("Airflow.gantt", **sanitize_args(request.args)))

    @expose("/dags/<string:dag_id>/gantt")
    @auth.has_access_dag("GET", DagAccessEntity.TASK_INSTANCE)
    @provide_session
    def gantt(self, dag_id: str, session: Session = NEW_SESSION):
        """Redirect to the replacement - grid + gantt. Kept for backwards compatibility."""
        dag = get_airflow_app().dag_bag.get_dag(dag_id, session=session)
        dt_nr_dr_data = get_date_time_num_runs_dag_runs_form_data(request, session, dag)
        dttm = dt_nr_dr_data["dttm"]
        dag_run = dag.get_dagrun(execution_date=dttm)
        dag_run_id = dag_run.run_id if dag_run else None

        kwargs = {**sanitize_args(request.args), "dag_id": dag_id, "tab": "gantt", "dag_run_id": dag_run_id}

        return redirect(url_for("Airflow.grid", **kwargs))

    @expose("/extra_links")
    @auth.has_access_dag("GET", DagAccessEntity.TASK_INSTANCE)
    @provide_session
    def extra_links(self, *, session: Session = NEW_SESSION):
        """
        Return external links for a given Operator.

        It queries the operator that sent the request for the links it wishes
        to provide for a given external link name.

        API: GET
        Args: dag_id: The id of the dag containing the task in question
              task_id: The id of the task in question
              execution_date: The date of execution of the task
              link_name: The name of the link reference to find the actual URL for

        Returns:
            200: {url: <url of link>, error: None} - returned when there was no problem
                finding the URL
            404: {url: None, error: <error message>} - returned when the operator does
                not return a URL
        """
        dag_id = request.args.get("dag_id")
        task_id = request.args.get("task_id")
        map_index = request.args.get("map_index", -1, type=int)
        execution_date = request.args.get("execution_date")
        dttm = _safe_parse_datetime(execution_date)
        dag = get_airflow_app().dag_bag.get_dag(dag_id)

        if not dag or task_id not in dag.task_ids:
            return {"url": None, "error": f"can't find dag {dag} or task_id {task_id}"}, 404

        task = dag.get_task(task_id)
        link_name = request.args.get("link_name")
        if link_name is None:
            return {"url": None, "error": "Link name not passed"}, 400

        ti = session.scalar(
            select(TaskInstance)
            .filter_by(dag_id=dag_id, task_id=task_id, execution_date=dttm, map_index=map_index)
            .options(joinedload(TaskInstance.dag_run))
            .limit(1)
        )

        if not ti:
            return {"url": None, "error": "Task Instances not found"}, 404
        try:
            url = task.get_extra_links(ti, link_name)
        except ValueError as err:
            return {"url": None, "error": str(err)}, 404
        if url:
            return {"error": None, "url": url}
        else:
            return {"url": None, "error": f"No URL found for {link_name}"}, 404

    @expose("/object/graph_data")
    @auth.has_access_dag("GET", DagAccessEntity.TASK_INSTANCE)
    @gzipped
    def graph_data(self):
        """Get Graph Data."""
        dag_id = request.args.get("dag_id")
        dag = get_airflow_app().dag_bag.get_dag(dag_id)
        root = request.args.get("root")
        if root:
            filter_upstream = request.args.get("filter_upstream") == "true"
            filter_downstream = request.args.get("filter_downstream") == "true"
            dag = dag.partial_subset(
                task_ids_or_regex=root, include_upstream=filter_upstream, include_downstream=filter_downstream
            )

        nodes = task_group_to_dict(dag.task_group)
        edges = dag_edges(dag)

        data = {
            "arrange": dag.orientation,
            "nodes": nodes,
            "edges": edges,
        }
        return (
            htmlsafe_json_dumps(data, separators=(",", ":"), dumps=flask.json.dumps),
            {"Content-Type": "application/json; charset=utf-8"},
        )

    @expose("/object/task_instances")
    @auth.has_access_dag("GET", DagAccessEntity.TASK_INSTANCE)
    def task_instances(self):
        """Show task instances."""
        dag_id = request.args.get("dag_id")
        dag = get_airflow_app().dag_bag.get_dag(dag_id)

        dttm = request.args.get("execution_date")
        if dttm:
            dttm = _safe_parse_datetime(dttm)
        else:
            return {"error": f"Invalid execution_date {dttm}"}, 400

        with create_session() as session:
            task_instances = {
                ti.task_id: wwwutils.get_instance_with_map(ti, session)
                for ti in dag.get_task_instances(dttm, dttm)
            }

        return flask.json.jsonify(task_instances)

    @expose("/object/grid_data")
    @auth.has_access_dag("GET", DagAccessEntity.TASK_INSTANCE)
    def grid_data(self):
        """Return grid data."""
        dag_id = request.args.get("dag_id")
        dag = get_airflow_app().dag_bag.get_dag(dag_id)

        if not dag:
            return {"error": f"can't find dag {dag_id}"}, 404

        root = request.args.get("root")
        if root:
            filter_upstream = request.args.get("filter_upstream") == "true"
            filter_downstream = request.args.get("filter_downstream") == "true"
            dag = dag.partial_subset(
                task_ids_or_regex=root, include_upstream=filter_upstream, include_downstream=filter_downstream
            )

        num_runs = request.args.get("num_runs", type=int)
        if num_runs is None:
            num_runs = conf.getint("webserver", "default_dag_run_display_number")

        try:
            base_date = timezone.parse(request.args["base_date"], strict=True)
        except (KeyError, ValueError):
            base_date = dag.get_latest_execution_date() or timezone.utcnow()

        with create_session() as session:
            query = select(DagRun).where(DagRun.dag_id == dag.dag_id, DagRun.execution_date <= base_date)

        run_types = request.args.getlist("run_type")
        if run_types:
            query = query.where(DagRun.run_type.in_(run_types))

        run_states = request.args.getlist("run_state")
        if run_states:
            query = query.where(DagRun.state.in_(run_states))

        # Retrieve, sort and encode the previous DAG Runs
        dag_runs = wwwutils.sorted_dag_runs(
            query, ordering=dag.timetable.run_ordering, limit=num_runs, session=session
        )
        encoded_runs = []
        encoding_errors = []
        for dr in dag_runs:
            encoded_dr, error = wwwutils.encode_dag_run(dr, json_encoder=utils_json.WebEncoder)
            if error:
                encoding_errors.append(error)
            else:
                encoded_runs.append(encoded_dr)

        data = {
            "groups": dag_to_grid(dag, dag_runs, session),
            "dag_runs": encoded_runs,
            "ordering": dag.timetable.run_ordering,
            "errors": encoding_errors,
        }
        # avoid spaces to reduce payload size
        return (
            htmlsafe_json_dumps(data, separators=(",", ":"), dumps=flask.json.dumps),
            {"Content-Type": "application/json; charset=utf-8"},
        )

    @expose("/object/historical_metrics_data")
    @auth.has_access_view(AccessView.CLUSTER_ACTIVITY)
    def historical_metrics_data(self):
        """Return cluster activity historical metrics."""
        start_date = _safe_parse_datetime(request.args.get("start_date"))
        end_date = _safe_parse_datetime(request.args.get("end_date"))

        with create_session() as session:
            # DagRuns
            dag_run_types = session.execute(
                select(DagRun.run_type, func.count(DagRun.run_id))
                .where(
                    DagRun.start_date >= start_date,
                    func.coalesce(DagRun.end_date, timezone.utcnow()) <= end_date,
                )
                .group_by(DagRun.run_type)
            ).all()

            dag_run_states = session.execute(
                select(DagRun.state, func.count(DagRun.run_id))
                .where(
                    DagRun.start_date >= start_date,
                    func.coalesce(DagRun.end_date, timezone.utcnow()) <= end_date,
                )
                .group_by(DagRun.state)
            ).all()

            # TaskInstances
            task_instance_states = session.execute(
                select(TaskInstance.state, func.count(TaskInstance.run_id))
                .join(TaskInstance.dag_run)
                .where(
                    DagRun.start_date >= start_date,
                    func.coalesce(DagRun.end_date, timezone.utcnow()) <= end_date,
                )
                .group_by(TaskInstance.state)
            ).all()

            data = {
                "dag_run_types": {
                    **{dag_run_type.value: 0 for dag_run_type in DagRunType},
                    **dict(dag_run_types),
                },
                "dag_run_states": {
                    **{dag_run_state.value: 0 for dag_run_state in DagRunState},
                    **dict(dag_run_states),
                },
                "task_instance_states": {
                    "no_status": 0,
                    **{ti_state.value: 0 for ti_state in TaskInstanceState},
                    **{ti_state or "no_status": sum_value for ti_state, sum_value in task_instance_states},
                },
            }

        return (
            htmlsafe_json_dumps(data, separators=(",", ":"), dumps=flask.json.dumps),
            {"Content-Type": "application/json; charset=utf-8"},
        )

    @expose("/object/next_run_datasets/<string:dag_id>")
    @auth.has_access_dag("GET", DagAccessEntity.RUN)
    @auth.has_access_dataset("GET")
    def next_run_datasets(self, dag_id):
        """Return datasets necessary, and their status, for the next dag run."""
        dag = get_airflow_app().dag_bag.get_dag(dag_id)
        if not dag:
            return {"error": f"can't find dag {dag_id}"}, 404

        with create_session() as session:
            dag_model = DagModel.get_dagmodel(dag_id, session=session)

            latest_run = dag_model.get_last_dagrun(session=session)

            events = [
                dict(info._mapping)
                for info in session.execute(
                    select(
                        DatasetModel.id,
                        DatasetModel.uri,
                        func.max(DatasetEvent.timestamp).label("lastUpdate"),
                    )
                    .join(
                        DagScheduleDatasetReference, DagScheduleDatasetReference.dataset_id == DatasetModel.id
                    )
                    .join(
                        DatasetDagRunQueue,
                        and_(
                            DatasetDagRunQueue.dataset_id == DatasetModel.id,
                            DatasetDagRunQueue.target_dag_id == DagScheduleDatasetReference.dag_id,
                        ),
                        isouter=True,
                    )
                    .join(
                        DatasetEvent,
                        and_(
                            DatasetEvent.dataset_id == DatasetModel.id,
                            (
                                DatasetEvent.timestamp >= latest_run.execution_date
                                if latest_run and latest_run.execution_date
                                else True
                            ),
                        ),
                        isouter=True,
                    )
                    .where(DagScheduleDatasetReference.dag_id == dag_id, ~DatasetModel.is_orphaned)
                    .group_by(DatasetModel.id, DatasetModel.uri)
                    .order_by(DatasetModel.uri)
                )
            ]
            data = {"dataset_expression": dag_model.dataset_expression, "events": events}
        return (
            htmlsafe_json_dumps(data, separators=(",", ":"), dumps=flask.json.dumps),
            {"Content-Type": "application/json; charset=utf-8"},
        )

    @expose("/object/dataset_dependencies")
    @auth.has_access_dag("GET", DagAccessEntity.DEPENDENCIES)
    def dataset_dependencies(self):
        """Return dataset dependencies graph."""
        nodes_dict: dict[str, Any] = {}
        edge_tuples: set[dict[str, str]] = set()

        for dag, dependencies in SerializedDagModel.get_dag_dependencies().items():
            dag_node_id = f"dag:{dag}"
            if dag_node_id not in nodes_dict:
                for dep in dependencies:
                    if dep.dependency_type in ("dag", "dataset", "dataset-alias"):
                        # add node
                        nodes_dict[dag_node_id] = node_dict(dag_node_id, dag, "dag")
                        if dep.node_id not in nodes_dict:
                            nodes_dict[dep.node_id] = node_dict(
                                dep.node_id, dep.dependency_id, dep.dependency_type
                            )

                        # add edge

                        # not start dep
                        if dep.source != dep.dependency_type:
                            source = dep.source if ":" in dep.source else f"dag:{dep.source}"
                            target = dep.node_id
                            edge_tuples.add((source, target))

                        # not end dep
                        if dep.target != dep.dependency_type:
                            source = dep.node_id
                            target = dep.target if ":" in dep.target else f"dag:{dep.target}"
                            edge_tuples.add((source, target))

        nodes = list(nodes_dict.values())
        edges = [{"source": source, "target": target} for source, target in edge_tuples]

        data = {
            "nodes": nodes,
            "edges": edges,
        }

        return (
            htmlsafe_json_dumps(data, separators=(",", ":"), dumps=flask.json.dumps),
            {"Content-Type": "application/json; charset=utf-8"},
        )

    @expose("/object/datasets_summary")
    @auth.has_access_dataset("GET")
    def datasets_summary(self):
        """
        Get a summary of datasets.

        Includes the datetime they were last updated and how many updates they've ever had.
        """
        allowed_attrs = ["uri", "last_dataset_update"]

        # Grab query parameters
        limit = int(request.args.get("limit", 25))
        offset = int(request.args.get("offset", 0))
        order_by = request.args.get("order_by", "uri")
        uri_pattern = request.args.get("uri_pattern", "")
        lstripped_orderby = order_by.lstrip("-")
        updated_after = _safe_parse_datetime(request.args.get("updated_after"), allow_empty=True)
        updated_before = _safe_parse_datetime(request.args.get("updated_before"), allow_empty=True)

        # Check and clean up query parameters
        limit = min(50, limit)

        uri_pattern = uri_pattern[:4000]

        if lstripped_orderby not in allowed_attrs:
            return {
                "detail": (
                    f"Ordering with '{lstripped_orderby}' is disallowed or the attribute does not "
                    "exist on the model"
                )
            }, 400

        with create_session() as session:
            if lstripped_orderby == "uri":
                if order_by.startswith("-"):
                    order_by = (DatasetModel.uri.desc(),)
                else:
                    order_by = (DatasetModel.uri.asc(),)
            elif lstripped_orderby == "last_dataset_update":
                if order_by.startswith("-"):
                    order_by = (
                        func.max(DatasetEvent.timestamp).desc(),
                        DatasetModel.uri.asc(),
                    )
                    if session.bind.dialect.name == "postgresql":
                        order_by = (order_by[0].nulls_last(), *order_by[1:])
                else:
                    order_by = (
                        func.max(DatasetEvent.timestamp).asc(),
                        DatasetModel.uri.desc(),
                    )
                    if session.bind.dialect.name == "postgresql":
                        order_by = (order_by[0].nulls_first(), *order_by[1:])

            count_query = select(func.count(DatasetModel.id))

            has_event_filters = bool(updated_before or updated_after)

            query = (
                select(
                    DatasetModel.id,
                    DatasetModel.uri,
                    func.max(DatasetEvent.timestamp).label("last_dataset_update"),
                    func.sum(case((DatasetEvent.id.is_not(None), 1), else_=0)).label("total_updates"),
                )
                .join(DatasetEvent, DatasetEvent.dataset_id == DatasetModel.id, isouter=not has_event_filters)
                .group_by(
                    DatasetModel.id,
                    DatasetModel.uri,
                )
                .order_by(*order_by)
            )

            if has_event_filters:
                count_query = count_query.join(DatasetEvent, DatasetEvent.dataset_id == DatasetModel.id)

            filters = [~DatasetModel.is_orphaned]
            if uri_pattern:
                filters.append(DatasetModel.uri.ilike(f"%{uri_pattern}%"))
            if updated_after:
                filters.append(DatasetEvent.timestamp >= updated_after)
            if updated_before:
                filters.append(DatasetEvent.timestamp <= updated_before)

            query = query.where(*filters).offset(offset).limit(limit)
            count_query = count_query.where(*filters)

            query = session.execute(query)
            datasets = [dict(dataset._mapping) for dataset in query]
            data = {"datasets": datasets, "total_entries": session.scalar(count_query)}

            return (
                htmlsafe_json_dumps(data, separators=(",", ":"), cls=utils_json.WebEncoder),
                {"Content-Type": "application/json; charset=utf-8"},
            )

    @expose("/robots.txt")
    @action_logging
    def robots(self):
        """
        Return a robots.txt file for blocking certain search engine crawlers.

        This mitigates some of the risk associated with exposing Airflow to the public
        internet, however it does not address the real security risks associated with
        such a deployment.
        """
        return send_from_directory(get_airflow_app().static_folder, "robots.txt")

    @expose("/audit_log")
    def legacy_audit_log(self):
        """Redirect from url param."""
        return redirect(url_for("Airflow.audit_log", **sanitize_args(request.args)))

    @expose("/dags/<string:dag_id>/audit_log")
    def audit_log(self, dag_id: str):
        current_page = request.args.get("page")
        arg_sorting_key = request.args.get("sorting_key")
        arg_sorting_direction = request.args.get("sorting_direction")
        sort_args = {
            "offset": current_page,
            f"sort.{arg_sorting_key}": arg_sorting_direction,
            "limit": PAGE_SIZE,
        }
        kwargs = {
            **sanitize_args(sort_args),
            "dag_id": dag_id,
            "tab": "audit_log",
        }

        return redirect(url_for("Airflow.grid", **kwargs))

    @expose("/parseDagFile/<string:file_token>")
    def parse_dag(self, file_token: str):
        from airflow.api_connexion.endpoints.dag_parsing import reparse_dag_file

        with create_session() as session:
            response = reparse_dag_file(file_token=file_token, session=session)
            response_messages = {
                201: ["Reparsing request submitted successfully", "info"],
                401: ["Unauthenticated request", "error"],
                403: ["Permission Denied", "error"],
                404: ["DAG not found", "error"],
            }
            flash(response_messages[response.status_code][0], response_messages[response.status_code][1])
        redirect_url = get_safe_url(request.values.get("redirect_url"))
        return redirect(redirect_url)


class ConfigurationView(AirflowBaseView):
    """View to show Airflow Configurations."""

    default_view = "conf"

    class_permission_name = permissions.RESOURCE_CONFIG
    base_permissions = [
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    @expose("/configuration")
    @auth.has_access_configuration("GET")
    def conf(self):
        """Show configuration."""
        raw = request.args.get("raw") == "true"
        title = "Airflow Configuration"
        expose_config = conf.get("webserver", "expose_config").lower()

        # TODO remove "if raw" usage in Airflow 3.0. Configuration can be fetched via the REST API.
        if raw:
            if expose_config == "non-sensitive-only":
                updater = configupdater.ConfigUpdater()
                updater.read(AIRFLOW_CONFIG)
                for sect, key in conf.sensitive_config_values:
                    if updater.has_option(sect, key):
                        updater[sect][key].value = "< hidden >"
                config = str(updater)
            elif expose_config in {"true", "t", "1"}:
                with open(AIRFLOW_CONFIG) as file:
                    config = file.read()
            else:
                config = (
                    "# Your Airflow administrator chose not to expose the configuration, "
                    "most likely for security reasons."
                )

            return Response(
                response=config,
                status=200,
                mimetype="application/text",
                headers={"Deprecation": "Endpoint will be removed in Airflow 3.0, use the REST API instead."},
            )

        if expose_config in {"non-sensitive-only", "true", "t", "1"}:
            display_sensitive = expose_config != "non-sensitive-only"

            table = [
                (section, key, str(value), source)
                for section, parameters in conf.as_dict(True, display_sensitive).items()
                for key, (value, source) in parameters.items()
            ]

            return self.render_template(
                template="airflow/config.html",
                title=title,
                table=table,
            )

        else:
            return self.render_template(
                "airflow/config.html",
                title=title,
                hide_config_msg=(
                    "Your Airflow administrator chose not to expose the configuration, "
                    "most likely for security reasons."
                ),
            )


class RedocView(AirflowBaseView):
    """Redoc Open API documentation."""

    default_view = "redoc"

    @expose("/redoc")
    def redoc(self):
        """Redoc API documentation."""
        openapi_spec_url = url_for("/api/v1./api/v1_openapi_yaml")
        return self.render_template("airflow/redoc.html", openapi_spec_url=openapi_spec_url)


######################################################################################
#                                    ModelViews
######################################################################################


class DagFilter(BaseFilter):
    """Filter using DagIDs."""

    def apply(self, query, func):
        if get_auth_manager().is_authorized_dag(method="GET", user=g.user):
            return query
        if get_auth_manager().is_authorized_dag(method="PUT", user=g.user):
            return query
        filter_dag_ids = get_auth_manager().get_permitted_dag_ids(user=g.user)
        return query.where(self.model.dag_id.in_(filter_dag_ids))


class AirflowModelView(ModelView):
    """
    Airflow Model View.

    Overridden `__getattribute__` to wraps REST methods with action_logger
    """

    list_widget = AirflowModelListWidget
    page_size = PAGE_SIZE

    CustomSQLAInterface = wwwutils.CustomSQLAInterface

    def __getattribute__(self, attr):
        """
        Wrap action REST methods with `action_logging` wrapper.

        Overriding enables differentiating resource and generation of event name at the decorator level.

        if attr in ["show", "list", "read", "get", "get_list"]:
            return action_logging(event="RESOURCE_NAME"."action_name")(attr)
        else:
            return attr
        """
        attribute = object.__getattribute__(self, attr)
        if (
            callable(attribute)
            and hasattr(attribute, "_permission_name")
            and attribute._permission_name in self.method_permission_name
        ):
            permission_str = self.method_permission_name[attribute._permission_name]
            if permission_str not in ["show", "list", "read", "get", "get_list"]:
                return action_logging(event=f"{self.route_base.strip('/')}.{permission_str}")(attribute)
        return attribute

    @expose("/show/<pk>", methods=["GET"])
    @auth.has_access_with_pk
    def show(self, pk):
        """
        Show view.

        Same implementation as
        https://github.com/dpgaspar/Flask-AppBuilder/blob/1c3af9b665ed9a3daf36673fee3327d0abf43e5b/flask_appbuilder/views.py#L566

        Override it to use a custom ``has_access_with_pk`` decorator to take into consideration resource for
        fined-grained access.
        """
        pk = self._deserialize_pk_if_composite(pk)
        widgets = self._show(pk)
        return self.render_template(
            self.show_template,
            pk=pk,
            title=self.show_title,
            widgets=widgets,
            related_views=self._related_views,
        )

    @expose("/edit/<pk>", methods=["GET", "POST"])
    @auth.has_access_with_pk
    def edit(self, pk):
        """
        Edit view.

        Same implementation as
        https://github.com/dpgaspar/Flask-AppBuilder/blob/1c3af9b665ed9a3daf36673fee3327d0abf43e5b/flask_appbuilder/views.py#L602

        Override it to use a custom ``has_access_with_pk`` decorator to take into consideration resource for
        fined-grained access.
        """
        pk = self._deserialize_pk_if_composite(pk)
        widgets = self._edit(pk)
        if not widgets:
            return self.post_edit_redirect()
        else:
            return self.render_template(
                self.edit_template,
                title=self.edit_title,
                widgets=widgets,
                related_views=self._related_views,
            )

    @expose("/delete/<pk>", methods=["GET", "POST"])
    @auth.has_access_with_pk
    def delete(self, pk):
        """
        Delete view.

        Same implementation as
        https://github.com/dpgaspar/Flask-AppBuilder/blob/1c3af9b665ed9a3daf36673fee3327d0abf43e5b/flask_appbuilder/views.py#L623

        Override it to use a custom ``has_access_with_pk`` decorator to take into consideration resource for
        fined-grained access.
        """
        # Maintains compatibility but refuses to delete on GET methods if CSRF is enabled
        if not self.is_get_mutation_allowed():
            self.update_redirect()
            logger.warning("CSRF is enabled and a delete using GET was invoked")
            flash(as_unicode(FLAMSG_ERR_SEC_ACCESS_DENIED), "danger")
            return self.post_delete_redirect()
        pk = self._deserialize_pk_if_composite(pk)
        self._delete(pk)
        return self.post_delete_redirect()

    @expose("/action_post", methods=["POST"])
    def action_post(self):
        """
        Handle multiple records selected from a list view.

        Same implementation as
        https://github.com/dpgaspar/Flask-AppBuilder/blob/2c5763371b81cd679d88b9971ba5d1fc4d71d54b/flask_appbuilder/views.py#L677

        The difference is, it no longer check permissions with ``self.appbuilder.sm.has_access``,
        it executes the function without verifying permissions.
        Thus, each action need to be annotated individually with ``@auth.has_access_*`` to check user
        permissions.
        """
        name = request.form["action"]
        pks = request.form.getlist("rowid")
        action = self.actions.get(name)
        items = [self.datamodel.get(self._deserialize_pk_if_composite(pk)) for pk in pks]
        return action.func(items)


class SlaMissModelView(AirflowModelView):
    """View to show SlaMiss table."""

    route_base = "/slamiss"

    datamodel = AirflowModelView.CustomSQLAInterface(SlaMiss)  # type: ignore

    class_permission_name = permissions.RESOURCE_SLA_MISS
    method_permission_name = {
        "list": "read",
        "action_muldelete": "delete",
        "action_mulnotificationsent": "edit",
        "action_mulnotificationsentfalse": "edit",
        "action_mulemailsent": "edit",
        "action_mulemailsentfalse": "edit",
    }

    base_permissions = [
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    list_columns = ["dag_id", "task_id", "execution_date", "email_sent", "notification_sent", "timestamp"]

    label_columns = {
        "execution_date": "Logical Date",
    }

    add_columns = ["dag_id", "task_id", "execution_date", "email_sent", "notification_sent", "timestamp"]
    edit_columns = ["dag_id", "task_id", "execution_date", "email_sent", "notification_sent", "timestamp"]
    search_columns = ["dag_id", "task_id", "email_sent", "notification_sent", "timestamp", "execution_date"]
    base_order = ("execution_date", "desc")
    base_filters = [["dag_id", DagFilter, list]]

    formatters_columns = {
        "task_id": wwwutils.task_instance_link,
        "execution_date": wwwutils.datetime_f("execution_date"),
        "timestamp": wwwutils.datetime_f("timestamp"),
        "dag_id": wwwutils.dag_link,
        "map_index": wwwutils.format_map_index,
    }

    @action("muldelete", "Delete", "Are you sure you want to delete selected records?", single=False)
    @auth.has_access_dag_entities("DELETE", DagAccessEntity.SLA_MISS)
    def action_muldelete(self, items):
        """Multiple delete action."""
        self.datamodel.delete_all(items)
        self.update_redirect()
        return redirect(self.get_redirect())

    @action(
        "mulnotificationsent",
        "Set notification sent to true",
        "Are you sure you want to set all these notifications to sent?",
        single=False,
    )
    @auth.has_access_dag_entities("PUT", DagAccessEntity.SLA_MISS)
    def action_mulnotificationsent(self, items: list[SlaMiss]):
        return self._set_notification_property(items, "notification_sent", True)

    @action(
        "mulnotificationsentfalse",
        "Set notification sent to false",
        "Are you sure you want to mark these SLA alerts as notification not sent yet?",
        single=False,
    )
    @auth.has_access_dag_entities("PUT", DagAccessEntity.SLA_MISS)
    def action_mulnotificationsentfalse(self, items: list[SlaMiss]):
        return self._set_notification_property(items, "notification_sent", False)

    @action(
        "mulemailsent",
        "Set email sent to true",
        "Are you sure you want to mark these SLA alerts as emails were sent?",
        single=False,
    )
    @auth.has_access_dag_entities("PUT", DagAccessEntity.SLA_MISS)
    def action_mulemailsent(self, items: list[SlaMiss]):
        return self._set_notification_property(items, "email_sent", True)

    @action(
        "mulemailsentfalse",
        "Set email sent to false",
        "Are you sure you want to mark these SLA alerts as emails not sent yet?",
        single=False,
    )
    @auth.has_access_dag_entities("PUT", DagAccessEntity.SLA_MISS)
    def action_mulemailsentfalse(self, items: list[SlaMiss]):
        return self._set_notification_property(items, "email_sent", False)

    @provide_session
    def _set_notification_property(
        self,
        items: list[SlaMiss],
        attr: str,
        new_value: bool,
        session: Session = NEW_SESSION,
    ):
        try:
            count = 0
            for sla in items:
                count += 1
                setattr(sla, attr, new_value)
                session.merge(sla)
            session.commit()
            flash(f"{count} SLAMisses had {attr} set to {new_value}.")
        except Exception as ex:
            flash(str(ex), "error")
            flash("Failed to set state", "error")
        self.update_redirect()
        return redirect(self.get_default_url())


class XComModelView(AirflowModelView):
    """View to show records from XCom table."""

    route_base = "/xcom"

    list_title = "List XComs"

    datamodel = AirflowModelView.CustomSQLAInterface(XCom)

    class_permission_name = permissions.RESOURCE_XCOM
    method_permission_name = {
        "list": "read",
        "delete": "delete",
        "action_muldelete": "delete",
    }
    base_permissions = [
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_DELETE,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    search_columns = ["key", "value", "timestamp", "dag_id", "task_id", "run_id", "execution_date"]
    list_columns = ["key", "value", "timestamp", "dag_id", "task_id", "run_id", "map_index", "execution_date"]
    base_order = ("dag_run_id", "desc")

    order_columns = [
        "key",
        "value",
        "timestamp",
        "dag_id",
        "task_id",
        "run_id",
        "map_index",
        # "execution_date", # execution_date sorting is not working and crashing the UI, disabled for now.
    ]

    base_filters = [["dag_id", DagFilter, list]]

    formatters_columns = {
        "task_id": wwwutils.task_instance_link,
        "timestamp": wwwutils.datetime_f("timestamp"),
        "dag_id": wwwutils.dag_link,
        "map_index": wwwutils.format_map_index,
        "execution_date": wwwutils.datetime_f("execution_date"),
    }

    @action("muldelete", "Delete", "Are you sure you want to delete selected records?", single=False)
    @auth.has_access_dag_entities("DELETE", DagAccessEntity.XCOM)
    def action_muldelete(self, items):
        """Multiple delete action."""
        self.datamodel.delete_all(items)
        self.update_redirect()
        return redirect(self.get_redirect())

    def pre_add(self, item):
        """Pre add hook."""
        item.execution_date = timezone.make_aware(item.execution_date)
        item.value = XCom.serialize_value(
            value=item.value,
            key=item.key,
            task_id=item.task_id,
            dag_id=item.dag_id,
            run_id=item.run_id,
            map_index=item.map_index,
        )

    def pre_update(self, item):
        """Pre update hook."""
        item.execution_date = timezone.make_aware(item.execution_date)
        item.value = XCom.serialize_value(
            value=item.value,
            key=item.key,
            task_id=item.task_id,
            dag_id=item.dag_id,
            run_id=item.run_id,
            map_index=item.map_index,
        )


# Used to store a dictionary of field behaviours used to dynamically change available
# fields in ConnectionForm based on type of connection chosen
# See airflow.hooks.base_hook.DiscoverableHook for details on how to customize your Hooks.
#
# Additionally, a list of connection types that support testing via Airflow REST API is stored to dynamically
# enable/disable the Test Connection button.
#
# These field behaviours and testable connection types are rendered as scripts in the conn_create.html and
# conn_edit.html templates.
class ConnectionFormWidget(FormWidget):
    """Form widget used to display connection."""

    @cached_property
    def field_behaviours(self) -> str:
        return json.dumps(ProvidersManager().field_behaviours)

    @cached_property
    def testable_connection_types(self) -> list[str]:
        return [
            connection_type
            for connection_type, hook_info in ProvidersManager().hooks.items()
            if hook_info and hook_info.connection_testable
        ]


class ConnectionFormProxy:
    """
    A stand-in for the connection form class.

    Flask-Appbuilder model views only ever call the ``refresh()`` function on
    the form class, so this is the perfect place to make the form generation
    dynamic. See docstring of ``create_connection_form_class`` for rationales.
    """

    @staticmethod
    def refresh(obj=None):
        return create_connection_form_class().refresh(obj)


class ConnectionModelView(AirflowModelView):
    """View to show records from Connections table."""

    route_base = "/connection"

    datamodel = AirflowModelView.CustomSQLAInterface(Connection)  # type: ignore

    class_permission_name = permissions.RESOURCE_CONNECTION
    method_permission_name = {
        "add": "create",
        "list": "read",
        "edit": "edit",
        "delete": "delete",
        "action_muldelete": "delete",
        "action_mulduplicate": "create",
    }

    base_permissions = [
        permissions.ACTION_CAN_CREATE,
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_DELETE,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    list_columns = [
        "conn_id",
        "conn_type",
        "description",
        "host",
        "port",
        "is_encrypted",
        "is_extra_encrypted",
    ]

    # The real add_columns and edit_columns are dynamically generated at runtime
    # so we can delay calculating entries relying on providers to make webserver
    # start up faster.
    _add_columns = _edit_columns = [
        "conn_id",
        "conn_type",
        "description",
        "host",
        "schema",
        "login",
        "password",
        "port",
        "extra",
    ]

    # We will generate the actual ConnectionForm when it is actually needed,
    # i.e. when the web form views are displayed and submitted.
    add_form = edit_form = ConnectionFormProxy

    add_template = "airflow/conn_create.html"
    edit_template = "airflow/conn_edit.html"

    add_widget = ConnectionFormWidget
    edit_widget = ConnectionFormWidget

    base_order = ("conn_id", "asc")

    def _iter_extra_field_names_and_sensitivity(self) -> Iterator[tuple[str, str, bool]]:
        """
        Iterate through provider-backed connection fields.

        Note that this cannot be a property (including a cached property)
        because Flask-Appbuilder attempts to access all members on startup, and
        using a property would initialize the providers manager too eagerly.

        Returns tuple of:

        * key
        * field_name
        * whether the field is sensitive
        """
        return (
            (k, v.field_name, v.is_sensitive) for k, v in ProvidersManager().connection_form_widgets.items()
        )

    @property
    def add_columns(self) -> list[str]:
        """
        A list of columns to show in the Add form.

        This dynamically calculates additional fields from providers and add
        them to the backing list. This calculation is done exactly once (by
        checking we're referencing the class-level variable instead of the
        instance-level), and only after we enter the request context (to skip
        superfuluous checks done by Flask-Appbuilder on startup).
        """
        if self._add_columns is type(self)._add_columns and has_request_context():
            self._add_columns = [
                *self._add_columns,
                *(k for k, _, _ in self._iter_extra_field_names_and_sensitivity()),
            ]
        return self._add_columns

    @property
    def edit_columns(self) -> list[str]:
        """
        A list of columns to show in the Edit form.

        This dynamically calculates additional fields from providers and add
        them to the backing list. This calculation is done exactly once (by
        checking we're referencing the class-level variable instead of the
        instance-level), and only after we enter the request context (to skip
        superfuluous checks done by Flask-Appbuilder on startup).
        """
        if self._edit_columns is type(self)._edit_columns and has_request_context():
            self._edit_columns = [
                *self._edit_columns,
                *(k for k, _, _ in self._iter_extra_field_names_and_sensitivity()),
            ]
        return self._edit_columns

    @action("muldelete", "Delete", "Are you sure you want to delete selected records?", single=False)
    @auth.has_access_connection("DELETE")
    def action_muldelete(self, connections):
        """Multiple delete."""
        self.datamodel.delete_all(connections)
        self.update_redirect()
        return redirect(self.get_redirect())

    @action(
        "mulduplicate",
        "Duplicate",
        "Are you sure you want to duplicate the selected connections?",
        single=False,
    )
    @provide_session
    @auth.has_access_connection("POST")
    @auth.has_access_connection("GET")
    def action_mulduplicate(self, connections, session: Session = NEW_SESSION):
        """Duplicate Multiple connections."""
        for selected_conn in connections:
            new_conn_id = selected_conn.conn_id
            match = re2.search(r"_copy(\d+)$", selected_conn.conn_id)

            base_conn_id = selected_conn.conn_id
            if match:
                base_conn_id = base_conn_id.split("_copy")[0]

            potential_connection_ids = [f"{base_conn_id}_copy{i}" for i in range(1, 11)]

            query = session.scalars(
                select(Connection.conn_id).where(Connection.conn_id.in_(potential_connection_ids))
            )

            found_conn_id_set = set(query)

            possible_conn_id_iter = (
                connection_id
                for connection_id in potential_connection_ids
                if connection_id not in found_conn_id_set
            )
            try:
                new_conn_id = next(possible_conn_id_iter)
            except StopIteration:
                flash(
                    f"Connection {new_conn_id} can't be added because it already exists, "
                    f"Please rename the existing connections",
                    "warning",
                )
            else:
                dup_conn = Connection(
                    new_conn_id,
                    selected_conn.conn_type,
                    selected_conn.description,
                    selected_conn.host,
                    selected_conn.login,
                    selected_conn.password,
                    selected_conn.schema,
                    selected_conn.port,
                    selected_conn.extra,
                )

                try:
                    session.add(dup_conn)
                    session.commit()
                    flash(f"Connection {new_conn_id} added successfully.", "success")
                except IntegrityError:
                    flash(
                        f"Connection {new_conn_id} can't be added. Integrity error, "
                        f"probably unique constraint.",
                        "warning",
                    )
                    session.rollback()

        self.update_redirect()
        return redirect(self.get_redirect())

    def process_form(self, form, is_created):
        """Process form data."""
        conn_id = form.data["conn_id"]
        conn_type = form.data["conn_type"]
        # The extra value is the combination of custom fields for this conn_type and the Extra field.
        # The extra form field with all extra values (including custom fields) is in the form being processed
        # so we start with those values, and override them with anything in the custom fields.
        extra = {}

        extra_json = form.data.get("extra")

        if extra_json:
            try:
                extra.update(json.loads(extra_json))
            except (JSONDecodeError, TypeError):
                flash(
                    Markup(
                        "<p>The <em>Extra</em> connection field contained an invalid value for Conn ID: "
                        "<q>{conn_id}</q>.</p>"
                        "<p>If connection parameters need to be added to <em>Extra</em>, "
                        "please make sure they are in the form of a single, valid JSON object.</p><br>"
                        "The following <em>Extra</em> parameters were <b>not</b> added to the connection:<br>"
                        "{extra_json}"
                    ).format(conn_id=conn_id, extra_json=extra_json),
                    category="error",
                )
                del form.extra
        del extra_json
        for key, field_name, _ in self._iter_extra_field_names_and_sensitivity():
            if key in form.data and key.startswith("extra__"):
                conn_type_from_extra_field = key.split("__")[1]
                if conn_type_from_extra_field == conn_type:
                    value = form.data[key]
                    # Some extra fields have a default value of False so we need to explicitly check the
                    # value isn't an empty string.
                    if value != "":
                        extra[field_name] = value
                    elif field_name in extra:
                        del extra[field_name]
        if extra.keys():
            sensitive_unchanged_keys = set()
            for key, value in extra.items():
                if value == SENSITIVE_FIELD_PLACEHOLDER:
                    sensitive_unchanged_keys.add(key)
            if sensitive_unchanged_keys:
                try:
                    conn = BaseHook.get_connection(conn_id)
                except AirflowNotFoundException:
                    conn = None
                for key in sensitive_unchanged_keys:
                    if conn and conn.extra_dejson.get(key):
                        extra[key] = conn.extra_dejson.get(key)
                    else:
                        del extra[key]
            form.extra.data = json.dumps(extra)

    def prefill_form(self, form, pk):
        """Prefill the form."""
        try:
            extra = form.data.get("extra")
            if extra is None:
                extra_dictionary = {}
            else:
                extra_dictionary = json.loads(extra)
        except JSONDecodeError:
            extra_dictionary = {}

        if not isinstance(extra_dictionary, dict):
            logger.warning("extra field for %s is not a dictionary", form.data.get("conn_id", "<unknown>"))
            return

        for field_key, field_name, is_sensitive in self._iter_extra_field_names_and_sensitivity():
            value = extra_dictionary.get(field_name, "")

            if not value:
                # check if connection `extra` json is using old prefixed field name style
                value = extra_dictionary.get(field_key, "")

            if value:
                field = getattr(form, field_key)
                field.data = value
            if is_sensitive and field_name in extra_dictionary:
                extra_dictionary[field_name] = SENSITIVE_FIELD_PLACEHOLDER
        # form.data is a property that builds the dictionary from fields so we have to modify the fields
        if extra_dictionary:
            form.extra.data = json.dumps(extra_dictionary)
        else:
            form.extra.data = None


class PluginView(AirflowBaseView):
    """View to show Airflow Plugins."""

    default_view = "list"

    class_permission_name = permissions.RESOURCE_PLUGIN

    method_permission_name = {
        "list": "read",
    }

    base_permissions = [
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    plugins_attributes_to_dump = PLUGINS_ATTRIBUTES_TO_DUMP

    @expose("/plugin")
    @auth.has_access_view(AccessView.PLUGINS)
    def list(self):
        """List loaded plugins."""
        plugins_manager.ensure_plugins_loaded()
        plugins_manager.integrate_executor_plugins()
        plugins_manager.initialize_extra_operators_links_plugins()
        plugins_manager.initialize_web_ui_plugins()

        plugins = []
        for plugin_no, plugin in enumerate(plugins_manager.plugins, 1):
            plugin_data = {
                "plugin_no": plugin_no,
                "plugin_name": plugin.name,
                "attrs": {},
            }
            for attr_name in self.plugins_attributes_to_dump:
                attr_value = getattr(plugin, attr_name)
                plugin_data["attrs"][attr_name] = attr_value

            plugins.append(plugin_data)

        title = "Airflow Plugins"
        doc_url = get_docs_url("plugins.html")
        return self.render_template(
            "airflow/plugin.html",
            plugins=plugins,
            title=title,
            doc_url=doc_url,
        )


class ProviderView(AirflowBaseView):
    """View to show Airflow Providers."""

    default_view = "list"

    class_permission_name = permissions.RESOURCE_PROVIDER

    method_permission_name = {
        "list": "read",
    }

    base_permissions = [
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    @expose("/provider")
    @auth.has_access_view(AccessView.PROVIDERS)
    def list(self):
        """List providers."""
        providers_manager = ProvidersManager()

        providers = []
        for pi in providers_manager.providers.values():
            provider_info = pi.data
            provider_data = {
                "package_name": provider_info["package-name"],
                "description": self._clean_description(provider_info["description"]),
                "version": pi.version,
                "documentation_url": get_doc_url_for_provider(provider_info["package-name"], pi.version),
            }
            providers.append(provider_data)

        title = "Providers"
        doc_url = get_docs_url("apache-airflow-providers/index.html")
        return self.render_template(
            "airflow/providers.html",
            providers=providers,
            title=title,
            doc_url=doc_url,
        )

    def _clean_description(self, description):
        def _build_link(match_obj):
            text = match_obj.group(1)
            url = match_obj.group(2)

            # parsing the url to check if it's a valid url
            parsed_url = urlparse(url)
            if not (parsed_url.scheme == "http" or parsed_url.scheme == "https"):
                # returning the original raw text
                return escape(match_obj.group(0))

            return Markup(f'<a href="{url}" target="_blank" rel="noopener noreferrer">{text}</a>')

        cd = escape(description)
        cd = re2.sub(r"`(.*)[\s+]+&lt;(.*)&gt;`__", _build_link, cd)
        return Markup(cd)


class PoolModelView(AirflowModelView):
    """View to show records from Pool table."""

    route_base = "/pool"

    list_template = "airflow/pool_list.html"

    datamodel = AirflowModelView.CustomSQLAInterface(models.Pool)  # type: ignore

    class_permission_name = permissions.RESOURCE_POOL
    method_permission_name = {
        "add": "create",
        "list": "read",
        "edit": "edit",
        "delete": "delete",
        "action_muldelete": "delete",
    }

    base_permissions = [
        permissions.ACTION_CAN_CREATE,
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_DELETE,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    list_columns = [
        "pool",
        "description",
        "slots",
        "running_slots",
        "queued_slots",
        "scheduled_slots",
        "deferred_slots",
    ]
    add_columns = ["pool", "slots", "description", "include_deferred"]
    edit_columns = ["pool", "slots", "description", "include_deferred"]

    # include_deferred is non-nullable, but as a checkbox in the resulting form we want to allow it unchecked
    include_deferred_field = BooleanField(
        validators=[validators.Optional()],
        description="Check to include deferred tasks when calculating open pool slots.",
    )
    edit_form_extra_fields = {"include_deferred": include_deferred_field}
    add_form_extra_fields = {"include_deferred": include_deferred_field}

    base_order = ("pool", "asc")

    @action("muldelete", "Delete", "Are you sure you want to delete selected records?", single=False)
    @auth.has_access_pool("DELETE")
    def action_muldelete(self, items):
        """Multiple delete."""
        if any(item.pool == models.Pool.DEFAULT_POOL_NAME for item in items):
            flash(f"{models.Pool.DEFAULT_POOL_NAME} cannot be deleted", "error")
            self.update_redirect()
            return redirect(self.get_redirect())
        self.datamodel.delete_all(items)
        self.update_redirect()
        return redirect(self.get_redirect())

    @expose("/delete/<pk>", methods=["GET", "POST"])
    @auth.has_access_with_pk
    def delete(self, pk):
        """Single delete."""
        if models.Pool.is_default_pool(pk):
            flash(f"{models.Pool.DEFAULT_POOL_NAME} cannot be deleted", "error")
            self.update_redirect()
            return redirect(self.get_redirect())

        return super().delete(pk)

    def pool_link(self):
        """Pool link rendering."""
        pool_id = self.get("pool")
        if pool_id is not None:
            url = url_for("TaskInstanceModelView.list", _flt_3_pool=pool_id)
            return Markup("<a href='{url}'>{pool_id}</a>").format(url=url, pool_id=pool_id)
        else:
            return Markup('<span class="label label-danger">Invalid</span>')

    def frunning_slots(self):
        """Format running slots rendering."""
        pool_id = self.get("pool")
        running_slots = self.get("running_slots")
        if pool_id is not None and running_slots is not None:
            url = url_for("TaskInstanceModelView.list", _flt_3_pool=pool_id, _flt_3_state="running")
            return Markup("<a href='{url}'>{running_slots}</a>").format(url=url, running_slots=running_slots)
        else:
            return Markup('<span class="label label-danger">Invalid</span>')

    def fqueued_slots(self):
        """Queued slots rendering."""
        pool_id = self.get("pool")
        queued_slots = self.get("queued_slots")
        if pool_id is not None and queued_slots is not None:
            url = url_for("TaskInstanceModelView.list", _flt_3_pool=pool_id, _flt_3_state="queued")
            return Markup("<a href='{url}'>{queued_slots}</a>").format(url=url, queued_slots=queued_slots)
        else:
            return Markup('<span class="label label-danger">Invalid</span>')

    def fscheduled_slots(self):
        """Scheduled slots rendering."""
        pool_id = self.get("pool")
        scheduled_slots = self.get("scheduled_slots")
        if pool_id is not None and scheduled_slots is not None:
            url = url_for("TaskInstanceModelView.list", _flt_3_pool=pool_id, _flt_3_state="scheduled")
            return Markup("<a href='{url}'>{scheduled_slots}</a>").format(
                url=url, scheduled_slots=scheduled_slots
            )
        else:
            return Markup('<span class="label label-danger">Invalid</span>')

    def fdeferred_slots(self):
        """Deferred slots rendering."""
        pool_id = self.get("pool")
        deferred_slots = self.get("deferred_slots")
        if pool_id is not None and deferred_slots is not None:
            url = url_for("TaskInstanceModelView.list", _flt_3_pool=pool_id, _flt_3_state="deferred")
            return Markup("<a href='{url}'>{deferred_slots}</a>").format(
                url=url, deferred_slots=deferred_slots
            )
        else:
            return Markup('<span class="label label-danger">Invalid</span>')

    formatters_columns = {
        "pool": pool_link,
        "running_slots": frunning_slots,
        "queued_slots": fqueued_slots,
        "scheduled_slots": fscheduled_slots,
        "deferred_slots": fdeferred_slots,
    }

    validators_columns = {"pool": [validators.DataRequired()], "slots": [validators.NumberRange(min=-1)]}


class VariableModelView(AirflowModelView):
    """View to show records from Variable table."""

    route_base = "/variable"

    list_template = "airflow/variable_list.html"
    edit_template = "airflow/variable_edit.html"
    show_template = "airflow/variable_show.html"

    show_widget = AirflowVariableShowWidget

    datamodel = AirflowModelView.CustomSQLAInterface(models.Variable)  # type: ignore

    class_permission_name = permissions.RESOURCE_VARIABLE
    method_permission_name = {
        "add": "create",
        "list": "read",
        "edit": "edit",
        "show": "read",
        "delete": "delete",
        "action_muldelete": "delete",
        "action_varexport": "read",
    }
    base_permissions = [
        permissions.ACTION_CAN_CREATE,
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_DELETE,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    list_columns = ["key", "val", "description", "is_encrypted"]
    add_columns = ["key", "val", "description"]
    edit_columns = ["key", "val", "description"]
    show_columns = ["key", "val", "description"]
    search_columns = ["key", "val"]

    base_order = ("key", "asc")

    def hidden_field_formatter(self):
        """Format hidden fields."""
        key = self.get("key")
        val = self.get("val")
        if secrets_masker.should_hide_value_for_key(key):
            return Markup("*" * 8)
        if val:
            return val
        else:
            return Markup('<span class="label label-danger">Invalid</span>')

    formatters_columns = {
        "val": hidden_field_formatter,
    }

    validators_columns = {"key": [validators.DataRequired()]}

    def prefill_form(self, form, request_id):
        if secrets_masker.should_hide_value_for_key(form.key.data):
            form.val.data = "*" * 8

    def prefill_show(self, item):
        if secrets_masker.should_hide_value_for_key(item.key):
            item.val = "*" * 8

    def _show(self, pk):
        pages = get_page_args()
        page_sizes = get_page_size_args()
        orders = get_order_args()

        item = self.datamodel.get(pk, self._base_filters)
        if not item:
            abort(404)
        self.prefill_show(item)
        widgets = self._get_show_widget(pk, item)
        self.update_redirect()

        return self._get_related_views_widgets(
            item, orders=orders, pages=pages, page_sizes=page_sizes, widgets=widgets
        )

    extra_args = {"can_create_variable": lambda: get_auth_manager().is_authorized_variable(method="POST")}

    @action("muldelete", "Delete", "Are you sure you want to delete selected records?", single=False)
    @auth.has_access_variable("DELETE")
    def action_muldelete(self, items):
        """Multiple delete."""
        self.datamodel.delete_all(items)
        self.update_redirect()
        return redirect(self.get_redirect())

    @action("varexport", "Export", "", single=False)
    @auth.has_access_variable("GET")
    def action_varexport(self, items):
        """Export variables."""
        var_dict = {}
        decoder = json.JSONDecoder()
        for var in items:
            try:
                val = decoder.decode(var.val)
            except Exception:
                val = var.val
            var_dict[var.key] = val

        response = make_response(json.dumps(var_dict, sort_keys=True, indent=4))
        response.headers["Content-Disposition"] = "attachment; filename=variables.json"
        response.headers["Content-Type"] = "application/json; charset=utf-8"
        return response

    @expose("/varimport", methods=["POST"])
    @auth.has_access_variable("POST")
    @action_logging(event=f"{permissions.RESOURCE_VARIABLE.lower()}.varimport")
    @provide_session
    def varimport(self, session):
        """Import variables."""
        try:
            variable_dict = json.loads(request.files["file"].read())
            action_on_existing = request.form.get("action_if_exists", "overwrite").lower()
        except Exception:
            self.update_redirect()
            flash("Missing file or syntax error.", "error")
            return redirect(self.get_redirect())
        else:
            existing_keys = set()
            if action_on_existing != "overwrite":
                existing_keys = set(
                    session.scalars(select(models.Variable.key).where(models.Variable.key.in_(variable_dict)))
                )
            if action_on_existing == "fail" and existing_keys:
                failed_repr = ", ".join(repr(k) for k in sorted(existing_keys))
                flash(f"Failed. The variables with these keys: {failed_repr}  already exists.")
                logger.error("Failed. The variables with these keys: %s already exists.", failed_repr)
                self.update_redirect()
                return redirect(self.get_redirect())
            skipped = set()
            suc_count = fail_count = 0
            for k, v in variable_dict.items():
                if action_on_existing == "skip" and k in existing_keys:
                    logger.warning("Variable: %s already exists, skipping.", k)
                    skipped.add(k)
                    continue
                try:
                    models.Variable.set(k, v, serialize_json=not isinstance(v, str))
                except Exception as exc:
                    logger.info("Variable import failed: %r", exc)
                    fail_count += 1
                else:
                    suc_count += 1
            flash(f"{suc_count} variable(s) successfully updated.")
            if fail_count:
                flash(f"{fail_count} variable(s) failed to be updated.", "error")
            if skipped:
                skipped_repr = ", ".join(repr(k) for k in sorted(skipped))
                flash(
                    f"The variables with these keys: {skipped_repr} were skipped "
                    "because they already exists",
                    "warning",
                )
            self.update_redirect()
            return redirect(self.get_redirect())


class JobModelView(AirflowModelView):
    """View to show records from Job table."""

    route_base = "/job"

    datamodel = AirflowModelView.CustomSQLAInterface(Job)  # type: ignore

    class_permission_name = permissions.RESOURCE_JOB
    method_permission_name = {
        "list": "read",
    }
    base_permissions = [
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    list_columns = [
        "id",
        "dag_id",
        "state",
        "job_type",
        "start_date",
        "end_date",
        "latest_heartbeat",
        "executor_class",
        "hostname",
        "unixname",
    ]
    search_columns = [
        "id",
        "dag_id",
        "state",
        "job_type",
        "start_date",
        "end_date",
        "latest_heartbeat",
        "executor_class",
        "hostname",
        "unixname",
    ]

    base_order = ("start_date", "desc")

    base_filters = [["dag_id", DagFilter, list]]

    formatters_columns = {
        "start_date": wwwutils.datetime_f("start_date"),
        "end_date": wwwutils.datetime_f("end_date"),
        "hostname": wwwutils.nobr_f("hostname"),
        "state": wwwutils.state_f,
        "latest_heartbeat": wwwutils.datetime_f("latest_heartbeat"),
    }


class DagRunModelView(AirflowModelView):
    """View to show records from DagRun table."""

    route_base = "/dagrun"

    datamodel = wwwutils.DagRunCustomSQLAInterface(models.DagRun)  # type: ignore

    class_permission_name = permissions.RESOURCE_DAG_RUN
    method_permission_name = {
        "delete": "delete",
        "edit": "edit",
        "list": "read",
        "action_clear": "edit",
        "action_muldelete": "delete",
        "action_set_queued": "edit",
        "action_set_running": "edit",
        "action_set_failed": "edit",
        "action_set_success": "edit",
    }
    base_permissions = [
        permissions.ACTION_CAN_CREATE,
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_DELETE,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    list_columns = [
        "state",
        "dag_id",
        "execution_date",
        "run_id",
        "run_type",
        "queued_at",
        "start_date",
        "end_date",
        "note",
        "external_trigger",
        "conf",
        "duration",
    ]
    search_columns = [
        "state",
        "dag_id",
        "execution_date",
        "run_id",
        "run_type",
        "start_date",
        "end_date",
        "note",
        "external_trigger",
    ]
    label_columns = {
        "execution_date": "Logical Date",
    }
    edit_columns = [
        "state",
        "dag_id",
        "execution_date",
        "start_date",
        "end_date",
        "run_id",
        "conf",
        "note",
    ]

    # duration is not a DB column, its derived
    order_columns = [
        "state",
        "dag_id",
        "execution_date",
        "run_id",
        "run_type",
        "queued_at",
        "start_date",
        "end_date",
        # "note", # todo: maybe figure out how to re-enable this
        "external_trigger",
        "conf",
    ]

    base_order = ("execution_date", "desc")

    base_filters = [["dag_id", DagFilter, list]]

    edit_form = DagRunEditForm

    def duration_f(self):
        """Duration calculation."""
        end_date = self.get("end_date")
        start_date = self.get("start_date")

        difference = "0s"
        if start_date and end_date:
            difference = td_format(end_date - start_date)

        return difference

    formatters_columns = {
        "execution_date": wwwutils.datetime_f("execution_date"),
        "state": wwwutils.state_f,
        "start_date": wwwutils.datetime_f("start_date"),
        "end_date": wwwutils.datetime_f("end_date"),
        "queued_at": wwwutils.datetime_f("queued_at"),
        "dag_id": wwwutils.dag_link,
        "run_id": wwwutils.dag_run_link,
        "conf": wwwutils.json_f("conf"),
        "duration": duration_f,
    }

    @action("muldelete", "Delete", "Are you sure you want to delete selected records?", single=False)
    @auth.has_access_dag_entities("DELETE", DagAccessEntity.RUN)
    @action_logging
    def action_muldelete(self, items: list[DagRun]):
        """Multiple delete."""
        self.datamodel.delete_all(items)
        self.update_redirect()
        return redirect(self.get_redirect())

    @action("set_queued", "Set state to 'queued'", "", single=False)
    @auth.has_access_dag_entities("PUT", DagAccessEntity.RUN)
    @action_logging
    def action_set_queued(self, drs: list[DagRun]):
        """Set state to queued."""
        return self._set_dag_runs_to_active_state(drs, DagRunState.QUEUED)

    @action("set_running", "Set state to 'running'", "", single=False)
    @auth.has_access_dag_entities("PUT", DagAccessEntity.RUN)
    @action_logging
    def action_set_running(self, drs: list[DagRun]):
        """Set state to running."""
        return self._set_dag_runs_to_active_state(drs, DagRunState.RUNNING)

    @provide_session
    def _set_dag_runs_to_active_state(
        self,
        drs: list[DagRun],
        state: DagRunState,
        session: Session = NEW_SESSION,
    ):
        """Set dag run to active state; this routine only supports Running and Queued state."""
        try:
            count = 0
            for dr in session.scalars(select(DagRun).where(DagRun.id.in_(dagrun.id for dagrun in drs))):
                count += 1
                if state == DagRunState.RUNNING:
                    dr.start_date = timezone.utcnow()
                dr.state = state
            session.commit()
            flash(f"{count} dag runs were set to {state}.")
        except Exception as ex:
            flash(str(ex), "error")
            flash("Failed to set state", "error")
        return redirect(self.get_default_url())

    @action(
        "set_failed",
        "Set state to 'failed'",
        "All running task instances would also be marked as failed, are you sure?",
        single=False,
    )
    @auth.has_access_dag_entities("PUT", DagAccessEntity.RUN)
    @provide_session
    @action_logging
    def action_set_failed(self, drs: list[DagRun], session: Session = NEW_SESSION):
        """Set state to failed."""
        try:
            count = 0
            altered_tis = []
            for dr in session.scalars(select(DagRun).where(DagRun.id.in_(dagrun.id for dagrun in drs))):
                count += 1
                altered_tis += set_dag_run_state_to_failed(
                    dag=get_airflow_app().dag_bag.get_dag(dr.dag_id),
                    run_id=dr.run_id,
                    commit=True,
                    session=session,
                )
            altered_ti_count = len(altered_tis)
            flash(f"{count} dag runs and {altered_ti_count} task instances were set to failed")
        except Exception:
            flash("Failed to set state", "error")
        return redirect(self.get_default_url())

    @action(
        "set_success",
        "Set state to 'success'",
        "All task instances would also be marked as success, are you sure?",
        single=False,
    )
    @auth.has_access_dag_entities("PUT", DagAccessEntity.RUN)
    @provide_session
    @action_logging
    def action_set_success(self, drs: list[DagRun], session: Session = NEW_SESSION):
        """Set state to success."""
        try:
            count = 0
            altered_tis = []
            for dr in session.scalars(select(DagRun).where(DagRun.id.in_(dagrun.id for dagrun in drs))):
                count += 1
                altered_tis += set_dag_run_state_to_success(
                    dag=get_airflow_app().dag_bag.get_dag(dr.dag_id),
                    run_id=dr.run_id,
                    commit=True,
                    session=session,
                )
            altered_ti_count = len(altered_tis)
            flash(f"{count} dag runs and {altered_ti_count} task instances were set to success")
        except Exception:
            flash("Failed to set state", "error")
        return redirect(self.get_default_url())

    @action("clear", "Clear the state", "All task instances would be cleared, are you sure?", single=False)
    @auth.has_access_dag_entities("PUT", DagAccessEntity.RUN)
    @provide_session
    @action_logging
    def action_clear(self, drs: list[DagRun], session: Session = NEW_SESSION):
        """Clear the state."""
        try:
            count = 0
            cleared_ti_count = 0
            dag_to_tis: dict[DAG, list[TaskInstance]] = {}
            for dr in session.scalars(select(DagRun).where(DagRun.id.in_(dagrun.id for dagrun in drs))):
                count += 1
                dag = get_airflow_app().dag_bag.get_dag(dr.dag_id)
                tis_to_clear = dag_to_tis.setdefault(dag, [])
                tis_to_clear += dr.get_task_instances()

            for dag, tis in dag_to_tis.items():
                cleared_ti_count += len(tis)
                models.clear_task_instances(tis, session, dag=dag)

            flash(f"{count} dag runs and {cleared_ti_count} task instances were cleared")
        except Exception:
            flash("Failed to clear state", "error")
        return redirect(self.get_default_url())


class LogModelView(AirflowModelView):
    """View to show records from Log table."""

    route_base = "/log"

    datamodel = AirflowModelView.CustomSQLAInterface(Log)  # type:ignore

    class_permission_name = permissions.RESOURCE_AUDIT_LOG
    method_permission_name = {
        "list": "read",
    }
    base_permissions = [
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    list_columns = [
        "id",
        "dttm",
        "dag_id",
        "task_id",
        "run_id",
        "event",
        "execution_date",
        "owner",
        "owner_display_name",
        "extra",
    ]
    search_columns = [
        "dttm",
        "dag_id",
        "task_id",
        "run_id",
        "event",
        "execution_date",
        "owner",
        "owner_display_name",
        "extra",
    ]

    label_columns = {
        "execution_date": "Logical Date",
        "owner": "Owner ID",
        "owner_display_name": "Owner Name",
    }

    base_order = ("dttm", "desc")

    base_filters = [["dag_id", DagFilter, list]]

    formatters_columns = {
        "dttm": wwwutils.datetime_f("dttm"),
        "execution_date": wwwutils.datetime_f("execution_date"),
        "dag_id": wwwutils.dag_link,
    }


class TaskRescheduleModelView(AirflowModelView):
    """View to show records from Task Reschedule table."""

    route_base = "/taskreschedule"

    datamodel = AirflowModelView.CustomSQLAInterface(models.TaskReschedule)  # type: ignore
    related_views = [DagRunModelView]

    class_permission_name = permissions.RESOURCE_TASK_RESCHEDULE
    method_permission_name = {
        "list": "read",
    }

    base_permissions = [
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    list_columns = [
        "id",
        "dag_id",
        "run_id",
        "dag_run.execution_date",
        "task_id",
        "map_index",
        "try_number",
        "start_date",
        "end_date",
        "duration",
        "reschedule_date",
    ]

    label_columns = {
        "dag_run.execution_date": "Logical Date",
    }

    search_columns = [
        "dag_id",
        "task_id",
        "run_id",
        "execution_date",
        "start_date",
        "end_date",
        "reschedule_date",
    ]

    base_order = ("id", "desc")

    base_filters = [["dag_id", DagFilter, list]]

    def duration_f(self):
        """Duration calculation."""
        end_date = self.get("end_date")
        duration = self.get("duration")
        if end_date and duration:
            return td_format(datetime.timedelta(seconds=duration))
        return None

    formatters_columns = {
        "dag_id": wwwutils.dag_link,
        "task_id": wwwutils.task_instance_link,
        "start_date": wwwutils.datetime_f("start_date"),
        "end_date": wwwutils.datetime_f("end_date"),
        "dag_run.execution_date": wwwutils.datetime_f("dag_run.execution_date"),
        "reschedule_date": wwwutils.datetime_f("reschedule_date"),
        "duration": duration_f,
        "map_index": wwwutils.format_map_index,
    }


class TriggerModelView(AirflowModelView):
    """View to show records from Task Reschedule table."""

    route_base = "/triggerview"

    datamodel = AirflowModelView.CustomSQLAInterface(models.Trigger)  # type: ignore

    class_permission_name = permissions.RESOURCE_TRIGGER
    method_permission_name = {
        "list": "read",
    }

    base_permissions = [
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    list_columns = [
        "id",
        "classpath",
        "created_date",
        "triggerer_id",
    ]

    search_columns = [
        "id",
        "classpath",
        "created_date",
        "triggerer_id",
    ]

    base_order = ("id", "created_date")

    formatters_columns = {
        "created_date": wwwutils.datetime_f("created_date"),
    }


class TaskInstanceModelView(AirflowModelView):
    """View to show records from TaskInstance table."""

    route_base = "/taskinstance"

    datamodel = AirflowModelView.CustomSQLAInterface(models.TaskInstance)  # type: ignore

    class_permission_name = permissions.RESOURCE_TASK_INSTANCE
    method_permission_name = {
        "list": "read",
        "delete": "delete",
        "action_clear": "edit",
        "action_clear_downstream": "edit",
        "action_muldelete": "delete",
        "action_set_failed": "edit",
        "action_set_success": "edit",
        "action_set_retry": "edit",
        "action_set_skipped": "edit",
    }
    base_permissions = [
        permissions.ACTION_CAN_CREATE,
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_DELETE,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    page_size = PAGE_SIZE

    list_columns = [
        "state",
        "dag_id",
        "task_id",
        "run_id",
        "map_index",
        "dag_run.execution_date",
        "operator",
        "start_date",
        "end_date",
        "duration",
        "note",
        "job_id",
        "hostname",
        "unixname",
        "priority_weight",
        "queue",
        "queued_dttm",
        "prev_attempted_tries",
        "pool",
        "queued_by_job_id",
        "external_executor_id",
        "log_url",
    ]

    order_columns = [
        "state",
        "dag_id",
        "task_id",
        "run_id",
        "map_index",
        "dag_run.execution_date",
        "operator",
        "start_date",
        "end_date",
        "duration",
        # "note",  # TODO: Maybe figure out how to re-enable this.
        "job_id",
        "hostname",
        "unixname",
        "priority_weight",
        "queue",
        "queued_dttm",
        "pool",
        "queued_by_job_id",
    ]
    # todo: don't use prev_attempted_tries; just use try_number
    label_columns = {"dag_run.execution_date": "Logical Date", "prev_attempted_tries": "Try Number"}

    search_columns = [
        "state",
        "dag_id",
        "task_id",
        "run_id",
        "map_index",
        "execution_date",
        "operator",
        "start_date",
        "end_date",
        "note",
        "hostname",
        "priority_weight",
        "queue",
        "queued_dttm",
        "try_number",
        "pool",
        "queued_by_job_id",
    ]

    edit_columns = [
        "dag_id",
        "task_id",
        "execution_date",
        "start_date",
        "end_date",
        "state",
        "note",
    ]

    add_exclude_columns = ["next_method", "next_kwargs", "trigger_id"]

    edit_form = TaskInstanceEditForm

    base_order = ("job_id", "asc")

    base_filters = [["dag_id", DagFilter, list]]

    def log_url_formatter(self):
        """Format log URL."""
        dag_id = self.get("dag_id")
        task_id = self.get("task_id")
        run_id = self.get("run_id")
        map_index = self.get("map_index", None)
        if map_index == -1:
            map_index = None

        url = url_for(
            "Airflow.grid",
            dag_id=dag_id,
            task_id=task_id,
            dag_run_id=run_id,
            map_index=map_index,
            tab="logs",
        )
        return Markup(
            '<a href="{log_url}"><span class="material-icons" aria-hidden="true">reorder</span></a>'
        ).format(log_url=url)

    def duration_f(self):
        """Format duration."""
        end_date = self.get("end_date")
        duration = self.get("duration")
        if end_date and duration:
            return td_format(datetime.timedelta(seconds=duration))
        return None

    formatters_columns = {
        "log_url": log_url_formatter,
        "task_id": wwwutils.task_instance_link,
        "run_id": wwwutils.dag_run_link,
        "map_index": wwwutils.format_map_index,
        "hostname": wwwutils.nobr_f("hostname"),
        "state": wwwutils.state_f,
        "dag_run.execution_date": wwwutils.datetime_f("dag_run.execution_date"),
        "start_date": wwwutils.datetime_f("start_date"),
        "end_date": wwwutils.datetime_f("end_date"),
        "queued_dttm": wwwutils.datetime_f("queued_dttm"),
        "dag_id": wwwutils.dag_link,
        "duration": duration_f,
    }

    def _clear_task_instances(
        self, task_instances: list[TaskInstance], session: Session, clear_downstream: bool = False
    ) -> tuple[int, int]:
        """
        Clear task instances, optionally including their downstream dependencies.

        :param task_instances: list of TIs to clear
        :param clear_downstream: should downstream task instances be cleared as well?

        :return: a tuple with:
            - count of cleared task instances actually selected by the user
            - count of downstream task instances that were additionally cleared
        """
        cleared_tis_count = 0
        cleared_downstream_tis_count = 0

        # Group TIs by dag id in order to call `get_dag` only once per dag
        tis_grouped_by_dag_id = itertools.groupby(task_instances, lambda ti: ti.dag_id)

        for dag_id, dag_tis in tis_grouped_by_dag_id:
            dag = get_airflow_app().dag_bag.get_dag(dag_id)

            tis_to_clear = list(dag_tis)
            downstream_tis_to_clear = []

            if clear_downstream:
                tis_to_clear_grouped_by_dag_run = itertools.groupby(tis_to_clear, lambda ti: ti.dag_run)

                for dag_run, dag_run_tis in tis_to_clear_grouped_by_dag_run:
                    # Determine tasks that are downstream of the cleared TIs and fetch associated TIs
                    # This has to be run for each dag run because the user may clear different TIs across runs
                    task_ids_to_clear = [ti.task_id for ti in dag_run_tis]

                    partial_dag = dag.partial_subset(
                        task_ids_or_regex=task_ids_to_clear, include_downstream=True, include_upstream=False
                    )

                    downstream_task_ids_to_clear = [
                        task_id for task_id in partial_dag.task_dict if task_id not in task_ids_to_clear
                    ]

                    # dag.clear returns TIs when in dry run mode
                    downstream_tis_to_clear.extend(
                        dag.clear(
                            start_date=dag_run.execution_date,
                            end_date=dag_run.execution_date,
                            task_ids=downstream_task_ids_to_clear,
                            include_subdags=False,
                            include_parentdag=False,
                            session=session,
                            dry_run=True,
                        )
                    )

            # Once all TIs are fetched, perform the actual clearing
            models.clear_task_instances(tis=tis_to_clear + downstream_tis_to_clear, session=session, dag=dag)

            cleared_tis_count += len(tis_to_clear)
            cleared_downstream_tis_count += len(downstream_tis_to_clear)

        return cleared_tis_count, cleared_downstream_tis_count

    @action(
        "clear",
        lazy_gettext("Clear"),
        lazy_gettext(
            "Are you sure you want to clear the state of the selected task"
            " instance(s) and set their dagruns to the QUEUED state?"
        ),
        single=False,
    )
    @auth.has_access_dag_entities("PUT", DagAccessEntity.TASK_INSTANCE)
    @provide_session
    @action_logging
    def action_clear(self, task_instances, session: Session = NEW_SESSION):
        """Clear an arbitrary number of task instances."""
        try:
            count, _ = self._clear_task_instances(
                task_instances=task_instances, session=session, clear_downstream=False
            )
            session.commit()
            flash(f"{count} task instance{'s have' if count > 1 else ' has'} been cleared")
        except Exception as e:
            flash(f'Failed to clear task instances: "{e}"', "error")

        self.update_redirect()
        return redirect(self.get_redirect())

    @action(
        "clear_downstream",
        lazy_gettext("Clear (including downstream tasks)"),
        lazy_gettext(
            "Are you sure you want to clear the state of the selected task"
            " instance(s) and all their downstream dependencies, and set their dagruns to the QUEUED state?"
        ),
        single=False,
    )
    @auth.has_access_dag_entities("PUT", DagAccessEntity.TASK_INSTANCE)
    @provide_session
    @action_logging
    def action_clear_downstream(self, task_instances, session: Session = NEW_SESSION):
        """Clear an arbitrary number of task instances, including downstream dependencies."""
        try:
            selected_ti_count, downstream_ti_count = self._clear_task_instances(
                task_instances=task_instances, session=session, clear_downstream=True
            )
            session.commit()
            flash(
                f"Cleared {selected_ti_count} selected task instance{'s' if selected_ti_count > 1 else ''} "
                f"and {downstream_ti_count} downstream dependencies"
            )
        except Exception as e:
            flash(f'Failed to clear task instances: "{e}"', "error")

        self.update_redirect()
        return redirect(self.get_redirect())

    @action("muldelete", "Delete", "Are you sure you want to delete selected records?", single=False)
    @auth.has_access_dag_entities("DELETE", DagAccessEntity.TASK_INSTANCE)
    @action_logging
    def action_muldelete(self, items):
        self.datamodel.delete_all(items)
        self.update_redirect()
        return redirect(self.get_redirect())

    @provide_session
    def set_task_instance_state(
        self,
        tis: Collection[TaskInstance],
        target_state: TaskInstanceState,
        session: Session = NEW_SESSION,
    ) -> None:
        """Set task instance state."""
        try:
            count = len(tis)
            for ti in tis:
                ti.set_state(target_state, session)
            session.commit()
            flash(f"{count} task instances were set to '{target_state}'")
        except Exception:
            flash("Failed to set state", "error")

    @action("set_failed", "Set state to 'failed'", "", single=False)
    @auth.has_access_dag_entities("PUT", DagAccessEntity.TASK_INSTANCE)
    @action_logging
    def action_set_failed(self, tis):
        """Set state to 'failed'."""
        self.set_task_instance_state(tis, TaskInstanceState.FAILED)
        self.update_redirect()
        return redirect(self.get_redirect())

    @action("set_success", "Set state to 'success'", "", single=False)
    @auth.has_access_dag_entities("PUT", DagAccessEntity.TASK_INSTANCE)
    @action_logging
    def action_set_success(self, tis):
        """Set state to 'success'."""
        self.set_task_instance_state(tis, TaskInstanceState.SUCCESS)
        self.update_redirect()
        return redirect(self.get_redirect())

    @action("set_retry", "Set state to 'up_for_retry'", "", single=False)
    @auth.has_access_dag_entities("PUT", DagAccessEntity.TASK_INSTANCE)
    @action_logging
    def action_set_retry(self, tis):
        """Set state to 'up_for_retry'."""
        self.set_task_instance_state(tis, TaskInstanceState.UP_FOR_RETRY)
        self.update_redirect()
        return redirect(self.get_redirect())

    @action("set_skipped", "Set state to 'skipped'", "", single=False)
    @auth.has_access_dag_entities("PUT", DagAccessEntity.TASK_INSTANCE)
    @action_logging
    def action_set_skipped(self, tis):
        """Set state to skipped."""
        self.set_task_instance_state(tis, TaskInstanceState.SKIPPED)
        self.update_redirect()
        return redirect(self.get_redirect())


class AutocompleteView(AirflowBaseView):
    """View to provide autocomplete results."""

    @provide_session
    @expose("/dagmodel/autocomplete")
    def autocomplete(self, session: Session = NEW_SESSION):
        """Autocomplete."""
        query = unquote(request.args.get("query", ""))

        if not query:
            return flask.json.jsonify([])

        # Provide suggestions of dag_ids and owners
        dag_ids_query = select(
            sqla.literal("dag").label("type"),
            DagModel.dag_id.label("name"),
            DagModel._dag_display_property_value.label("dag_display_name"),
        ).where(
            ~DagModel.is_subdag,
            DagModel.is_active,
            or_(
                DagModel.dag_id.ilike(f"%{query}%"),
                DagModel._dag_display_property_value.ilike(f"%{query}%"),
            ),
        )

        owners_query = (
            select(
                sqla.literal("owner").label("type"),
                DagModel.owners.label("name"),
                sqla.literal(None).label("dag_display_name"),
            )
            .distinct()
            .where(~DagModel.is_subdag, DagModel.is_active, DagModel.owners.ilike(f"%{query}%"))
        )

        # Hide DAGs if not showing status: "all"
        status = flask_session.get(FILTER_STATUS_COOKIE)
        if status == "active":
            dag_ids_query = dag_ids_query.where(~DagModel.is_paused)
            owners_query = owners_query.where(~DagModel.is_paused)
        elif status == "paused":
            dag_ids_query = dag_ids_query.where(DagModel.is_paused)
            owners_query = owners_query.where(DagModel.is_paused)

        filter_dag_ids = get_auth_manager().get_permitted_dag_ids(user=g.user)

        dag_ids_query = dag_ids_query.where(DagModel.dag_id.in_(filter_dag_ids))
        owners_query = owners_query.where(DagModel.dag_id.in_(filter_dag_ids))
        payload = [
            row._asdict()
            for row in session.execute(dag_ids_query.union(owners_query).order_by("name").limit(10))
        ]
        return flask.json.jsonify(payload)


class DagDependenciesView(AirflowBaseView):
    """View to show dependencies between DAGs."""

    refresh_interval = datetime.timedelta(
        seconds=conf.getint(
            "webserver",
            "dag_dependencies_refresh_interval",
            fallback=conf.getint("scheduler", "dag_dir_list_interval"),
        )
    )
    last_refresh = timezone.utcnow() - refresh_interval
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, str]] = []

    @expose("/dag-dependencies")
    @auth.has_access_dag("GET", DagAccessEntity.DEPENDENCIES)
    @gzipped
    def list(self):
        """Display DAG dependencies."""
        title = "DAG Dependencies"

        if not self.nodes or not self.edges:
            self._calculate_graph()
            self.last_refresh = timezone.utcnow()
        elif timezone.utcnow() > self.last_refresh + self.refresh_interval:
            max_last_updated = SerializedDagModel.get_max_last_updated_datetime()
            if max_last_updated is None or max_last_updated > self.last_refresh:
                self._calculate_graph()
            self.last_refresh = timezone.utcnow()

        return self.render_template(
            "airflow/dag_dependencies.html",
            title=title,
            nodes=self.nodes,
            edges=self.edges,
            last_refresh=self.last_refresh,
            arrange=conf.get("webserver", "dag_orientation"),
            width=request.args.get("width", "100%"),
            height=request.args.get("height", "800"),
        )

    def _calculate_graph(self):
        nodes_dict: dict[str, Any] = {}
        edge_tuples: set[dict[str, str]] = set()

        for dag, dependencies in SerializedDagModel.get_dag_dependencies().items():
            dag_node_id = f"dag:{dag}"
            if dag_node_id not in nodes_dict:
                nodes_dict[dag_node_id] = node_dict(dag_node_id, dag, "dag")

            for dep in dependencies:
                if dep.node_id not in nodes_dict:
                    nodes_dict[dep.node_id] = node_dict(dep.node_id, dep.dependency_id, dep.dependency_type)
                edge_tuples.add((f"dag:{dep.source}", dep.node_id))
                edge_tuples.add((dep.node_id, f"dag:{dep.target}"))

        self.nodes = list(nodes_dict.values())
        self.edges = [{"u": u, "v": v} for u, v in edge_tuples]


def add_user_permissions_to_dag(sender, template, context, **extra):
    """
    Add `.can_edit`, `.can_trigger`, and `.can_delete` properties to DAG based on current user's permissions.

    Located in `views.py` rather than the DAG model to keep permissions logic out of the Airflow core.
    """
    if "dag" not in context:
        return
    dag = context["dag"]
    can_create_dag_run = get_auth_manager().is_authorized_dag(
        method="POST", access_entity=DagAccessEntity.RUN, details=DagDetails(id=dag.dag_id)
    )

    dag.can_edit = get_auth_manager().is_authorized_dag(method="PUT", details=DagDetails(id=dag.dag_id))
    dag.can_trigger = dag.can_edit and can_create_dag_run
    dag.can_delete = get_auth_manager().is_authorized_dag(method="DELETE", details=DagDetails(id=dag.dag_id))
    context["dag"] = dag


##############################################################################
#                                                                            #
#                          Development Views                                 #
#                                                                            #
##############################################################################


def restrict_to_dev(f):
    def wrapper(*args, **kwargs):
        if not os.environ.get("AIRFLOW_ENV", None) == "development":
            logger.error(
                "You can only access this view in development mode. Set AIRFLOW_ENV=development to view it."
            )
            return abort(404)
        return f(*args, **kwargs)

    return wrapper


class DevView(BaseView):
    """
    View to show Airflow Dev Endpoints.

    This view should only be accessible in development mode. You can enable development mode by setting
    `AIRFLOW_ENV=development` in your environment.

    :meta private:
    """

    route_base = "/dev"

    @expose("/coverage/<path:path>")
    @restrict_to_dev
    def coverage(self, path):
        return send_from_directory(Path("htmlcov").resolve(), path)


class DocsView(BaseView):
    """
    View to show airflow dev docs endpoints.

    This view should only be accessible in development mode. You can enable development mode by setting
    `AIRFLOW_ENV=development` in your environment.
    """

    route_base = "/docs"

    @expose("/")
    @expose("/<path:filename>")
    @restrict_to_dev
    def home(self, filename="index.html"):
        """Serve documentation from the build directory."""
        if filename != "index.html":
            return send_from_directory(Path("docs/_build/docs/").resolve(), filename)
        return send_from_directory(Path("docs/_build/").resolve(), filename)


# NOTE: Put this at the end of the file. Pylance is too clever and detects that
# before_render_template.connect() is declared as NoReturn, and marks everything
# after this line as unreachable code. It's technically correct based on the
# lint-time information, but that's not what actually happens at runtime.
before_render_template.connect(add_user_permissions_to_dag)

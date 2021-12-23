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
#
import collections
import copy
import json
import logging
import math
import re
import socket
import sys
import traceback
from collections import defaultdict
from datetime import timedelta
from functools import wraps
from json import JSONDecodeError
from operator import itemgetter
from typing import Any, Callable, Iterable, List, Optional, Set, Tuple, Union
from urllib.parse import parse_qsl, unquote, urlencode, urlparse

import lazy_object_proxy
import markupsafe
import nvd3
import sqlalchemy as sqla
from flask import (
    Markup,
    Response,
    abort,
    before_render_template,
    current_app,
    escape,
    flash,
    g,
    jsonify,
    make_response,
    redirect,
    render_template,
    request,
    send_from_directory,
    session as flask_session,
    url_for,
)
from flask_appbuilder import BaseView, ModelView, expose
from flask_appbuilder.actions import action
from flask_appbuilder.fieldwidgets import Select2Widget
from flask_appbuilder.models.sqla.filters import BaseFilter
from flask_appbuilder.security.decorators import has_access
from flask_appbuilder.security.views import (
    PermissionModelView,
    PermissionViewModelView,
    ResetMyPasswordView,
    ResetPasswordView,
    RoleModelView,
    UserDBModelView,
    UserInfoEditView,
    UserLDAPModelView,
    UserOAuthModelView,
    UserOIDModelView,
    UserRemoteUserModelView,
    UserStatsChartView,
    ViewMenuModelView,
)
from flask_appbuilder.widgets import FormWidget
from flask_babel import lazy_gettext
from jinja2.utils import htmlsafe_json_dumps, pformat  # type: ignore
from pendulum.datetime import DateTime
from pendulum.parsing.exceptions import ParserError
from pygments import highlight, lexers
from pygments.formatters import HtmlFormatter
from sqlalchemy import Date, and_, desc, func, inspect, union_all
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload
from wtforms import SelectField, validators
from wtforms.validators import InputRequired

import airflow
from airflow import models, plugins_manager, settings
from airflow.api.common.experimental.mark_tasks import (
    set_dag_run_state_to_failed,
    set_dag_run_state_to_success,
)
from airflow.configuration import AIRFLOW_CONFIG, conf
from airflow.exceptions import AirflowException
from airflow.executors.executor_loader import ExecutorLoader
from airflow.jobs.base_job import BaseJob
from airflow.jobs.scheduler_job import SchedulerJob
from airflow.jobs.triggerer_job import TriggererJob
from airflow.models import DAG, Connection, DagModel, DagTag, Log, SlaMiss, TaskFail, XCom, errors
from airflow.models.baseoperator import BaseOperator
from airflow.models.dagcode import DagCode
from airflow.models.dagrun import DagRun, DagRunType
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance
from airflow.providers_manager import ProvidersManager
from airflow.security import permissions
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_deps import RUNNING_DEPS, SCHEDULER_QUEUED_DEPS
from airflow.utils import json as utils_json, timezone, yaml
from airflow.utils.dates import infer_time_unit, scale_time_units
from airflow.utils.docs import get_doc_url_for_provider, get_docs_url
from airflow.utils.helpers import alchemy_to_dict
from airflow.utils.log import secrets_masker
from airflow.utils.log.log_reader import TaskLogReader
from airflow.utils.session import create_session, provide_session
from airflow.utils.state import State
from airflow.utils.strings import to_boolean
from airflow.version import version
from airflow.www import auth, utils as wwwutils
from airflow.www.decorators import action_logging, gzipped
from airflow.www.forms import (
    ConnectionForm,
    DagRunEditForm,
    DateTimeForm,
    DateTimeWithNumRunsForm,
    DateTimeWithNumRunsWithDagRunsForm,
    TaskInstanceEditForm,
)
from airflow.www.widgets import AirflowModelListWidget

PAGE_SIZE = conf.getint('webserver', 'page_size')
FILTER_TAGS_COOKIE = 'tags_filter'
FILTER_STATUS_COOKIE = 'dag_status_filter'


def truncate_task_duration(task_duration):
    """
    Cast the task_duration to an int was for optimization for large/huge dags if task_duration > 10s
    otherwise we keep it as a float with 3dp
    """
    return int(task_duration) if task_duration > 10.0 else round(task_duration, 3)


def get_safe_url(url):
    """Given a user-supplied URL, ensure it points to our web server"""
    valid_schemes = ['http', 'https', '']
    valid_netlocs = [request.host, '']

    if not url:
        return url_for('Airflow.index')

    parsed = urlparse(url)

    # If the url contains semicolon, redirect it to homepage to avoid
    # potential XSS. (Similar to https://github.com/python/cpython/pull/24297/files (bpo-42967))
    if ';' in unquote(url):
        return url_for('Airflow.index')

    query = parse_qsl(parsed.query, keep_blank_values=True)

    url = parsed._replace(query=urlencode(query)).geturl()

    if parsed.scheme in valid_schemes and parsed.netloc in valid_netlocs:
        return url

    return url_for('Airflow.index')


def get_date_time_num_runs_dag_runs_form_data(www_request, session, dag):
    """Get Execution Data, Base Date & Number of runs from a Request"""
    date_time = www_request.args.get('execution_date')
    if date_time:
        date_time = timezone.parse(date_time)
    else:
        date_time = dag.get_latest_execution_date(session=session) or timezone.utcnow()

    base_date = www_request.args.get('base_date')
    if base_date:
        base_date = timezone.parse(base_date)
    else:
        # The DateTimeField widget truncates milliseconds and would loose
        # the first dag run. Round to next second.
        base_date = (date_time + timedelta(seconds=1)).replace(microsecond=0)

    default_dag_run = conf.getint('webserver', 'default_dag_run_display_number')
    num_runs = www_request.args.get('num_runs', default=default_dag_run, type=int)

    drs = (
        session.query(DagRun)
        .filter(DagRun.dag_id == dag.dag_id, DagRun.execution_date <= base_date)
        .order_by(desc(DagRun.execution_date))
        .limit(num_runs)
        .all()
    )
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
        'dttm': date_time,
        'base_date': base_date,
        'num_runs': num_runs,
        'execution_date': date_time.isoformat(),
        'dr_choices': dr_choices,
        'dr_state': dr_state,
    }


def task_group_to_dict(task_group):
    """
    Create a nested dict representation of this TaskGroup and its children used to construct
    the Graph.
    """
    if isinstance(task_group, BaseOperator):
        return {
            'id': task_group.task_id,
            'value': {
                'label': task_group.label,
                'labelStyle': f"fill:{task_group.ui_fgcolor};",
                'style': f"fill:{task_group.ui_color};",
                'rx': 5,
                'ry': 5,
            },
        }

    children = [
        task_group_to_dict(child) for child in sorted(task_group.children.values(), key=lambda t: t.label)
    ]

    if task_group.upstream_group_ids or task_group.upstream_task_ids:
        children.append(
            {
                'id': task_group.upstream_join_id,
                'value': {
                    'label': '',
                    'labelStyle': f"fill:{task_group.ui_fgcolor};",
                    'style': f"fill:{task_group.ui_color};",
                    'shape': 'circle',
                },
            }
        )

    if task_group.downstream_group_ids or task_group.downstream_task_ids:
        # This is the join node used to reduce the number of edges between two TaskGroup.
        children.append(
            {
                'id': task_group.downstream_join_id,
                'value': {
                    'label': '',
                    'labelStyle': f"fill:{task_group.ui_fgcolor};",
                    'style': f"fill:{task_group.ui_color};",
                    'shape': 'circle',
                },
            }
        )

    return {
        "id": task_group.group_id,
        'value': {
            'label': task_group.label,
            'labelStyle': f"fill:{task_group.ui_fgcolor};",
            'style': f"fill:{task_group.ui_color}",
            'rx': 5,
            'ry': 5,
            'clusterLabelPos': 'top',
            'tooltip': task_group.tooltip,
        },
        'children': children,
    }


def get_key_paths(input_dict):
    """Return a list of dot-separated dictionary paths"""
    for key, value in input_dict.items():
        if isinstance(value, dict):
            for sub_key in get_key_paths(value):
                yield '.'.join((key, sub_key))
        else:
            yield key


def get_value_from_path(key_path, content):
    """Return the value from a dictionary based on dot-separated path of keys"""
    elem = content
    for x in key_path.strip(".").split("."):
        try:
            x = int(x)
            elem = elem[x]
        except ValueError:
            elem = elem.get(x)

    return elem


def dag_edges(dag):
    """
    Create the list of edges needed to construct the Graph view.

    A special case is made if a TaskGroup is immediately upstream/downstream of another
    TaskGroup or task. Two dummy nodes named upstream_join_id and downstream_join_id are
    created for the TaskGroup. Instead of drawing an edge onto every task in the TaskGroup,
    all edges are directed onto the dummy nodes. This is to cut down the number of edges on
    the graph.

    For example: A DAG with TaskGroups group1 and group2:
        group1: task1, task2, task3
        group2: task4, task5, task6

    group2 is downstream of group1:
        group1 >> group2

    Edges to add (This avoids having to create edges between every task in group1 and group2):
        task1 >> downstream_join_id
        task2 >> downstream_join_id
        task3 >> downstream_join_id
        downstream_join_id >> upstream_join_id
        upstream_join_id >> task4
        upstream_join_id >> task5
        upstream_join_id >> task6
    """
    # Edges to add between TaskGroup
    edges_to_add = set()
    # Edges to remove between individual tasks that are replaced by edges_to_add.
    edges_to_skip = set()

    task_group_map = dag.task_group.get_task_group_dict()

    def collect_edges(task_group):
        """Update edges_to_add and edges_to_skip according to TaskGroups."""
        if isinstance(task_group, BaseOperator):
            return

        for target_id in task_group.downstream_group_ids:
            # For every TaskGroup immediately downstream, add edges between downstream_join_id
            # and upstream_join_id. Skip edges between individual tasks of the TaskGroups.
            target_group = task_group_map[target_id]
            edges_to_add.add((task_group.downstream_join_id, target_group.upstream_join_id))

            for child in task_group.get_leaves():
                edges_to_add.add((child.task_id, task_group.downstream_join_id))
                for target in target_group.get_roots():
                    edges_to_skip.add((child.task_id, target.task_id))
                edges_to_skip.add((child.task_id, target_group.upstream_join_id))

            for child in target_group.get_roots():
                edges_to_add.add((target_group.upstream_join_id, child.task_id))
                edges_to_skip.add((task_group.downstream_join_id, child.task_id))

        # For every individual task immediately downstream, add edges between downstream_join_id and
        # the downstream task. Skip edges between individual tasks of the TaskGroup and the
        # downstream task.
        for target_id in task_group.downstream_task_ids:
            edges_to_add.add((task_group.downstream_join_id, target_id))

            for child in task_group.get_leaves():
                edges_to_add.add((child.task_id, task_group.downstream_join_id))
                edges_to_skip.add((child.task_id, target_id))

        # For every individual task immediately upstream, add edges between the upstream task
        # and upstream_join_id. Skip edges between the upstream task and individual tasks
        # of the TaskGroup.
        for source_id in task_group.upstream_task_ids:
            edges_to_add.add((source_id, task_group.upstream_join_id))
            for child in task_group.get_roots():
                edges_to_add.add((task_group.upstream_join_id, child.task_id))
                edges_to_skip.add((source_id, child.task_id))

        for child in task_group.children.values():
            collect_edges(child)

    collect_edges(dag.task_group)

    # Collect all the edges between individual tasks
    edges = set()

    def get_downstream(task):
        for child in task.downstream_list:
            edge = (task.task_id, child.task_id)
            if edge not in edges:
                edges.add(edge)
                get_downstream(child)

    for root in dag.roots:
        get_downstream(root)

    result = []
    # Build result dicts with the two ends of the edge, plus any extra metadata
    # if we have it.
    for source_id, target_id in sorted(edges.union(edges_to_add) - edges_to_skip):
        record = {"source_id": source_id, "target_id": target_id}
        label = dag.get_edge_info(source_id, target_id).get("label")
        if label:
            record["label"] = label
        result.append(record)
    return result


######################################################################################
#                                    Error handlers
######################################################################################


def not_found(error):
    """Show Not Found on screen for any error in the Webserver"""
    return (
        render_template(
            'airflow/not_found.html',
            hostname=socket.getfqdn()
            if conf.getboolean('webserver', 'EXPOSE_HOSTNAME', fallback=True)
            else 'redact',
        ),
        404,
    )


def show_traceback(error):
    """Show Traceback for a given error"""
    return (
        render_template(
            'airflow/traceback.html',
            python_version=sys.version.split(" ")[0],
            airflow_version=version,
            hostname=socket.getfqdn()
            if conf.getboolean('webserver', 'EXPOSE_HOSTNAME', fallback=True)
            else 'redact',
            info=traceback.format_exc()
            if conf.getboolean('webserver', 'EXPOSE_STACKTRACE', fallback=True)
            else 'Error! Please contact server admin.',
        ),
        500,
    )


######################################################################################
#                                    BaseViews
######################################################################################


class AirflowBaseView(BaseView):
    """Base View to set Airflow related properties"""

    from airflow import macros

    route_base = ''

    extra_args = {
        # Make our macros available to our UI templates too.
        'macros': macros,
        'get_docs_url': get_docs_url,
    }

    if not conf.getboolean('core', 'unit_test_mode'):
        extra_args['sqlite_warning'] = settings.Session.bind.dialect.name == 'sqlite'
        extra_args['sequential_executor_warning'] = conf.get('core', 'executor') == 'SequentialExecutor'

    line_chart_attr = {
        'legend.maxKeyLength': 200,
    }

    def render_template(self, *args, **kwargs):
        # Add triggerer_job only if we need it
        if TriggererJob.is_needed():
            kwargs["triggerer_job"] = lazy_object_proxy.Proxy(TriggererJob.most_recent_job)
        return super().render_template(
            *args,
            # Cache this at most once per request, not for the lifetime of the view instance
            scheduler_job=lazy_object_proxy.Proxy(SchedulerJob.most_recent_job),
            **kwargs,
        )


def add_user_permissions_to_dag(sender, template, context, **extra):
    """
    Adds `.can_edit`, `.can_trigger`, and `.can_delete` properties
    to DAG based on current user's permissions.
    Located in `views.py` rather than the DAG model to keep
    permissions logic out of the Airflow core.
    """
    if 'dag' in context:
        dag = context['dag']
        can_create_dag_run = current_app.appbuilder.sm.has_access(
            permissions.ACTION_CAN_CREATE, permissions.RESOURCE_DAG_RUN
        )

        dag.can_edit = current_app.appbuilder.sm.can_edit_dag(dag.dag_id)
        dag.can_trigger = dag.can_edit and can_create_dag_run
        dag.can_delete = current_app.appbuilder.sm.has_access(
            permissions.ACTION_CAN_DELETE,
            permissions.RESOURCE_DAG,
        )
        context['dag'] = dag


before_render_template.connect(add_user_permissions_to_dag)


class Airflow(AirflowBaseView):
    """Main Airflow application."""

    @expose('/health')
    def health(self):
        """
        An endpoint helping check the health status of the Airflow instance,
        including metadatabase and scheduler.
        """
        payload = {'metadatabase': {'status': 'unhealthy'}}

        latest_scheduler_heartbeat = None
        scheduler_status = 'unhealthy'
        payload['metadatabase'] = {'status': 'healthy'}
        try:
            scheduler_job = SchedulerJob.most_recent_job()

            if scheduler_job:
                latest_scheduler_heartbeat = scheduler_job.latest_heartbeat.isoformat()
                if scheduler_job.is_alive():
                    scheduler_status = 'healthy'
        except Exception:
            payload['metadatabase']['status'] = 'unhealthy'

        payload['scheduler'] = {
            'status': scheduler_status,
            'latest_scheduler_heartbeat': latest_scheduler_heartbeat,
        }

        return wwwutils.json_response(payload)

    @expose('/home')
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ]
    )
    def index(self):
        """Home view."""
        hide_paused_dags_by_default = conf.getboolean('webserver', 'hide_paused_dags_by_default')
        default_dag_run = conf.getint('webserver', 'default_dag_run_display_number')

        num_runs = request.args.get('num_runs', default=default_dag_run, type=int)
        current_page = request.args.get('page', default=0, type=int)
        arg_search_query = request.args.get('search')
        arg_tags_filter = request.args.getlist('tags')
        arg_status_filter = request.args.get('status')

        if request.args.get('reset_tags') is not None:
            flask_session[FILTER_TAGS_COOKIE] = None
            # Remove the reset_tags=reset from the URL
            return redirect(url_for('Airflow.index'))

        cookie_val = flask_session.get(FILTER_TAGS_COOKIE)
        if arg_tags_filter:
            flask_session[FILTER_TAGS_COOKIE] = ','.join(arg_tags_filter)
        elif cookie_val:
            # If tags exist in cookie, but not URL, add them to the URL
            return redirect(url_for('Airflow.index', tags=cookie_val.split(',')))

        if arg_status_filter is None:
            cookie_val = flask_session.get(FILTER_STATUS_COOKIE)
            if cookie_val:
                arg_status_filter = cookie_val
            else:
                arg_status_filter = 'active' if hide_paused_dags_by_default else 'all'
                flask_session[FILTER_STATUS_COOKIE] = arg_status_filter
        else:
            status = arg_status_filter.strip().lower()
            flask_session[FILTER_STATUS_COOKIE] = status
            arg_status_filter = status

        dags_per_page = PAGE_SIZE

        start = current_page * dags_per_page
        end = start + dags_per_page

        # Get all the dag id the user could access
        filter_dag_ids = current_app.appbuilder.sm.get_accessible_dag_ids(g.user)

        with create_session() as session:
            # read orm_dags from the db
            dags_query = session.query(DagModel).filter(~DagModel.is_subdag, DagModel.is_active)

            if arg_search_query:
                dags_query = dags_query.filter(
                    DagModel.dag_id.ilike('%' + arg_search_query + '%')
                    | DagModel.owners.ilike('%' + arg_search_query + '%')
                )

            if arg_tags_filter:
                dags_query = dags_query.filter(DagModel.tags.any(DagTag.name.in_(arg_tags_filter)))

            dags_query = dags_query.filter(DagModel.dag_id.in_(filter_dag_ids))

            all_dags = dags_query
            active_dags = dags_query.filter(~DagModel.is_paused)
            paused_dags = dags_query.filter(DagModel.is_paused)

            is_paused_count = dict(
                all_dags.with_entities(DagModel.is_paused, func.count(DagModel.dag_id))
                .group_by(DagModel.is_paused)
                .all()
            )
            status_count_active = is_paused_count.get(False, 0)
            status_count_paused = is_paused_count.get(True, 0)
            all_dags_count = status_count_active + status_count_paused
            if arg_status_filter == 'active':
                current_dags = active_dags
                num_of_all_dags = status_count_active
            elif arg_status_filter == 'paused':
                current_dags = paused_dags
                num_of_all_dags = status_count_paused
            else:
                current_dags = all_dags
                num_of_all_dags = all_dags_count

            dags = (
                current_dags.order_by(DagModel.dag_id)
                .options(joinedload(DagModel.tags))
                .offset(start)
                .limit(dags_per_page)
                .all()
            )

            user_permissions = current_app.appbuilder.sm.get_current_user_permissions()
            all_dags_editable = (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG) in user_permissions
            can_create_dag_run = (
                permissions.ACTION_CAN_CREATE,
                permissions.RESOURCE_DAG_RUN,
            ) in user_permissions

            can_delete_dag = (
                permissions.ACTION_CAN_DELETE,
                permissions.RESOURCE_DAG,
            ) in user_permissions

            for dag in dags:
                if all_dags_editable:
                    dag.can_edit = True
                else:
                    dag_resource_name = permissions.RESOURCE_DAG_PREFIX + dag.dag_id
                    dag.can_edit = (permissions.ACTION_CAN_EDIT, dag_resource_name) in user_permissions
                dag.can_trigger = dag.can_edit and can_create_dag_run
                dag.can_delete = can_delete_dag

            dagtags = session.query(DagTag.name).distinct(DagTag.name).all()
            tags = [
                {"name": name, "selected": bool(arg_tags_filter and name in arg_tags_filter)}
                for name, in dagtags
            ]

            import_errors = session.query(errors.ImportError).order_by(errors.ImportError.id)

            if (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG) not in user_permissions:
                # if the user doesn't have access to all DAGs, only display errors from visible DAGs
                import_errors = import_errors.join(
                    DagModel, DagModel.fileloc == errors.ImportError.filename
                ).filter(DagModel.dag_id.in_(filter_dag_ids))

            for import_error in import_errors:
                flash(
                    "Broken DAG: [{ie.filename}] {ie.stacktrace}".format(ie=import_error),
                    "dag_import_error",
                )

        from airflow.plugins_manager import import_errors as plugin_import_errors

        for filename, stacktrace in plugin_import_errors.items():
            flash(
                f"Broken plugin: [{filename}] {stacktrace}",
                "error",
            )

        num_of_pages = int(math.ceil(num_of_all_dags / float(dags_per_page)))

        state_color_mapping = State.state_color.copy()
        state_color_mapping["null"] = state_color_mapping.pop(None)

        page_title = conf.get(section="webserver", key="instance_name", fallback="DAGs")

        dashboard_alerts = [
            fm for fm in settings.DASHBOARD_UIALERTS if fm.should_show(current_app.appbuilder.sm)
        ]

        def _iter_parsed_moved_data_table_names():
            for table_name in inspect(session.get_bind()).get_table_names():
                segments = table_name.split("__", 2)
                if len(segments) < 3:
                    continue
                if segments[0] != settings.AIRFLOW_MOVED_TABLE_PREFIX:
                    continue
                # Second segment is a version marker that we don't need to show.
                yield segments[2], table_name

        return self.render_template(
            'airflow/dags.html',
            dags=dags,
            dashboard_alerts=dashboard_alerts,
            migration_moved_data_alerts=sorted(set(_iter_parsed_moved_data_table_names())),
            current_page=current_page,
            search_query=arg_search_query if arg_search_query else '',
            page_title=page_title,
            page_size=dags_per_page,
            num_of_pages=num_of_pages,
            num_dag_from=min(start + 1, num_of_all_dags),
            num_dag_to=min(end, num_of_all_dags),
            num_of_all_dags=num_of_all_dags,
            paging=wwwutils.generate_pages(
                current_page,
                num_of_pages,
                search=escape(arg_search_query) if arg_search_query else None,
                status=arg_status_filter if arg_status_filter else None,
                tags=arg_tags_filter if arg_tags_filter else None,
            ),
            num_runs=num_runs,
            tags=tags,
            state_color=state_color_mapping,
            status_filter=arg_status_filter,
            status_count_all=all_dags_count,
            status_count_active=status_count_active,
            status_count_paused=status_count_paused,
            tags_filter=arg_tags_filter,
        )

    @expose('/dag_stats', methods=['POST'])
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        ]
    )
    @provide_session
    def dag_stats(self, session=None):
        """Dag statistics."""
        dr = models.DagRun

        allowed_dag_ids = current_app.appbuilder.sm.get_accessible_dag_ids(g.user)

        dag_state_stats = session.query(dr.dag_id, dr.state, sqla.func.count(dr.state)).group_by(
            dr.dag_id, dr.state
        )

        # Filter by post parameters
        selected_dag_ids = {unquote(dag_id) for dag_id in request.form.getlist('dag_ids') if dag_id}

        if selected_dag_ids:
            filter_dag_ids = selected_dag_ids.intersection(allowed_dag_ids)
        else:
            filter_dag_ids = allowed_dag_ids

        if not filter_dag_ids:
            return wwwutils.json_response({})

        payload = {}
        dag_state_stats = dag_state_stats.filter(dr.dag_id.in_(filter_dag_ids))
        data = {}

        for dag_id, state, count in dag_state_stats:
            if dag_id not in data:
                data[dag_id] = {}
            data[dag_id][state] = count

        for dag_id in filter_dag_ids:
            payload[dag_id] = []
            for state in State.dag_states:
                count = data.get(dag_id, {}).get(state, 0)
                payload[dag_id].append({'state': state, 'count': count})

        return wwwutils.json_response(payload)

    @expose('/task_stats', methods=['POST'])
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        ]
    )
    @provide_session
    def task_stats(self, session=None):
        """Task Statistics"""
        allowed_dag_ids = current_app.appbuilder.sm.get_accessible_dag_ids(g.user)

        if not allowed_dag_ids:
            return wwwutils.json_response({})

        # Filter by post parameters
        selected_dag_ids = {unquote(dag_id) for dag_id in request.form.getlist('dag_ids') if dag_id}

        if selected_dag_ids:
            filter_dag_ids = selected_dag_ids.intersection(allowed_dag_ids)
        else:
            filter_dag_ids = allowed_dag_ids

        running_dag_run_query_result = (
            session.query(DagRun.dag_id, DagRun.run_id)
            .join(DagModel, DagModel.dag_id == DagRun.dag_id)
            .filter(DagRun.state == State.RUNNING, DagModel.is_active)
        )

        running_dag_run_query_result = running_dag_run_query_result.filter(DagRun.dag_id.in_(filter_dag_ids))

        running_dag_run_query_result = running_dag_run_query_result.subquery('running_dag_run')

        # Select all task_instances from active dag_runs.
        running_task_instance_query_result = session.query(
            TaskInstance.dag_id.label('dag_id'), TaskInstance.state.label('state')
        ).join(
            running_dag_run_query_result,
            and_(
                running_dag_run_query_result.c.dag_id == TaskInstance.dag_id,
                running_dag_run_query_result.c.run_id == TaskInstance.run_id,
            ),
        )

        if conf.getboolean('webserver', 'SHOW_RECENT_STATS_FOR_COMPLETED_RUNS', fallback=True):

            last_dag_run = (
                session.query(DagRun.dag_id, sqla.func.max(DagRun.execution_date).label('execution_date'))
                .join(DagModel, DagModel.dag_id == DagRun.dag_id)
                .filter(DagRun.state != State.RUNNING, DagModel.is_active)
                .group_by(DagRun.dag_id)
            )

            last_dag_run = last_dag_run.filter(DagRun.dag_id.in_(filter_dag_ids))
            last_dag_run = last_dag_run.subquery('last_dag_run')

            # Select all task_instances from active dag_runs.
            # If no dag_run is active, return task instances from most recent dag_run.
            last_task_instance_query_result = (
                session.query(TaskInstance.dag_id.label('dag_id'), TaskInstance.state.label('state'))
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
            ).alias('final_ti')
        else:
            final_task_instance_query_result = running_task_instance_query_result.subquery('final_ti')

        qry = session.query(
            final_task_instance_query_result.c.dag_id,
            final_task_instance_query_result.c.state,
            sqla.func.count(),
        ).group_by(final_task_instance_query_result.c.dag_id, final_task_instance_query_result.c.state)

        data = {}
        for dag_id, state, count in qry:
            if dag_id not in data:
                data[dag_id] = {}
            data[dag_id][state] = count

        payload = {}
        for dag_id in filter_dag_ids:
            payload[dag_id] = []
            for state in State.task_states:
                count = data.get(dag_id, {}).get(state, 0)
                payload[dag_id].append({'state': state, 'count': count})
        return wwwutils.json_response(payload)

    @expose('/last_dagruns', methods=['POST'])
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        ]
    )
    @provide_session
    def last_dagruns(self, session=None):
        """Last DAG runs"""
        allowed_dag_ids = current_app.appbuilder.sm.get_accessible_dag_ids(g.user)

        # Filter by post parameters
        selected_dag_ids = {unquote(dag_id) for dag_id in request.form.getlist('dag_ids') if dag_id}

        if selected_dag_ids:
            filter_dag_ids = selected_dag_ids.intersection(allowed_dag_ids)
        else:
            filter_dag_ids = allowed_dag_ids

        if not filter_dag_ids:
            return wwwutils.json_response({})

        last_runs_subquery = (
            session.query(
                DagRun.dag_id,
                sqla.func.max(DagRun.execution_date).label("max_execution_date"),
            )
            .group_by(DagRun.dag_id)
            .filter(DagRun.dag_id.in_(filter_dag_ids))  # Only include accessible/selected DAGs.
            .subquery("last_runs")
        )

        query = session.query(
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

        def _datetime_to_string(value: Optional[DateTime]) -> Optional[str]:
            if value is None:
                return None
            return value.isoformat()

        resp = {
            r.dag_id.replace('.', '__dot__'): {
                "dag_id": r.dag_id,
                "state": r.state,
                "execution_date": _datetime_to_string(r.execution_date),
                "start_date": _datetime_to_string(r.start_date),
                "end_date": _datetime_to_string(r.end_date),
                "data_interval_start": _datetime_to_string(r.data_interval_start),
                "data_interval_end": _datetime_to_string(r.data_interval_end),
            }
            for r in query
        }
        return wwwutils.json_response(resp)

    @expose('/code')
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_CODE),
        ]
    )
    @provide_session
    def code(self, session=None):
        """Dag Code."""
        all_errors = ""
        dag_orm = None
        dag_id = None

        try:
            dag_id = request.args.get('dag_id')
            dag_orm = DagModel.get_dagmodel(dag_id, session=session)
            code = DagCode.get_code_by_fileloc(dag_orm.fileloc)
            html_code = Markup(highlight(code, lexers.PythonLexer(), HtmlFormatter(linenos=True)))

        except Exception as e:
            all_errors += (
                "Exception encountered during "
                + f"dag_id retrieval/dag retrieval fallback/code highlighting:\n\n{e}\n"
            )
            html_code = Markup('<p>Failed to load DAG file Code.</p><p>Details: {}</p>').format(
                escape(all_errors)
            )

        wwwutils.check_import_errors(dag_orm.fileloc, session)

        return self.render_template(
            'airflow/dag_code.html',
            html_code=html_code,
            dag=dag_orm,
            dag_model=dag_orm,
            title=dag_id,
            root=request.args.get('root'),
            wrapped=conf.getboolean('webserver', 'default_wrap'),
        )

    @expose('/dag_details')
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        ]
    )
    @provide_session
    def dag_details(self, session=None):
        """Get Dag details."""
        dag_id = request.args.get('dag_id')
        dag = current_app.dag_bag.get_dag(dag_id)
        dag_model = DagModel.get_dagmodel(dag_id)

        title = "DAG Details"
        root = request.args.get('root', '')

        wwwutils.check_import_errors(dag.fileloc, session)

        states = (
            session.query(TaskInstance.state, sqla.func.count(TaskInstance.dag_id))
            .filter(TaskInstance.dag_id == dag_id)
            .group_by(TaskInstance.state)
            .all()
        )

        active_runs = models.DagRun.find(dag_id=dag_id, state=State.RUNNING, external_trigger=False)

        tags = session.query(models.DagTag).filter(models.DagTag.dag_id == dag_id).all()

        return self.render_template(
            'airflow/dag_details.html',
            dag=dag,
            title=title,
            root=root,
            states=states,
            State=State,
            active_runs=active_runs,
            tags=tags,
            dag_model=dag_model,
        )

    @expose('/rendered-templates')
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        ]
    )
    @action_logging
    @provide_session
    def rendered_templates(self, session):
        """Get rendered Dag."""
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        dttm = timezone.parse(execution_date)
        form = DateTimeForm(data={'execution_date': dttm})
        root = request.args.get('root', '')

        logging.info("Retrieving rendered templates.")
        dag = current_app.dag_bag.get_dag(dag_id)
        dag_run = dag.get_dagrun(execution_date=dttm, session=session)
        task = copy.copy(dag.get_task(task_id))

        if dag_run is None:
            # No DAG run matching given logical date. This usually means this
            # DAG has never been run. Task instance rendering does not really
            # make sense in this situation, but "works" prior to AIP-39. This
            # "fakes" a temporary DagRun-TaskInstance association (not saved to
            # database) for presentation only.
            ti = TaskInstance(task)
            ti.dag_run = DagRun(dag_id=dag_id, execution_date=dttm)
        else:
            ti = dag_run.get_task_instance(task_id=task.task_id, session=session)
            ti.refresh_from_task(task)

        try:
            ti.get_rendered_template_fields(session=session)
        except AirflowException as e:
            msg = "Error rendering template: " + escape(e)
            if e.__cause__:
                msg += Markup("<br><br>OriginalError: ") + escape(e.__cause__)
            flash(msg, "error")
        except Exception as e:
            flash("Error rendering template: " + str(e), "error")

        title = "Rendered Template"
        html_dict = {}
        renderers = wwwutils.get_attr_renderer()

        for template_field in task.template_fields:
            content = getattr(task, template_field)
            renderer = task.template_fields_renderers.get(template_field, template_field)
            if renderer in renderers:
                if isinstance(content, (dict, list)):
                    json_content = json.dumps(content, sort_keys=True, indent=4)
                    html_dict[template_field] = renderers[renderer](json_content)
                else:
                    html_dict[template_field] = renderers[renderer](content)
            else:
                html_dict[template_field] = Markup("<pre><code>{}</pre></code>").format(pformat(content))

            if isinstance(content, dict):
                if template_field == 'op_kwargs':
                    for key, value in content.items():
                        renderer = task.template_fields_renderers.get(key, key)
                        if renderer in renderers:
                            html_dict['.'.join([template_field, key])] = renderers[renderer](value)
                        else:
                            html_dict['.'.join([template_field, key])] = Markup(
                                "<pre><code>{}</pre></code>"
                            ).format(pformat(value))
                else:
                    for dict_keys in get_key_paths(content):
                        template_path = '.'.join((template_field, dict_keys))
                        renderer = task.template_fields_renderers.get(template_path, template_path)
                        if renderer in renderers:
                            content_value = get_value_from_path(dict_keys, content)
                            html_dict[template_path] = renderers[renderer](content_value)

        return self.render_template(
            'airflow/ti_code.html',
            html_dict=html_dict,
            dag=dag,
            task_id=task_id,
            execution_date=execution_date,
            form=form,
            root=root,
            title=title,
        )

    @expose('/rendered-k8s')
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        ]
    )
    @action_logging
    def rendered_k8s(self):
        """Get rendered k8s yaml."""
        if not settings.IS_K8S_OR_K8SCELERY_EXECUTOR:
            abort(404)
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        dttm = timezone.parse(execution_date)
        form = DateTimeForm(data={'execution_date': dttm})
        root = request.args.get('root', '')
        logging.info("Retrieving rendered templates.")
        dag = current_app.dag_bag.get_dag(dag_id)
        task = dag.get_task(task_id)
        dag_run = dag.get_dagrun(execution_date=dttm)
        ti = dag_run.get_task_instance(task_id=task.task_id)

        pod_spec = None
        try:
            pod_spec = ti.get_rendered_k8s_spec()
        except AirflowException as e:
            msg = "Error rendering Kubernetes POD Spec: " + escape(e)
            if e.__cause__:
                msg += Markup("<br><br>OriginalError: ") + escape(e.__cause__)
            flash(msg, "error")
        except Exception as e:
            flash("Error rendering Kubernetes Pod Spec: " + str(e), "error")
        title = "Rendered K8s Pod Spec"
        html_dict = {}
        renderers = wwwutils.get_attr_renderer()
        if pod_spec:
            content = yaml.dump(pod_spec)
            content = renderers["yaml"](content)
        else:
            content = Markup("<pre><code>Error rendering Kubernetes POD Spec</pre></code>")
        html_dict['k8s'] = content

        return self.render_template(
            'airflow/ti_code.html',
            html_dict=html_dict,
            dag=dag,
            task_id=task_id,
            execution_date=execution_date,
            form=form,
            root=root,
            title=title,
        )

    @expose('/get_logs_with_metadata')
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_LOG),
        ]
    )
    @action_logging
    @provide_session
    def get_logs_with_metadata(self, session=None):
        """Retrieve logs including metadata."""
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        try_number = request.args.get('try_number', type=int)
        metadata = request.args.get('metadata')
        metadata = json.loads(metadata)
        response_format = request.args.get('format', 'json')

        # metadata may be null
        if not metadata:
            metadata = {}

        # Convert string datetime into actual datetime
        try:
            execution_date = timezone.parse(execution_date)
        except ValueError:
            error_message = (
                'Given execution date, {}, could not be identified '
                'as a date. Example date format: 2015-11-16T14:34:15+00:00'.format(execution_date)
            )
            response = jsonify({'error': error_message})
            response.status_code = 400

            return response

        task_log_reader = TaskLogReader()
        if not task_log_reader.supports_read:
            return jsonify(
                message="Task log handler does not support read logs.",
                error=True,
                metadata={"end_of_log": True},
            )

        ti = (
            session.query(models.TaskInstance)
            .filter(
                models.TaskInstance.dag_id == dag_id,
                models.TaskInstance.task_id == task_id,
                models.TaskInstance.execution_date == execution_date,
            )
            .first()
        )

        if ti is None:
            return jsonify(
                message="*** Task instance did not exist in the DB\n",
                error=True,
                metadata={"end_of_log": True},
            )

        try:
            dag = current_app.dag_bag.get_dag(dag_id)
            if dag:
                ti.task = dag.get_task(ti.task_id)

            if response_format == 'json':
                logs, metadata = task_log_reader.read_log_chunks(ti, try_number, metadata)
                message = logs[0] if try_number is not None else logs
                return jsonify(message=message, metadata=metadata)

            metadata['download_logs'] = True
            attachment_filename = task_log_reader.render_log_filename(ti, try_number)
            log_stream = task_log_reader.read_log_stream(ti, try_number, metadata)
            return Response(
                response=log_stream,
                mimetype="text/plain",
                headers={"Content-Disposition": f"attachment; filename={attachment_filename}"},
            )
        except AttributeError as e:
            error_message = [f"Task log handler does not support read logs.\n{str(e)}\n"]
            metadata['end_of_log'] = True
            return jsonify(message=error_message, error=True, metadata=metadata)

    @expose('/log')
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_LOG),
        ]
    )
    @action_logging
    @provide_session
    def log(self, session=None):
        """Retrieve log."""
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        dttm = timezone.parse(execution_date)
        form = DateTimeForm(data={'execution_date': dttm})
        dag_model = DagModel.get_dagmodel(dag_id)

        ti = (
            session.query(models.TaskInstance)
            .filter(
                models.TaskInstance.dag_id == dag_id,
                models.TaskInstance.task_id == task_id,
                models.TaskInstance.execution_date == dttm,
            )
            .first()
        )

        num_logs = 0
        if ti is not None:
            num_logs = ti.next_try_number - 1
            if ti.state in (State.UP_FOR_RESCHEDULE, State.DEFERRED):
                # Tasks in reschedule state decremented the try number
                num_logs += 1
        logs = [''] * num_logs
        root = request.args.get('root', '')
        return self.render_template(
            'airflow/ti_log.html',
            logs=logs,
            dag=dag_model,
            title="Log by attempts",
            dag_id=dag_id,
            task_id=task_id,
            execution_date=execution_date,
            form=form,
            root=root,
            wrapped=conf.getboolean('webserver', 'default_wrap'),
        )

    @expose('/redirect_to_external_log')
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_LOG),
        ]
    )
    @action_logging
    @provide_session
    def redirect_to_external_log(self, session=None):
        """Redirects to external log."""
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        dttm = timezone.parse(execution_date)
        try_number = request.args.get('try_number', 1)

        ti = (
            session.query(models.TaskInstance)
            .filter(
                models.TaskInstance.dag_id == dag_id,
                models.TaskInstance.task_id == task_id,
                models.TaskInstance.execution_date == dttm,
            )
            .first()
        )

        if not ti:
            flash(f"Task [{dag_id}.{task_id}] does not exist", "error")
            return redirect(url_for('Airflow.index'))

        task_log_reader = TaskLogReader()
        if not task_log_reader.supports_external_link:
            flash("Task log handler does not support external links", "error")
            return redirect(url_for('Airflow.index'))

        handler = task_log_reader.log_handler
        url = handler.get_external_log_url(ti, try_number)
        return redirect(url)

    @expose('/task')
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        ]
    )
    @action_logging
    @provide_session
    def task(self, session):
        """Retrieve task."""
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        dttm = timezone.parse(execution_date)
        form = DateTimeForm(data={'execution_date': dttm})
        root = request.args.get('root', '')
        dag = current_app.dag_bag.get_dag(dag_id)

        if not dag or task_id not in dag.task_ids:
            flash(f"Task [{dag_id}.{task_id}] doesn't seem to exist at the moment", "error")
            return redirect(url_for('Airflow.index'))
        task = copy.copy(dag.get_task(task_id))
        task.resolve_template_files()

        ti: Optional[TaskInstance] = (
            session.query(TaskInstance)
            .options(
                # HACK: Eager-load relationships. This is needed because
                # multiple properties mis-use provide_session() that destroys
                # the session object ti is bounded to.
                joinedload(TaskInstance.queued_by_job, innerjoin=False),
                joinedload(TaskInstance.trigger, innerjoin=False),
            )
            .join(TaskInstance.dag_run)
            .filter(
                DagRun.execution_date == dttm,
                TaskInstance.dag_id == dag_id,
                TaskInstance.task_id == task_id,
            )
            .one_or_none()
        )
        if ti is None:
            ti_attrs: Optional[List[Tuple[str, Any]]] = None
        else:
            ti.refresh_from_task(task)
            all_ti_attrs = ((name, getattr(ti, name)) for name in dir(ti) if not name.startswith("_"))
            ti_attrs = sorted((name, attr) for name, attr in all_ti_attrs if not callable(attr))

        attr_renderers = wwwutils.get_attr_renderer()
        task_attrs = [
            (attr_name, attr)
            for attr_name, attr in (
                (attr_name, getattr(task, attr_name))
                for attr_name in dir(task)
                if not attr_name.startswith("_") and attr_name not in attr_renderers
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
                    if ti and ti.state == State.NONE
                    else ""
                ),
            )
        ]

        # Use the scheduler's context to figure out which dependencies are not met
        if ti is None:
            failed_dep_reasons: List[Tuple[str, str]] = []
        else:
            dep_context = DepContext(SCHEDULER_QUEUED_DEPS)
            failed_dep_reasons = [
                (dep.dep_name, dep.reason) for dep in ti.get_failed_dep_statuses(dep_context=dep_context)
            ]

        title = "Task Instance Details"
        return self.render_template(
            'airflow/task.html',
            task_attrs=task_attrs,
            ti_attrs=ti_attrs,
            failed_dep_reasons=failed_dep_reasons or no_failed_deps_result,
            task_id=task_id,
            execution_date=execution_date,
            special_attrs_rendered=special_attrs_rendered,
            form=form,
            root=root,
            dag=dag,
            title=title,
        )

    @expose('/xcom')
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_XCOM),
        ]
    )
    @action_logging
    @provide_session
    def xcom(self, session=None):
        """Retrieve XCOM."""
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        # Carrying execution_date through, even though it's irrelevant for
        # this context
        execution_date = request.args.get('execution_date')
        dttm = timezone.parse(execution_date)
        form = DateTimeForm(data={'execution_date': dttm})
        root = request.args.get('root', '')
        ti_db = models.TaskInstance
        dag = DagModel.get_dagmodel(dag_id)
        ti = session.query(ti_db).filter(and_(ti_db.dag_id == dag_id, ti_db.task_id == task_id)).first()

        if not ti:
            flash(f"Task [{dag_id}.{task_id}] doesn't seem to exist at the moment", "error")
            return redirect(url_for('Airflow.index'))

        xcomlist = (
            session.query(XCom)
            .filter(XCom.dag_id == dag_id, XCom.task_id == task_id, XCom.execution_date == dttm)
            .all()
        )

        attributes = []
        for xcom in xcomlist:
            if not xcom.key.startswith('_'):
                attributes.append((xcom.key, xcom.value))

        title = "XCom"
        return self.render_template(
            'airflow/xcom.html',
            attributes=attributes,
            task_id=task_id,
            execution_date=execution_date,
            form=form,
            root=root,
            dag=dag,
            title=title,
        )

    @expose('/run', methods=['POST'])
    @auth.has_access(
        [
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_TASK_INSTANCE),
        ]
    )
    @action_logging
    def run(self):
        """Runs Task Instance."""
        dag_id = request.form.get('dag_id')
        task_id = request.form.get('task_id')
        origin = get_safe_url(request.form.get('origin'))
        dag = current_app.dag_bag.get_dag(dag_id)
        task = dag.get_task(task_id)

        execution_date = request.form.get('execution_date')
        execution_date = timezone.parse(execution_date)
        ignore_all_deps = request.form.get('ignore_all_deps') == "true"
        ignore_task_deps = request.form.get('ignore_task_deps') == "true"
        ignore_ti_state = request.form.get('ignore_ti_state') == "true"

        executor = ExecutorLoader.get_default_executor()
        valid_celery_config = False
        valid_kubernetes_config = False
        valid_celery_kubernetes_config = False

        try:
            from airflow.executors.celery_executor import CeleryExecutor

            valid_celery_config = isinstance(executor, CeleryExecutor)
        except ImportError:
            pass

        try:
            from airflow.executors.kubernetes_executor import KubernetesExecutor

            valid_kubernetes_config = isinstance(executor, KubernetesExecutor)
        except ImportError:
            pass
        try:
            from airflow.executors.celery_kubernetes_executor import CeleryKubernetesExecutor

            valid_celery_kubernetes_config = isinstance(executor, CeleryKubernetesExecutor)
        except ImportError:
            pass

        if not valid_celery_config and not valid_kubernetes_config and not valid_celery_kubernetes_config:
            flash("Only works with the Celery, CeleryKubernetes or Kubernetes executors, sorry", "error")
            return redirect(origin)

        dag_run = dag.get_dagrun(execution_date=execution_date)
        ti = dag_run.get_task_instance(task_id=task.task_id)
        if not ti:
            flash(
                "Could not queue task instance for execution, task instance is missing",
                "error",
            )
            return redirect(origin)

        ti.refresh_from_task(task)

        # Make sure the task instance can be run
        dep_context = DepContext(
            deps=RUNNING_DEPS,
            ignore_all_deps=ignore_all_deps,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
        )
        failed_deps = list(ti.get_failed_dep_statuses(dep_context=dep_context))
        if failed_deps:
            failed_deps_str = ", ".join(f"{dep.dep_name}: {dep.reason}" for dep in failed_deps)
            flash(
                "Could not queue task instance for execution, dependencies not met: "
                "{}".format(failed_deps_str),
                "error",
            )
            return redirect(origin)

        executor.job_id = "manual"
        executor.start()
        executor.queue_task_instance(
            ti,
            ignore_all_deps=ignore_all_deps,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
        )
        executor.heartbeat()
        flash(f"Sent {ti} to the message queue, it should start any moment now.")
        return redirect(origin)

    @expose('/delete', methods=['POST'])
    @auth.has_access(
        [
            (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_DAG),
        ]
    )
    @action_logging
    def delete(self):
        """Deletes DAG."""
        from airflow.api.common.experimental import delete_dag
        from airflow.exceptions import DagNotFound

        dag_id = request.values.get('dag_id')
        origin = get_safe_url(request.values.get('origin'))

        try:
            delete_dag.delete_dag(dag_id)
        except DagNotFound:
            flash(f"DAG with id {dag_id} not found. Cannot delete", 'error')
            return redirect(request.referrer)
        except AirflowException:
            flash(
                f"Cannot delete DAG with id {dag_id} because some task instances of the DAG "
                "are still running. Please mark the  task instances as "
                "failed/succeeded before deleting the DAG",
                "error",
            )
            return redirect(request.referrer)

        flash(f"Deleting DAG with id {dag_id}. May take a couple minutes to fully disappear.")

        # Upon success return to origin.
        return redirect(origin)

    @expose('/trigger', methods=['POST', 'GET'])
    @auth.has_access(
        [
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_DAG_RUN),
        ]
    )
    @action_logging
    @provide_session
    def trigger(self, session=None):
        """Triggers DAG Run."""
        dag_id = request.values.get('dag_id')
        origin = get_safe_url(request.values.get('origin'))
        unpause = request.values.get('unpause')
        request_conf = request.values.get('conf')
        request_execution_date = request.values.get('execution_date', default=timezone.utcnow().isoformat())
        is_dag_run_conf_overrides_params = conf.getboolean('core', 'dag_run_conf_overrides_params')
        dag = current_app.dag_bag.get_dag(dag_id)
        dag_orm = session.query(models.DagModel).filter(models.DagModel.dag_id == dag_id).first()
        if not dag_orm:
            flash(f"Cannot find dag {dag_id}")
            return redirect(origin)

        if dag_orm.has_import_errors:
            flash(f"Cannot create dagruns because the dag {dag_id} has import errors", "error")
            return redirect(origin)

        if request.method == 'GET':
            # Populate conf textarea with conf requests parameter, or dag.params
            default_conf = ''

            doc_md = wwwutils.wrapped_markdown(getattr(dag, 'doc_md', None))
            form = DateTimeForm(data={'execution_date': request_execution_date})

            if request_conf:
                default_conf = request_conf
            else:
                try:
                    default_conf = json.dumps(
                        {str(k): v.resolve(suppress_exception=True) for k, v in dag.params.items()}, indent=4
                    )
                except TypeError:
                    flash("Could not pre-populate conf field due to non-JSON-serializable data-types")
            return self.render_template(
                'airflow/trigger.html',
                dag_id=dag_id,
                origin=origin,
                conf=default_conf,
                doc_md=doc_md,
                form=form,
                is_dag_run_conf_overrides_params=is_dag_run_conf_overrides_params,
            )

        try:
            execution_date = timezone.parse(request_execution_date)
        except ParserError:
            flash("Invalid execution date", "error")
            form = DateTimeForm(data={'execution_date': timezone.utcnow().isoformat()})
            return self.render_template(
                'airflow/trigger.html',
                dag_id=dag_id,
                origin=origin,
                conf=request_conf,
                form=form,
                is_dag_run_conf_overrides_params=is_dag_run_conf_overrides_params,
            )

        dr = DagRun.find(dag_id=dag_id, execution_date=execution_date, run_type=DagRunType.MANUAL)
        if dr:
            flash(f"This run_id {dr.run_id} already exists")
            return redirect(origin)

        run_conf = {}
        if request_conf:
            try:
                run_conf = json.loads(request_conf)
                if not isinstance(run_conf, dict):
                    flash("Invalid JSON configuration, must be a dict", "error")
                    form = DateTimeForm(data={'execution_date': execution_date})
                    return self.render_template(
                        'airflow/trigger.html',
                        dag_id=dag_id,
                        origin=origin,
                        conf=request_conf,
                        form=form,
                        is_dag_run_conf_overrides_params=is_dag_run_conf_overrides_params,
                    )
            except json.decoder.JSONDecodeError:
                flash("Invalid JSON configuration, not parseable", "error")
                form = DateTimeForm(data={'execution_date': execution_date})
                return self.render_template(
                    'airflow/trigger.html',
                    dag_id=dag_id,
                    origin=origin,
                    conf=request_conf,
                    form=form,
                    is_dag_run_conf_overrides_params=is_dag_run_conf_overrides_params,
                )

        if unpause and dag.is_paused:
            models.DagModel.get_dagmodel(dag_id).set_is_paused(is_paused=False)

        try:
            dag.create_dagrun(
                run_type=DagRunType.MANUAL,
                execution_date=execution_date,
                data_interval=dag.timetable.infer_manual_data_interval(run_after=execution_date),
                state=State.QUEUED,
                conf=run_conf,
                external_trigger=True,
                dag_hash=current_app.dag_bag.dags_hash.get(dag_id),
            )
        except ValueError as ve:
            flash(f"{ve}", "error")
            form = DateTimeForm(data={'execution_date': execution_date})
            return self.render_template(
                'airflow/trigger.html',
                dag_id=dag_id,
                origin=origin,
                conf=request_conf,
                form=form,
                is_dag_run_conf_overrides_params=is_dag_run_conf_overrides_params,
            )

        flash(f"Triggered {dag_id}, it should start any moment now.")
        return redirect(origin)

    def _clear_dag_tis(
        self, dag, start_date, end_date, origin, recursive=False, confirmed=False, only_failed=False
    ):
        if confirmed:
            count = dag.clear(
                start_date=start_date,
                end_date=end_date,
                include_subdags=recursive,
                include_parentdag=recursive,
                only_failed=only_failed,
            )

            flash(f"{count} task instances have been cleared")
            return redirect(origin)

        try:
            tis = dag.clear(
                start_date=start_date,
                end_date=end_date,
                include_subdags=recursive,
                include_parentdag=recursive,
                only_failed=only_failed,
                dry_run=True,
            )
        except AirflowException as ex:
            flash(str(ex), 'error')
            return redirect(origin)

        if not tis:
            flash("No task instances to clear", 'error')
            response = redirect(origin)
        else:
            details = "\n".join(str(t) for t in tis)

            response = self.render_template(
                'airflow/confirm.html',
                endpoint=None,
                message="Here's the list of task instances you are about to clear:",
                details=details,
            )

        return response

    @expose('/clear', methods=['POST'])
    @auth.has_access(
        [
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_TASK_INSTANCE),
        ]
    )
    @action_logging
    def clear(self):
        """Clears the Dag."""
        dag_id = request.form.get('dag_id')
        task_id = request.form.get('task_id')
        origin = get_safe_url(request.form.get('origin'))
        dag = current_app.dag_bag.get_dag(dag_id)

        execution_date = request.form.get('execution_date')
        execution_date = timezone.parse(execution_date)
        confirmed = request.form.get('confirmed') == "true"
        upstream = request.form.get('upstream') == "true"
        downstream = request.form.get('downstream') == "true"
        future = request.form.get('future') == "true"
        past = request.form.get('past') == "true"
        recursive = request.form.get('recursive') == "true"
        only_failed = request.form.get('only_failed') == "true"

        dag = dag.partial_subset(
            task_ids_or_regex=fr"^{task_id}$",
            include_downstream=downstream,
            include_upstream=upstream,
        )

        end_date = execution_date if not future else None
        start_date = execution_date if not past else None

        return self._clear_dag_tis(
            dag,
            start_date,
            end_date,
            origin,
            recursive=recursive,
            confirmed=confirmed,
            only_failed=only_failed,
        )

    @expose('/dagrun_clear', methods=['POST'])
    @auth.has_access(
        [
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_TASK_INSTANCE),
        ]
    )
    @action_logging
    def dagrun_clear(self):
        """Clears the DagRun"""
        dag_id = request.form.get('dag_id')
        origin = get_safe_url(request.form.get('origin'))
        execution_date = request.form.get('execution_date')
        confirmed = request.form.get('confirmed') == "true"

        dag = current_app.dag_bag.get_dag(dag_id)
        execution_date = timezone.parse(execution_date)
        start_date = execution_date
        end_date = execution_date

        return self._clear_dag_tis(dag, start_date, end_date, origin, recursive=True, confirmed=confirmed)

    @expose('/blocked', methods=['POST'])
    @auth.has_access(
        [
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        ]
    )
    @provide_session
    def blocked(self, session=None):
        """Mark Dag Blocked."""
        allowed_dag_ids = current_app.appbuilder.sm.get_accessible_dag_ids(g.user)

        # Filter by post parameters
        selected_dag_ids = {unquote(dag_id) for dag_id in request.form.getlist('dag_ids') if dag_id}

        if selected_dag_ids:
            filter_dag_ids = selected_dag_ids.intersection(allowed_dag_ids)
        else:
            filter_dag_ids = allowed_dag_ids

        if not filter_dag_ids:
            return wwwutils.json_response([])

        dags = (
            session.query(DagRun.dag_id, sqla.func.count(DagRun.id))
            .filter(DagRun.state == State.RUNNING)
            .filter(DagRun.dag_id.in_(filter_dag_ids))
            .group_by(DagRun.dag_id)
        )

        payload = []
        for dag_id, active_dag_runs in dags:
            max_active_runs = 0
            dag = current_app.dag_bag.get_dag(dag_id)
            if dag:
                # TODO: Make max_active_runs a column so we can query for it directly
                max_active_runs = dag.max_active_runs
            payload.append(
                {
                    'dag_id': dag_id,
                    'active_dag_run': active_dag_runs,
                    'max_active_runs': max_active_runs,
                }
            )
        return wwwutils.json_response(payload)

    def _mark_dagrun_state_as_failed(self, dag_id, execution_date, confirmed, origin):
        if not execution_date:
            flash('Invalid execution date', 'error')
            return redirect(origin)

        execution_date = timezone.parse(execution_date)
        dag = current_app.dag_bag.get_dag(dag_id)

        if not dag:
            flash(f'Cannot find DAG: {dag_id}', 'error')
            return redirect(origin)

        new_dag_state = set_dag_run_state_to_failed(dag, execution_date, commit=confirmed)

        if confirmed:
            flash(f'Marked failed on {len(new_dag_state)} task instances')
            return redirect(origin)

        else:
            details = '\n'.join(str(t) for t in new_dag_state)

            response = self.render_template(
                'airflow/confirm.html',
                message="Here's the list of task instances you are about to mark as failed",
                details=details,
            )

            return response

    def _mark_dagrun_state_as_success(self, dag_id, execution_date, confirmed, origin):
        if not execution_date:
            flash('Invalid execution date', 'error')
            return redirect(origin)

        execution_date = timezone.parse(execution_date)
        dag = current_app.dag_bag.get_dag(dag_id)

        if not dag:
            flash(f'Cannot find DAG: {dag_id}', 'error')
            return redirect(origin)

        new_dag_state = set_dag_run_state_to_success(dag, execution_date, commit=confirmed)

        if confirmed:
            flash(f'Marked success on {len(new_dag_state)} task instances')
            return redirect(origin)

        else:
            details = '\n'.join(str(t) for t in new_dag_state)

            response = self.render_template(
                'airflow/confirm.html',
                message="Here's the list of task instances you are about to mark as success",
                details=details,
            )

            return response

    @expose('/dagrun_failed', methods=['POST'])
    @auth.has_access(
        [
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG_RUN),
        ]
    )
    @action_logging
    def dagrun_failed(self):
        """Mark DagRun failed."""
        dag_id = request.form.get('dag_id')
        execution_date = request.form.get('execution_date')
        confirmed = request.form.get('confirmed') == 'true'
        origin = get_safe_url(request.form.get('origin'))
        return self._mark_dagrun_state_as_failed(dag_id, execution_date, confirmed, origin)

    @expose('/dagrun_success', methods=['POST'])
    @auth.has_access(
        [
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG_RUN),
        ]
    )
    @action_logging
    def dagrun_success(self):
        """Mark DagRun success"""
        dag_id = request.form.get('dag_id')
        execution_date = request.form.get('execution_date')
        confirmed = request.form.get('confirmed') == 'true'
        origin = get_safe_url(request.form.get('origin'))
        return self._mark_dagrun_state_as_success(dag_id, execution_date, confirmed, origin)

    def _mark_task_instance_state(
        self,
        dag_id,
        task_id,
        origin,
        execution_date,
        upstream,
        downstream,
        future,
        past,
        state,
    ):
        dag = current_app.dag_bag.get_dag(dag_id)
        latest_execution_date = dag.get_latest_execution_date()

        if not latest_execution_date:
            flash(f"Cannot mark tasks as {state}, seem that dag {dag_id} has never run", "error")
            return redirect(origin)

        execution_date = timezone.parse(execution_date)

        altered = dag.set_task_instance_state(
            task_id, execution_date, state, upstream=upstream, downstream=downstream, future=future, past=past
        )

        flash(f"Marked {state} on {len(altered)} task instances")
        return redirect(origin)

    @expose('/confirm', methods=['GET'])
    @auth.has_access(
        [
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
        ]
    )
    @action_logging
    def confirm(self):
        """Show confirmation page for marking tasks as success or failed."""
        args = request.args
        dag_id = args.get('dag_id')
        task_id = args.get('task_id')
        execution_date = args.get('execution_date')
        state = args.get('state')

        upstream = to_boolean(args.get('upstream'))
        downstream = to_boolean(args.get('downstream'))
        future = to_boolean(args.get('future'))
        past = to_boolean(args.get('past'))

        dag = current_app.dag_bag.get_dag(dag_id)
        if not dag:
            flash(f'DAG {dag_id} not found', "error")
            return redirect(request.referrer or url_for('Airflow.index'))

        try:
            task = dag.get_task(task_id)
        except airflow.exceptions.TaskNotFound:
            flash(f"Task {task_id} not found", "error")
            return redirect(request.referrer or url_for('Airflow.index'))

        task.dag = dag

        if state not in (
            'success',
            'failed',
        ):
            flash(f"Invalid state {state}, must be either 'success' or 'failed'", "error")
            return redirect(request.referrer or url_for('Airflow.index'))

        latest_execution_date = dag.get_latest_execution_date()
        if not latest_execution_date:
            flash(f"Cannot mark tasks as {state}, seem that dag {dag_id} has never run", "error")
            return redirect(request.referrer or url_for('Airflow.index'))

        execution_date = timezone.parse(execution_date)

        from airflow.api.common.experimental.mark_tasks import set_state

        to_be_altered = set_state(
            tasks=[task],
            execution_date=execution_date,
            upstream=upstream,
            downstream=downstream,
            future=future,
            past=past,
            state=state,
            commit=False,
        )

        details = "\n".join(str(t) for t in to_be_altered)

        response = self.render_template(
            "airflow/confirm.html",
            endpoint=url_for(f'Airflow.{state}'),
            message=f"Here's the list of task instances you are about to mark as {state}:",
            details=details,
        )

        return response

    @expose('/failed', methods=['POST'])
    @auth.has_access(
        [
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
        ]
    )
    @action_logging
    def failed(self):
        """Mark task as failed."""
        args = request.form
        dag_id = args.get('dag_id')
        task_id = args.get('task_id')
        origin = get_safe_url(args.get('origin'))
        execution_date = args.get('execution_date')

        upstream = to_boolean(args.get('upstream'))
        downstream = to_boolean(args.get('downstream'))
        future = to_boolean(args.get('future'))
        past = to_boolean(args.get('past'))

        return self._mark_task_instance_state(
            dag_id,
            task_id,
            origin,
            execution_date,
            upstream,
            downstream,
            future,
            past,
            State.FAILED,
        )

    @expose('/success', methods=['POST'])
    @auth.has_access(
        [
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
        ]
    )
    @action_logging
    def success(self):
        """Mark task as success."""
        args = request.form
        dag_id = args.get('dag_id')
        task_id = args.get('task_id')
        origin = get_safe_url(args.get('origin'))
        execution_date = args.get('execution_date')

        upstream = to_boolean(args.get('upstream'))
        downstream = to_boolean(args.get('downstream'))
        future = to_boolean(args.get('future'))
        past = to_boolean(args.get('past'))

        return self._mark_task_instance_state(
            dag_id,
            task_id,
            origin,
            execution_date,
            upstream,
            downstream,
            future,
            past,
            State.SUCCESS,
        )

    def _get_tree_data(
        self,
        dag_runs: Iterable[DagRun],
        dag: DAG,
        base_date: DateTime,
        session: settings.Session,
    ):
        """Returns formatted dag_runs for Tree view"""
        dates = sorted(dag_runs.keys())
        min_date = min(dag_runs, default=None)

        task_instances = {
            (ti.task_id, ti.execution_date): ti
            for ti in dag.get_task_instances(start_date=min_date, end_date=base_date, session=session)
        }

        expanded = set()
        # The default recursion traces every path so that tree view has full
        # expand/collapse functionality. After 5,000 nodes we stop and fall
        # back on a quick DFS search for performance. See PR #320.
        node_count = 0
        node_limit = 5000 / max(1, len(dag.leaves))

        def encode_ti(task_instance: Optional[models.TaskInstance]) -> Optional[List]:
            if not task_instance:
                return None

            # NOTE: order of entry is important here because client JS relies on it for
            # tree node reconstruction. Remember to change JS code in tree.html
            # whenever order is altered.
            task_instance_data = [
                task_instance.state,
                task_instance.try_number,
                None,  # start_ts
                None,  # duration
            ]

            if task_instance.start_date:
                # round to seconds to reduce payload size
                task_instance_data[2] = int(task_instance.start_date.timestamp())
                if task_instance.duration is not None:
                    task_instance_data[3] = truncate_task_duration(task_instance.duration)

            return task_instance_data

        def recurse_nodes(task, visited):
            nonlocal node_count
            node_count += 1
            visited.add(task)
            task_id = task.task_id

            node = {
                'name': task.task_id,
                'instances': [encode_ti(task_instances.get((task_id, d))) for d in dates],
                'num_dep': len(task.downstream_list),
                'operator': task.task_type,
                'retries': task.retries,
                'owner': task.owner,
                'ui_color': task.ui_color,
            }

            if task.downstream_list:
                children = [
                    recurse_nodes(t, visited)
                    for t in task.downstream_list
                    if node_count < node_limit or t not in visited
                ]

                # D3 tree uses children vs _children to define what is
                # expanded or not. The following block makes it such that
                # repeated nodes are collapsed by default.
                if task.task_id not in expanded:
                    children_key = 'children'
                    expanded.add(task.task_id)
                else:
                    children_key = "_children"
                node[children_key] = children

            if task.depends_on_past:
                node['depends_on_past'] = task.depends_on_past
            if task.start_date:
                # round to seconds to reduce payload size
                node['start_ts'] = int(task.start_date.timestamp())
                if task.end_date:
                    # round to seconds to reduce payload size
                    node['end_ts'] = int(task.end_date.timestamp())
            if task.extra_links:
                node['extra_links'] = task.extra_links
            return node

        return {
            'name': '[DAG]',
            'children': [recurse_nodes(t, set()) for t in dag.roots],
            'instances': [dag_runs.get(d) or {'execution_date': d.isoformat()} for d in dates],
        }

    @expose('/tree')
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_LOG),
        ]
    )
    @gzipped
    @action_logging
    @provide_session
    def tree(self, session=None):
        """Get Dag as tree."""
        dag_id = request.args.get('dag_id')
        dag = current_app.dag_bag.get_dag(dag_id)
        dag_model = DagModel.get_dagmodel(dag_id)
        if not dag:
            flash(f'DAG "{dag_id}" seems to be missing from DagBag.', "error")
            return redirect(url_for('Airflow.index'))
        wwwutils.check_import_errors(dag.fileloc, session)

        root = request.args.get('root')
        if root:
            dag = dag.partial_subset(task_ids_or_regex=root, include_downstream=False, include_upstream=True)

        num_runs = request.args.get('num_runs', type=int)
        if num_runs is None:
            num_runs = conf.getint('webserver', 'default_dag_run_display_number')

        try:
            base_date = timezone.parse(request.args["base_date"])
        except (KeyError, ValueError):
            base_date = dag.get_latest_execution_date() or timezone.utcnow()

        dag_runs = (
            session.query(DagRun)
            .filter(DagRun.dag_id == dag.dag_id, DagRun.execution_date <= base_date)
            .order_by(DagRun.execution_date.desc())
            .limit(num_runs)
            .all()
        )
        dag_runs = {dr.execution_date: alchemy_to_dict(dr) for dr in dag_runs}

        max_date = max(dag_runs.keys(), default=None)

        form = DateTimeWithNumRunsForm(
            data={
                'base_date': max_date or timezone.utcnow(),
                'num_runs': num_runs,
            }
        )

        doc_md = wwwutils.wrapped_markdown(getattr(dag, 'doc_md', None))

        task_log_reader = TaskLogReader()
        if task_log_reader.supports_external_link:
            external_log_name = task_log_reader.log_handler.log_name
        else:
            external_log_name = None

        data = self._get_tree_data(dag_runs, dag, base_date, session=session)

        # avoid spaces to reduce payload size
        data = htmlsafe_json_dumps(data, separators=(',', ':'))

        return self.render_template(
            'airflow/tree.html',
            operators=sorted({op.task_type: op for op in dag.tasks}.values(), key=lambda x: x.task_type),
            root=root,
            form=form,
            dag=dag,
            doc_md=doc_md,
            data=data,
            num_runs=num_runs,
            show_external_log_redirect=task_log_reader.supports_external_link,
            external_log_name=external_log_name,
            dag_model=dag_model,
            auto_refresh_interval=conf.getint('webserver', 'auto_refresh_interval'),
        )

    @expose('/calendar')
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        ]
    )
    @gzipped
    @action_logging
    @provide_session
    def calendar(self, session=None):
        """Get DAG runs as calendar"""

        def _convert_to_date(session, column):
            """Convert column to date."""
            if session.bind.dialect.name == 'mssql':
                return column.cast(Date)
            else:
                return func.date(column)

        dag_id = request.args.get('dag_id')
        dag = current_app.dag_bag.get_dag(dag_id)
        dag_model = DagModel.get_dagmodel(dag_id)
        if not dag:
            flash(f'DAG "{dag_id}" seems to be missing from DagBag.', "error")
            return redirect(url_for('Airflow.index'))

        wwwutils.check_import_errors(dag.fileloc, session)

        root = request.args.get('root')
        if root:
            dag = dag.partial_subset(task_ids_or_regex=root, include_downstream=False, include_upstream=True)

        dag_states = (
            session.query(
                (_convert_to_date(session, DagRun.execution_date)).label('date'),
                DagRun.state,
                func.count('*').label('count'),
            )
            .filter(DagRun.dag_id == dag.dag_id)
            .group_by(_convert_to_date(session, DagRun.execution_date), DagRun.state)
            .order_by(_convert_to_date(session, DagRun.execution_date).asc())
            .all()
        )

        dag_states = [
            {
                # DATE() in SQLite and MySQL behave differently:
                # SQLite returns a string, MySQL returns a date.
                'date': dr.date if isinstance(dr.date, str) else dr.date.isoformat(),
                'state': dr.state,
                'count': dr.count,
            }
            for dr in dag_states
        ]

        data = {
            'dag_states': dag_states,
            'start_date': (dag.start_date or DateTime.utcnow()).date().isoformat(),
            'end_date': (dag.end_date or DateTime.utcnow()).date().isoformat(),
        }

        doc_md = wwwutils.wrapped_markdown(getattr(dag, 'doc_md', None))

        # avoid spaces to reduce payload size
        data = htmlsafe_json_dumps(data, separators=(',', ':'))

        return self.render_template(
            'airflow/calendar.html',
            dag=dag,
            doc_md=doc_md,
            data=data,
            root=root,
            dag_model=dag_model,
        )

    @expose('/graph')
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_LOG),
        ]
    )
    @gzipped
    @action_logging
    @provide_session
    def graph(self, session=None):
        """Get DAG as Graph."""
        dag_id = request.args.get('dag_id')
        dag = current_app.dag_bag.get_dag(dag_id)
        dag_model = DagModel.get_dagmodel(dag_id)
        if not dag:
            flash(f'DAG "{dag_id}" seems to be missing.', "error")
            return redirect(url_for('Airflow.index'))
        wwwutils.check_import_errors(dag.fileloc, session)

        root = request.args.get('root')
        if root:
            dag = dag.partial_subset(task_ids_or_regex=root, include_upstream=True, include_downstream=False)
        arrange = request.args.get('arrange', dag.orientation)

        nodes = task_group_to_dict(dag.task_group)
        edges = dag_edges(dag)

        dt_nr_dr_data = get_date_time_num_runs_dag_runs_form_data(request, session, dag)
        dt_nr_dr_data['arrange'] = arrange
        dttm = dt_nr_dr_data['dttm']

        class GraphForm(DateTimeWithNumRunsWithDagRunsForm):
            """Graph Form class."""

            arrange = SelectField(
                "Layout",
                choices=(
                    ('LR', "Left > Right"),
                    ('RL', "Right > Left"),
                    ('TB', "Top > Bottom"),
                    ('BT', "Bottom > Top"),
                ),
            )

        form = GraphForm(data=dt_nr_dr_data)
        form.execution_date.choices = dt_nr_dr_data['dr_choices']

        task_instances = {ti.task_id: alchemy_to_dict(ti) for ti in dag.get_task_instances(dttm, dttm)}
        tasks = {
            t.task_id: {
                'dag_id': t.dag_id,
                'task_type': t.task_type,
                'extra_links': t.extra_links,
            }
            for t in dag.tasks
        }
        if not tasks:
            flash("No tasks found", "error")
        session.commit()
        doc_md = wwwutils.wrapped_markdown(getattr(dag, 'doc_md', None))

        task_log_reader = TaskLogReader()
        if task_log_reader.supports_external_link:
            external_log_name = task_log_reader.log_handler.log_name
        else:
            external_log_name = None

        return self.render_template(
            'airflow/graph.html',
            dag=dag,
            form=form,
            width=request.args.get('width', "100%"),
            height=request.args.get('height', "800"),
            execution_date=dttm.isoformat(),
            state_token=wwwutils.state_token(dt_nr_dr_data['dr_state']),
            doc_md=doc_md,
            arrange=arrange,
            operators=sorted({op.task_type: op for op in dag.tasks}.values(), key=lambda x: x.task_type),
            root=root or '',
            task_instances=task_instances,
            tasks=tasks,
            nodes=nodes,
            edges=edges,
            show_external_log_redirect=task_log_reader.supports_external_link,
            external_log_name=external_log_name,
            dag_run_state=dt_nr_dr_data['dr_state'],
            dag_model=dag_model,
            auto_refresh_interval=conf.getint('webserver', 'auto_refresh_interval'),
        )

    @expose('/duration')
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        ]
    )
    @action_logging
    @provide_session
    def duration(self, session=None):
        """Get Dag as duration graph."""
        default_dag_run = conf.getint('webserver', 'default_dag_run_display_number')
        dag_id = request.args.get('dag_id')
        dag_model = DagModel.get_dagmodel(dag_id)

        dag: Optional[DAG] = current_app.dag_bag.get_dag(dag_id)
        if dag is None:
            flash(f'DAG "{dag_id}" seems to be missing.', "error")
            return redirect(url_for('Airflow.index'))

        wwwutils.check_import_errors(dag.fileloc, session)

        base_date = request.args.get('base_date')
        num_runs = request.args.get('num_runs', default=default_dag_run, type=int)

        if base_date:
            base_date = timezone.parse(base_date)
        else:
            base_date = dag.get_latest_execution_date() or timezone.utcnow()

        root = request.args.get('root')
        if root:
            dag = dag.partial_subset(task_ids_or_regex=root, include_upstream=True, include_downstream=False)
        chart_height = wwwutils.get_chart_height(dag)
        chart = nvd3.lineChart(
            name="lineChart", x_is_date=True, height=chart_height, chart_attr=self.line_chart_attr
        )
        cum_chart = nvd3.lineChart(
            name="cumLineChart", x_is_date=True, height=chart_height, chart_attr=self.line_chart_attr
        )

        y_points = defaultdict(list)
        x_points = defaultdict(list)
        cumulative_y = defaultdict(list)

        task_instances = dag.get_task_instances_before(base_date, num_runs, session=session)
        if task_instances:
            min_date = task_instances[0].execution_date
        else:
            min_date = timezone.utc_epoch()
        ti_fails = (
            session.query(TaskFail)
            .filter(
                TaskFail.dag_id == dag.dag_id,
                TaskFail.execution_date >= min_date,
                TaskFail.execution_date <= base_date,
                TaskFail.task_id.in_([t.task_id for t in dag.tasks]),
            )
            .all()
        )

        fails_totals = defaultdict(int)
        for failed_task_instance in ti_fails:
            dict_key = (
                failed_task_instance.dag_id,
                failed_task_instance.task_id,
                failed_task_instance.execution_date,
            )
            if failed_task_instance.duration:
                fails_totals[dict_key] += failed_task_instance.duration

        for task_instance in task_instances:
            if task_instance.duration:
                date_time = wwwutils.epoch(task_instance.execution_date)
                x_points[task_instance.task_id].append(date_time)
                y_points[task_instance.task_id].append(float(task_instance.duration))
                fails_dict_key = (task_instance.dag_id, task_instance.task_id, task_instance.execution_date)
                fails_total = fails_totals[fails_dict_key]
                cumulative_y[task_instance.task_id].append(float(task_instance.duration + fails_total))

        # determine the most relevant time unit for the set of task instance
        # durations for the DAG
        y_unit = infer_time_unit([d for t in y_points.values() for d in t])
        cum_y_unit = infer_time_unit([d for t in cumulative_y.values() for d in t])
        # update the y Axis on both charts to have the correct time units
        chart.create_y_axis('yAxis', format='.02f', custom_format=False, label=f'Duration ({y_unit})')
        chart.axislist['yAxis']['axisLabelDistance'] = '-15'
        cum_chart.create_y_axis('yAxis', format='.02f', custom_format=False, label=f'Duration ({cum_y_unit})')
        cum_chart.axislist['yAxis']['axisLabelDistance'] = '-15'

        for task_id in x_points:
            chart.add_serie(
                name=task_id,
                x=x_points[task_id],
                y=scale_time_units(y_points[task_id], y_unit),
            )
            cum_chart.add_serie(
                name=task_id,
                x=x_points[task_id],
                y=scale_time_units(cumulative_y[task_id], cum_y_unit),
            )

        dates = sorted({ti.execution_date for ti in task_instances})
        max_date = max(ti.execution_date for ti in task_instances) if dates else None

        session.commit()

        form = DateTimeWithNumRunsForm(
            data={
                'base_date': max_date or timezone.utcnow(),
                'num_runs': num_runs,
            }
        )
        chart.buildcontent()
        cum_chart.buildcontent()
        s_index = cum_chart.htmlcontent.rfind('});')
        cum_chart.htmlcontent = (
            cum_chart.htmlcontent[:s_index]
            + "$( document ).trigger('chartload')"
            + cum_chart.htmlcontent[s_index:]
        )

        return self.render_template(
            'airflow/duration_chart.html',
            dag=dag,
            root=root,
            form=form,
            chart=Markup(chart.htmlcontent),
            cum_chart=Markup(cum_chart.htmlcontent),
            dag_model=dag_model,
        )

    @expose('/tries')
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        ]
    )
    @action_logging
    @provide_session
    def tries(self, session=None):
        """Shows all tries."""
        default_dag_run = conf.getint('webserver', 'default_dag_run_display_number')
        dag_id = request.args.get('dag_id')
        dag = current_app.dag_bag.get_dag(dag_id)
        dag_model = DagModel.get_dagmodel(dag_id)
        base_date = request.args.get('base_date')
        num_runs = request.args.get('num_runs', default=default_dag_run, type=int)

        if base_date:
            base_date = timezone.parse(base_date)
        else:
            base_date = dag.get_latest_execution_date() or timezone.utcnow()

        wwwutils.check_import_errors(dag.fileloc, session)

        root = request.args.get('root')
        if root:
            dag = dag.partial_subset(task_ids_or_regex=root, include_upstream=True, include_downstream=False)

        chart_height = wwwutils.get_chart_height(dag)
        chart = nvd3.lineChart(
            name="lineChart",
            x_is_date=True,
            y_axis_format='d',
            height=chart_height,
            chart_attr=self.line_chart_attr,
        )

        tis = dag.get_task_instances_before(base_date, num_runs, session=session)
        for task in dag.tasks:
            y_points = []
            x_points = []
            for ti in tis:
                dttm = wwwutils.epoch(ti.execution_date)
                x_points.append(dttm)
                # y value should reflect completed tries to have a 0 baseline.
                y_points.append(ti.prev_attempted_tries)
            if x_points:
                chart.add_serie(name=task.task_id, x=x_points, y=y_points)

        tries = sorted({ti.try_number for ti in tis})
        max_date = max(ti.execution_date for ti in tis) if tries else None
        chart.create_y_axis('yAxis', format='.02f', custom_format=False, label='Tries')
        chart.axislist['yAxis']['axisLabelDistance'] = '-15'

        session.commit()

        form = DateTimeWithNumRunsForm(
            data={
                'base_date': max_date or timezone.utcnow(),
                'num_runs': num_runs,
            }
        )

        chart.buildcontent()

        return self.render_template(
            'airflow/chart.html',
            dag=dag,
            root=root,
            form=form,
            chart=Markup(chart.htmlcontent),
            tab_title='Tries',
            dag_model=dag_model,
        )

    @expose('/landing_times')
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        ]
    )
    @action_logging
    @provide_session
    def landing_times(self, session=None):
        """Shows landing times."""
        default_dag_run = conf.getint('webserver', 'default_dag_run_display_number')
        dag_id = request.args.get('dag_id')
        dag: DAG = current_app.dag_bag.get_dag(dag_id)
        dag_model = DagModel.get_dagmodel(dag_id)
        base_date = request.args.get('base_date')
        num_runs = request.args.get('num_runs', default=default_dag_run, type=int)

        if base_date:
            base_date = timezone.parse(base_date)
        else:
            base_date = dag.get_latest_execution_date() or timezone.utcnow()

        wwwutils.check_import_errors(dag.fileloc, session)

        root = request.args.get('root')
        if root:
            dag = dag.partial_subset(task_ids_or_regex=root, include_upstream=True, include_downstream=False)

        tis = dag.get_task_instances_before(base_date, num_runs, session=session)

        chart_height = wwwutils.get_chart_height(dag)
        chart = nvd3.lineChart(
            name="lineChart", x_is_date=True, height=chart_height, chart_attr=self.line_chart_attr
        )
        y_points = {}
        x_points = {}
        for task in dag.tasks:
            task_id = task.task_id
            y_points[task_id] = []
            x_points[task_id] = []
            for ti in tis:
                ts = dag.get_run_data_interval(ti.dag_run).end
                if ti.end_date:
                    dttm = wwwutils.epoch(ti.execution_date)
                    secs = (ti.end_date - ts).total_seconds()
                    x_points[task_id].append(dttm)
                    y_points[task_id].append(secs)

        # determine the most relevant time unit for the set of landing times
        # for the DAG
        y_unit = infer_time_unit([d for t in y_points.values() for d in t])
        # update the y Axis to have the correct time units
        chart.create_y_axis('yAxis', format='.02f', custom_format=False, label=f'Landing Time ({y_unit})')
        chart.axislist['yAxis']['axisLabelDistance'] = '-15'

        for task_id in x_points:
            chart.add_serie(
                name=task_id,
                x=x_points[task_id],
                y=scale_time_units(y_points[task_id], y_unit),
            )

        dates = sorted({ti.execution_date for ti in tis})
        max_date = max(ti.execution_date for ti in tis) if dates else None

        session.commit()

        form = DateTimeWithNumRunsForm(
            data={
                'base_date': max_date or timezone.utcnow(),
                'num_runs': num_runs,
            }
        )
        chart.buildcontent()
        return self.render_template(
            'airflow/chart.html',
            dag=dag,
            chart=Markup(chart.htmlcontent),
            height=str(chart_height + 100) + "px",
            root=root,
            form=form,
            tab_title='Landing times',
            dag_model=dag_model,
        )

    @expose('/paused', methods=['POST'])
    @auth.has_access(
        [
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
        ]
    )
    @action_logging
    def paused(self):
        """Toggle paused."""
        dag_id = request.args.get('dag_id')
        is_paused = request.args.get('is_paused') == 'false'
        models.DagModel.get_dagmodel(dag_id).set_is_paused(is_paused=is_paused)
        return "OK"

    @expose('/gantt')
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        ]
    )
    @action_logging
    @provide_session
    def gantt(self, session=None):
        """Show GANTT chart."""
        dag_id = request.args.get('dag_id')
        dag = current_app.dag_bag.get_dag(dag_id)
        dag_model = DagModel.get_dagmodel(dag_id)

        root = request.args.get('root')
        if root:
            dag = dag.partial_subset(task_ids_or_regex=root, include_upstream=True, include_downstream=False)

        wwwutils.check_import_errors(dag.fileloc, session)

        dt_nr_dr_data = get_date_time_num_runs_dag_runs_form_data(request, session, dag)
        dttm = dt_nr_dr_data['dttm']

        form = DateTimeWithNumRunsWithDagRunsForm(data=dt_nr_dr_data)
        form.execution_date.choices = dt_nr_dr_data['dr_choices']

        tis = (
            session.query(TaskInstance)
            .join(TaskInstance.dag_run)
            .filter(
                DagRun.execution_date == dttm,
                TaskInstance.dag_id == dag_id,
                TaskInstance.start_date.isnot(None),
                TaskInstance.state.isnot(None),
            )
            .order_by(TaskInstance.start_date)
        )

        ti_fails = (
            session.query(TaskFail)
            .join(DagRun, DagRun.execution_date == TaskFail.execution_date)
            .filter(DagRun.execution_date == dttm, TaskFail.dag_id == dag_id)
        )

        tasks = []
        for ti in tis:
            # prev_attempted_tries will reflect the currently running try_number
            # or the try_number of the last complete run
            # https://issues.apache.org/jira/browse/AIRFLOW-2143
            try_count = ti.prev_attempted_tries if ti.prev_attempted_tries != 0 else ti.try_number
            task_dict = alchemy_to_dict(ti)
            task_dict['end_date'] = task_dict['end_date'] or timezone.utcnow()
            task_dict['extraLinks'] = dag.get_task(ti.task_id).extra_links
            task_dict['try_number'] = try_count
            task_dict['execution_date'] = dttm.isoformat()
            tasks.append(task_dict)

        tf_count = 0
        try_count = 1
        prev_task_id = ""
        for failed_task_instance in ti_fails:
            if tf_count != 0 and failed_task_instance.task_id == prev_task_id:
                try_count += 1
            else:
                try_count = 1
            prev_task_id = failed_task_instance.task_id
            tf_count += 1
            task = dag.get_task(failed_task_instance.task_id)
            task_dict = alchemy_to_dict(failed_task_instance)
            end_date = task_dict['end_date'] or timezone.utcnow()
            task_dict['end_date'] = end_date
            task_dict['start_date'] = task_dict['start_date'] or end_date
            task_dict['state'] = State.FAILED
            task_dict['operator'] = task.task_type
            task_dict['try_number'] = try_count
            task_dict['extraLinks'] = task.extra_links
            task_dict['execution_date'] = dttm.isoformat()
            tasks.append(task_dict)

        task_names = [ti.task_id for ti in tis]
        data = {
            'taskNames': task_names,
            'tasks': tasks,
            'height': len(task_names) * 25 + 25,
        }

        session.commit()

        return self.render_template(
            'airflow/gantt.html',
            dag=dag,
            execution_date=dttm.isoformat(),
            form=form,
            data=data,
            base_date='',
            root=root,
            dag_model=dag_model,
        )

    @expose('/extra_links')
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        ]
    )
    @action_logging
    def extra_links(self):
        """
        A restful endpoint that returns external links for a given Operator

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
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        link_name = request.args.get('link_name')
        dttm = timezone.parse(execution_date)
        dag = current_app.dag_bag.get_dag(dag_id)

        if not dag or task_id not in dag.task_ids:
            response = jsonify(
                {
                    'url': None,
                    'error': f"can't find dag {dag} or task_id {task_id}",
                }
            )
            response.status_code = 404
            return response

        task = dag.get_task(task_id)
        try:
            url = task.get_extra_links(dttm, link_name)
        except ValueError as err:
            response = jsonify({'url': None, 'error': str(err)})
            response.status_code = 404
            return response
        if url:
            response = jsonify({'error': None, 'url': url})
            response.status_code = 200
            return response
        else:
            response = jsonify({'url': None, 'error': f'No URL found for {link_name}'})
            response.status_code = 404
            return response

    @expose('/object/task_instances')
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        ]
    )
    @action_logging
    def task_instances(self):
        """Shows task instances."""
        dag_id = request.args.get('dag_id')
        dag = current_app.dag_bag.get_dag(dag_id)

        dttm = request.args.get('execution_date')
        if dttm:
            dttm = timezone.parse(dttm)
        else:
            return "Error: Invalid execution_date"

        task_instances = {ti.task_id: alchemy_to_dict(ti) for ti in dag.get_task_instances(dttm, dttm)}

        return json.dumps(task_instances, cls=utils_json.AirflowJsonEncoder)

    @expose('/object/tree_data')
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        ]
    )
    @action_logging
    def tree_data(self):
        """Returns tree data"""
        dag_id = request.args.get('dag_id')
        dag = current_app.dag_bag.get_dag(dag_id)

        if not dag:
            response = jsonify({'error': f"can't find dag {dag_id}"})
            response.status_code = 404
            return response

        root = request.args.get('root')
        if root:
            dag = dag.partial_subset(task_ids_or_regex=root, include_downstream=False, include_upstream=True)

        num_runs = request.args.get('num_runs', type=int)
        if num_runs is None:
            num_runs = conf.getint('webserver', 'default_dag_run_display_number')

        try:
            base_date = timezone.parse(request.args["base_date"])
        except (KeyError, ValueError):
            base_date = dag.get_latest_execution_date() or timezone.utcnow()

        with create_session() as session:
            dag_runs = (
                session.query(DagRun)
                .filter(DagRun.dag_id == dag.dag_id, DagRun.execution_date <= base_date)
                .order_by(DagRun.execution_date.desc())
                .limit(num_runs)
                .all()
            )
            dag_runs = {dr.execution_date: alchemy_to_dict(dr) for dr in dag_runs}
            tree_data = self._get_tree_data(dag_runs, dag, base_date, session=session)

        # avoid spaces to reduce payload size
        return htmlsafe_json_dumps(tree_data, separators=(',', ':'))

    @expose('/robots.txt')
    @action_logging
    def robots(self):
        """
        Returns a robots.txt file for blocking certain search engine crawlers. This mitigates some
        of the risk associated with exposing Airflow to the public internet, however it does not
        address the real security risks associated with such a deployment.
        """
        return send_from_directory(current_app.static_folder, 'robots.txt')


class ConfigurationView(AirflowBaseView):
    """View to show Airflow Configurations"""

    default_view = 'conf'

    class_permission_name = permissions.RESOURCE_CONFIG
    base_permissions = [
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    @expose('/configuration')
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG),
        ]
    )
    def conf(self):
        """Shows configuration."""
        raw = request.args.get('raw') == "true"
        title = "Airflow Configuration"
        subtitle = AIRFLOW_CONFIG
        # Don't show config when expose_config variable is False in airflow config
        if conf.getboolean("webserver", "expose_config"):
            with open(AIRFLOW_CONFIG) as file:
                config = file.read()
            table = [
                (section, key, value, source)
                for section, parameters in conf.as_dict(True, True).items()
                for key, (value, source) in parameters.items()
            ]
        else:
            config = (
                "# Your Airflow administrator chose not to expose the "
                "configuration, most likely for security reasons."
            )
            table = None

        if raw:
            return Response(response=config, status=200, mimetype="application/text")
        else:
            code_html = Markup(
                highlight(
                    config,
                    lexers.IniLexer(),  # Lexer call
                    HtmlFormatter(noclasses=True),
                )
            )
            return self.render_template(
                'airflow/config.html',
                pre_subtitle=settings.HEADER + "  v" + airflow.__version__,
                code_html=code_html,
                title=title,
                subtitle=subtitle,
                table=table,
            )


class RedocView(AirflowBaseView):
    """Redoc Open API documentation"""

    default_view = 'redoc'

    @expose('/redoc')
    def redoc(self):
        """Redoc API documentation."""
        openapi_spec_url = url_for("/api/v1./api/v1_openapi_yaml")
        return self.render_template('airflow/redoc.html', openapi_spec_url=openapi_spec_url)


######################################################################################
#                                    ModelViews
######################################################################################


class DagFilter(BaseFilter):
    """Filter using DagIDs"""

    def apply(self, query, func):
        if current_app.appbuilder.sm.has_all_dags_access():
            return query
        filter_dag_ids = current_app.appbuilder.sm.get_accessible_dag_ids(g.user)
        return query.filter(self.model.dag_id.in_(filter_dag_ids))


class DagEditFilter(BaseFilter):
    """Filter using DagIDs"""

    def apply(self, query, func):  # pylint: disable=redefined-outer-name,unused-argument
        filter_dag_ids = current_app.appbuilder.sm.get_editable_dag_ids(g.user)
        return query.filter(self.model.dag_id.in_(filter_dag_ids))


class AirflowModelView(ModelView):
    """Airflow Mode View."""

    list_widget = AirflowModelListWidget
    page_size = PAGE_SIZE

    CustomSQLAInterface = wwwutils.CustomSQLAInterface


class AirflowPrivilegeVerifierModelView(AirflowModelView):
    """
    This ModelView prevents ability to pass primary keys of objects relating to DAGs you shouldn't be able to
    edit. This only holds for the add, update and delete operations.
    You will still need to use the `action_has_dag_edit_access()` for actions.
    """

    @staticmethod
    def validate_dag_edit_access(item: Union[DagRun, TaskInstance]):
        """Validates whether the user has 'can_edit' access for this specific DAG."""
        if not current_app.appbuilder.sm.can_edit_dag(item.dag_id):
            raise AirflowException(f"Access denied for dag_id {item.dag_id}")

    def pre_add(self, item: Union[DagRun, TaskInstance]):
        self.validate_dag_edit_access(item)

    def pre_update(self, item: Union[DagRun, TaskInstance]):
        self.validate_dag_edit_access(item)

    def pre_delete(self, item: Union[DagRun, TaskInstance]):
        self.validate_dag_edit_access(item)

    def post_add_redirect(self):  # Required to prevent redirect loop
        return redirect(self.get_default_url())

    def post_edit_redirect(self):  # Required to prevent redirect loop
        return redirect(self.get_default_url())

    def post_delete_redirect(self):  # Required to prevent redirect loop
        return redirect(self.get_default_url())


def action_has_dag_edit_access(action_func: Callable) -> Callable:
    """Decorator for actions which verifies you have DAG edit access on the given tis/drs."""

    @wraps(action_func)
    def check_dag_edit_acl_for_actions(
        self,
        items: Optional[Union[List[TaskInstance], List[DagRun], TaskInstance, DagRun]],
        *args,
        **kwargs,
    ) -> None:
        if items is None:
            dag_ids: Set[str] = set()
        elif isinstance(items, list):
            dag_ids = {item.dag_id for item in items if item is not None}
        elif isinstance(items, TaskInstance) or isinstance(items, DagRun):
            dag_ids = {items.dag_id}
        else:
            raise ValueError(
                "Was expecting the first argument of the action to be of type "
                "Optional[Union[List[TaskInstance], List[DagRun], TaskInstance, DagRun]]."
                f"Was of type: {type(items)}"
            )

        for dag_id in dag_ids:
            if not current_app.appbuilder.sm.can_edit_dag(dag_id):
                flash(f"Access denied for dag_id {dag_id}", "danger")
                logging.warning("User %s tried to modify %s without having access.", g.user.username, dag_id)
                return redirect(self.get_default_url())
        return action_func(self, items, *args, **kwargs)

    return check_dag_edit_acl_for_actions


class SlaMissModelView(AirflowModelView):
    """View to show SlaMiss table"""

    route_base = '/slamiss'

    datamodel = AirflowModelView.CustomSQLAInterface(SlaMiss)  # type: ignore

    class_permission_name = permissions.RESOURCE_SLA_MISS
    method_permission_name = {
        'list': 'read',
    }

    base_permissions = [
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    list_columns = ['dag_id', 'task_id', 'execution_date', 'email_sent', 'timestamp']

    label_columns = {
        'execution_date': 'Logical Date',
    }

    add_columns = ['dag_id', 'task_id', 'execution_date', 'email_sent', 'timestamp']
    edit_columns = ['dag_id', 'task_id', 'execution_date', 'email_sent', 'timestamp']
    search_columns = ['dag_id', 'task_id', 'email_sent', 'timestamp', 'execution_date']
    base_order = ('execution_date', 'desc')
    base_filters = [['dag_id', DagFilter, lambda: []]]

    formatters_columns = {
        'task_id': wwwutils.task_instance_link,
        'execution_date': wwwutils.datetime_f('execution_date'),
        'timestamp': wwwutils.datetime_f('timestamp'),
        'dag_id': wwwutils.dag_link,
    }


class XComModelView(AirflowModelView):
    """View to show records from XCom table"""

    route_base = '/xcom'

    list_title = 'List XComs'

    datamodel = AirflowModelView.CustomSQLAInterface(XCom)

    class_permission_name = permissions.RESOURCE_XCOM
    method_permission_name = {
        'list': 'read',
        'delete': 'delete',
        'action_muldelete': 'delete',
    }
    base_permissions = [
        permissions.ACTION_CAN_CREATE,
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_DELETE,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    label_columns = {
        'execution_date': 'Logical Date',
    }

    search_columns = ['key', 'value', 'timestamp', 'execution_date', 'task_id', 'dag_id']
    list_columns = ['key', 'value', 'timestamp', 'execution_date', 'task_id', 'dag_id']
    base_order = ('execution_date', 'desc')

    base_filters = [['dag_id', DagFilter, lambda: []]]

    formatters_columns = {
        'task_id': wwwutils.task_instance_link,
        'execution_date': wwwutils.datetime_f('execution_date'),
        'timestamp': wwwutils.datetime_f('timestamp'),
        'dag_id': wwwutils.dag_link,
    }

    @action('muldelete', 'Delete', "Are you sure you want to delete selected records?", single=False)
    def action_muldelete(self, items):
        """Multiple delete action."""
        self.datamodel.delete_all(items)
        self.update_redirect()
        return redirect(self.get_redirect())

    def pre_add(self, item):
        """Pre add hook."""
        item.execution_date = timezone.make_aware(item.execution_date)
        item.value = XCom.serialize_value(item.value)

    def pre_update(self, item):
        """Pre update hook."""
        item.execution_date = timezone.make_aware(item.execution_date)
        item.value = XCom.serialize_value(item.value)


def lazy_add_provider_discovered_options_to_connection_form():
    """Adds provider-discovered connection parameters as late as possible"""

    def _get_connection_types() -> List[Tuple[str, str]]:
        """Returns connection types available."""
        _connection_types = [
            ('fs', 'File (path)'),
            ('mesos_framework-id', 'Mesos Framework ID'),
            ('email', 'Email'),
        ]
        providers_manager = ProvidersManager()
        for connection_type, provider_info in providers_manager.hooks.items():
            if provider_info:
                _connection_types.append((connection_type, provider_info.hook_name))
        return _connection_types

    ConnectionForm.conn_type = SelectField(
        lazy_gettext('Connection Type'),
        choices=sorted(_get_connection_types(), key=itemgetter(1)),
        widget=Select2Widget(),
        validators=[InputRequired()],
        description="""
            Connection Type missing?
            Make sure you've installed the corresponding Airflow Provider Package.
        """,
    )
    for key, value in ProvidersManager().connection_form_widgets.items():
        setattr(ConnectionForm, key, value.field)


# Used to store a dictionary of field behaviours used to dynamically change available
# fields in ConnectionForm based on type of connection chosen
# See airflow.hooks.base_hook.DiscoverableHook for details on how to customize your Hooks.
# those field behaviours are rendered as scripts in the conn_create.html and conn_edit.html templates
class ConnectionFormWidget(FormWidget):
    """Form widget used to display connection"""

    field_behaviours = json.dumps(ProvidersManager().field_behaviours)


class ConnectionModelView(AirflowModelView):
    """View to show records from Connections table"""

    route_base = '/connection'

    datamodel = AirflowModelView.CustomSQLAInterface(Connection)  # type: ignore

    class_permission_name = permissions.RESOURCE_CONNECTION
    method_permission_name = {
        'add': 'create',
        'list': 'read',
        'edit': 'edit',
        'delete': 'delete',
        'action_muldelete': 'delete',
        'action_mulduplicate': 'create',
    }

    base_permissions = [
        permissions.ACTION_CAN_CREATE,
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_DELETE,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    extra_fields = list(ProvidersManager().connection_form_widgets.keys())
    list_columns = [
        'conn_id',
        'conn_type',
        'description',
        'host',
        'port',
        'is_encrypted',
        'is_extra_encrypted',
    ]
    add_columns = edit_columns = [
        'conn_id',
        'conn_type',
        'description',
        'host',
        'schema',
        'login',
        'password',
        'port',
        'extra',
    ] + extra_fields

    add_form = edit_form = ConnectionForm
    add_template = 'airflow/conn_create.html'
    edit_template = 'airflow/conn_edit.html'

    add_widget = ConnectionFormWidget
    edit_widget = ConnectionFormWidget

    base_order = ('conn_id', 'asc')

    @action('muldelete', 'Delete', 'Are you sure you want to delete selected records?', single=False)
    @auth.has_access(
        [
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
        ]
    )
    def action_muldelete(self, items):
        """Multiple delete."""
        self.datamodel.delete_all(items)
        self.update_redirect()
        return redirect(self.get_redirect())

    @action(
        'mulduplicate',
        'Duplicate',
        'Are you sure you want to duplicate the selected connections?',
        single=False,
    )
    @provide_session
    @auth.has_access(
        [
            (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_CONNECTION),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_CONNECTION),
        ]
    )
    def action_mulduplicate(self, connections, session=None):
        """Duplicate Multiple connections"""
        for selected_conn in connections:
            new_conn_id = selected_conn.conn_id
            match = re.search(r"_copy(\d+)$", selected_conn.conn_id)
            if match:
                conn_id_prefix = selected_conn.conn_id[: match.start()]
                new_conn_id = f"{conn_id_prefix}_copy{int(match.group(1)) + 1}"
            else:
                new_conn_id += '_copy1'

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
                    f"Connection {new_conn_id} can't be added. Integrity error, probably unique constraint.",
                    "warning",
                )
                session.rollback()

        self.update_redirect()
        return redirect(self.get_redirect())

    def process_form(self, form, is_created):
        """Process form data."""
        conn_type = form.data['conn_type']
        conn_id = form.data["conn_id"]
        extra = {
            key: form.data[key]
            for key in self.extra_fields
            if key in form.data and key.startswith(f"extra__{conn_type}__")
        }

        # If parameters are added to the classic `Extra` field, include these values along with
        # custom-field extras.
        extra_conn_params = form.data.get("extra")

        if extra_conn_params:
            try:
                extra.update(json.loads(extra_conn_params))
            except (JSONDecodeError, TypeError):
                flash(
                    Markup(
                        "<p>The <em>Extra</em> connection field contained an invalid value for Conn ID: "
                        f"<q>{conn_id}</q>.</p>"
                        "<p>If connection parameters need to be added to <em>Extra</em>, "
                        "please make sure they are in the form of a single, valid JSON object.</p><br>"
                        "The following <em>Extra</em> parameters were <b>not</b> added to the connection:<br>"
                        f"{extra_conn_params}",
                    ),
                    category="error",
                )

        if extra.keys():
            form.extra.data = json.dumps(extra)

    def prefill_form(self, form, pk):
        """Prefill the form."""
        try:
            extra = form.data.get('extra')
            if extra is None:
                extra_dictionary = {}
            else:
                extra_dictionary = json.loads(extra)
        except JSONDecodeError:
            extra_dictionary = {}

        if not isinstance(extra_dictionary, dict):
            logging.warning('extra field for %s is not a dictionary', form.data.get('conn_id', '<unknown>'))
            return

        for field in self.extra_fields:
            value = extra_dictionary.get(field, '')
            if value:
                field = getattr(form, field)
                field.data = value


class PluginView(AirflowBaseView):
    """View to show Airflow Plugins"""

    default_view = 'list'

    class_permission_name = permissions.RESOURCE_PLUGIN

    method_permission_name = {
        'list': 'read',
    }

    base_permissions = [
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    plugins_attributes_to_dump = [
        "hooks",
        "executors",
        "macros",
        "admin_views",
        "flask_blueprints",
        "menu_links",
        "appbuilder_views",
        "appbuilder_menu_items",
        "global_operator_extra_links",
        "operator_extra_links",
        "source",
    ]

    @expose('/plugin')
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_PLUGIN),
        ]
    )
    def list(self):
        """List loaded plugins."""
        plugins_manager.ensure_plugins_loaded()
        plugins_manager.integrate_executor_plugins()
        plugins_manager.initialize_extra_operators_links_plugins()
        plugins_manager.initialize_web_ui_plugins()

        plugins = []
        for plugin_no, plugin in enumerate(plugins_manager.plugins, 1):
            plugin_data = {
                'plugin_no': plugin_no,
                'plugin_name': plugin.name,
                'attrs': {},
            }
            for attr_name in self.plugins_attributes_to_dump:
                attr_value = getattr(plugin, attr_name)
                plugin_data['attrs'][attr_name] = attr_value

            plugins.append(plugin_data)

        title = "Airflow Plugins"
        doc_url = get_docs_url("plugins.html")
        return self.render_template(
            'airflow/plugin.html',
            plugins=plugins,
            title=title,
            doc_url=doc_url,
        )


class ProviderView(AirflowBaseView):
    """View to show Airflow Providers"""

    default_view = 'list'

    class_permission_name = permissions.RESOURCE_PROVIDER

    method_permission_name = {
        'list': 'read',
    }

    base_permissions = [
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    @expose('/provider')
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_PROVIDER),
        ]
    )
    def list(self):
        """List providers."""
        providers_manager = ProvidersManager()

        providers = []
        for pi in providers_manager.providers.values():
            provider_info = pi[1]
            provider_data = {
                "package_name": provider_info["package-name"],
                "description": self._clean_description(provider_info["description"]),
                "version": pi[0],
                "documentation_url": get_doc_url_for_provider(provider_info["package-name"], pi[0]),
            }
            providers.append(provider_data)

        title = "Providers"
        doc_url = get_docs_url("apache-airflow-providers/index.html")
        return self.render_template(
            'airflow/providers.html',
            providers=providers,
            title=title,
            doc_url=doc_url,
        )

    def _clean_description(self, description):
        def _build_link(match_obj):
            text = match_obj.group(1)
            url = match_obj.group(2)
            return markupsafe.Markup(f'<a href="{url}">{text}</a>')

        cd = markupsafe.escape(description)
        cd = re.sub(r"`(.*)[\s+]+&lt;(.*)&gt;`__", _build_link, cd)
        cd = re.sub(r"\n", r"<br>", cd)
        return markupsafe.Markup(cd)


class PoolModelView(AirflowModelView):
    """View to show records from Pool table"""

    route_base = '/pool'

    datamodel = AirflowModelView.CustomSQLAInterface(models.Pool)  # type: ignore

    class_permission_name = permissions.RESOURCE_POOL
    method_permission_name = {
        'add': 'create',
        'list': 'read',
        'edit': 'edit',
        'delete': 'delete',
        'action_muldelete': 'delete',
    }

    base_permissions = [
        permissions.ACTION_CAN_CREATE,
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_DELETE,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    list_columns = ['pool', 'slots', 'running_slots', 'queued_slots']
    add_columns = ['pool', 'slots', 'description']
    edit_columns = ['pool', 'slots', 'description']

    base_order = ('pool', 'asc')

    @action('muldelete', 'Delete', 'Are you sure you want to delete selected records?', single=False)
    def action_muldelete(self, items):
        """Multiple delete."""
        if any(item.pool == models.Pool.DEFAULT_POOL_NAME for item in items):
            flash("default_pool cannot be deleted", 'error')
            self.update_redirect()
            return redirect(self.get_redirect())
        self.datamodel.delete_all(items)
        self.update_redirect()
        return redirect(self.get_redirect())

    def pool_link(self):
        """Pool link rendering."""
        pool_id = self.get('pool')
        if pool_id is not None:
            url = url_for('TaskInstanceModelView.list', _flt_3_pool=pool_id)
            return Markup("<a href='{url}'>{pool_id}</a>").format(url=url, pool_id=pool_id)
        else:
            return Markup('<span class="label label-danger">Invalid</span>')

    def frunning_slots(self):
        """Running slots rendering."""
        pool_id = self.get('pool')
        running_slots = self.get('running_slots')
        if pool_id is not None and running_slots is not None:
            url = url_for('TaskInstanceModelView.list', _flt_3_pool=pool_id, _flt_3_state='running')
            return Markup("<a href='{url}'>{running_slots}</a>").format(url=url, running_slots=running_slots)
        else:
            return Markup('<span class="label label-danger">Invalid</span>')

    def fqueued_slots(self):
        """Queued slots rendering."""
        pool_id = self.get('pool')
        queued_slots = self.get('queued_slots')
        if pool_id is not None and queued_slots is not None:
            url = url_for('TaskInstanceModelView.list', _flt_3_pool=pool_id, _flt_3_state='queued')
            return Markup("<a href='{url}'>{queued_slots}</a>").format(url=url, queued_slots=queued_slots)
        else:
            return Markup('<span class="label label-danger">Invalid</span>')

    formatters_columns = {'pool': pool_link, 'running_slots': frunning_slots, 'queued_slots': fqueued_slots}

    validators_columns = {'pool': [validators.DataRequired()], 'slots': [validators.NumberRange(min=-1)]}


def _can_create_variable() -> bool:
    return current_app.appbuilder.sm.has_access(permissions.ACTION_CAN_CREATE, permissions.RESOURCE_VARIABLE)


class VariableModelView(AirflowModelView):
    """View to show records from Variable table"""

    route_base = '/variable'

    list_template = 'airflow/variable_list.html'
    edit_template = 'airflow/variable_edit.html'

    datamodel = AirflowModelView.CustomSQLAInterface(models.Variable)  # type: ignore

    class_permission_name = permissions.RESOURCE_VARIABLE
    method_permission_name = {
        'add': 'create',
        'list': 'read',
        'edit': 'edit',
        'delete': 'delete',
        'action_muldelete': 'delete',
        'action_varexport': 'read',
    }
    base_permissions = [
        permissions.ACTION_CAN_CREATE,
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_DELETE,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    list_columns = ['key', 'val', 'description', 'is_encrypted']
    add_columns = ['key', 'val', 'description']
    edit_columns = ['key', 'val', 'description']
    search_columns = ['key', 'val']

    base_order = ('key', 'asc')

    def hidden_field_formatter(self):
        """Formats hidden fields"""
        key = self.get('key')
        val = self.get('val')
        if secrets_masker.should_hide_value_for_key(key):
            return Markup('*' * 8)
        if val:
            return val
        else:
            return Markup('<span class="label label-danger">Invalid</span>')

    formatters_columns = {
        'val': hidden_field_formatter,
    }

    validators_columns = {'key': [validators.DataRequired()]}

    def prefill_form(self, form, request_id):
        if secrets_masker.should_hide_value_for_key(form.key.data):
            form.val.data = '*' * 8

    extra_args = {"can_create_variable": _can_create_variable}

    @action('muldelete', 'Delete', 'Are you sure you want to delete selected records?', single=False)
    def action_muldelete(self, items):
        """Multiple delete."""
        self.datamodel.delete_all(items)
        self.update_redirect()
        return redirect(self.get_redirect())

    @action('varexport', 'Export', '', single=False)
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

    @expose('/varimport', methods=["POST"])
    @auth.has_access([(permissions.ACTION_CAN_CREATE, permissions.RESOURCE_VARIABLE)])
    @action_logging
    def varimport(self):
        """Import variables"""
        try:
            variable_dict = json.loads(request.files['file'].read())
        except Exception:
            self.update_redirect()
            flash("Missing file or syntax error.", 'error')
            return redirect(self.get_redirect())
        else:
            suc_count = fail_count = 0
            for k, v in variable_dict.items():
                try:
                    models.Variable.set(k, v, serialize_json=not isinstance(v, str))
                except Exception as e:
                    logging.info('Variable import failed: %s', repr(e))
                    fail_count += 1
                else:
                    suc_count += 1
            flash(f"{suc_count} variable(s) successfully updated.")
            if fail_count:
                flash(f"{fail_count} variable(s) failed to be updated.", 'error')
            self.update_redirect()
            return redirect(self.get_redirect())


class JobModelView(AirflowModelView):
    """View to show records from Job table"""

    route_base = '/job'

    datamodel = AirflowModelView.CustomSQLAInterface(BaseJob)  # type: ignore

    class_permission_name = permissions.RESOURCE_JOB
    method_permission_name = {
        'list': 'read',
    }
    base_permissions = [
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    list_columns = [
        'id',
        'dag_id',
        'state',
        'job_type',
        'start_date',
        'end_date',
        'latest_heartbeat',
        'executor_class',
        'hostname',
        'unixname',
    ]
    search_columns = [
        'id',
        'dag_id',
        'state',
        'job_type',
        'start_date',
        'end_date',
        'latest_heartbeat',
        'executor_class',
        'hostname',
        'unixname',
    ]

    base_order = ('start_date', 'desc')

    base_filters = [['dag_id', DagFilter, lambda: []]]

    formatters_columns = {
        'start_date': wwwutils.datetime_f('start_date'),
        'end_date': wwwutils.datetime_f('end_date'),
        'hostname': wwwutils.nobr_f('hostname'),
        'state': wwwutils.state_f,
        'latest_heartbeat': wwwutils.datetime_f('latest_heartbeat'),
    }


class DagRunModelView(AirflowPrivilegeVerifierModelView):
    """View to show records from DagRun table"""

    route_base = '/dagrun'

    datamodel = AirflowModelView.CustomSQLAInterface(models.DagRun)  # type: ignore

    class_permission_name = permissions.RESOURCE_DAG_RUN
    method_permission_name = {
        'list': 'read',
        'action_clear': 'edit',
        'action_muldelete': 'delete',
        'action_set_running': 'edit',
        'action_set_failed': 'edit',
        'action_set_success': 'edit',
    }
    base_permissions = [
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_DELETE,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    list_columns = [
        'state',
        'dag_id',
        'execution_date',
        'run_id',
        'run_type',
        'queued_at',
        'start_date',
        'end_date',
        'external_trigger',
        'conf',
    ]
    search_columns = [
        'state',
        'dag_id',
        'execution_date',
        'run_id',
        'run_type',
        'start_date',
        'end_date',
        'external_trigger',
    ]
    label_columns = {
        'execution_date': 'Logical Date',
    }
    edit_columns = ['state', 'dag_id', 'execution_date', 'start_date', 'end_date', 'run_id', 'conf']

    base_order = ('execution_date', 'desc')

    base_filters = [['dag_id', DagEditFilter, lambda: []]]

    edit_form = DagRunEditForm

    formatters_columns = {
        'execution_date': wwwutils.datetime_f('execution_date'),
        'state': wwwutils.state_f,
        'start_date': wwwutils.datetime_f('start_date'),
        'end_date': wwwutils.datetime_f('end_date'),
        'queued_at': wwwutils.datetime_f('queued_at'),
        'dag_id': wwwutils.dag_link,
        'run_id': wwwutils.dag_run_link,
        'conf': wwwutils.json_f('conf'),
    }

    @action('muldelete', "Delete", "Are you sure you want to delete selected records?", single=False)
    @action_has_dag_edit_access
    @provide_session
    def action_muldelete(self, items, session=None):
        """Multiple delete."""
        self.datamodel.delete_all(items)
        self.update_redirect()
        return redirect(self.get_redirect())

    @action('set_running', "Set state to 'running'", '', single=False)
    @action_has_dag_edit_access
    @provide_session
    def action_set_running(self, drs, session=None):
        """Set state to running."""
        try:
            count = 0
            for dr in session.query(DagRun).filter(DagRun.id.in_([dagrun.id for dagrun in drs])).all():
                count += 1
                dr.start_date = timezone.utcnow()
                dr.state = State.RUNNING
            session.commit()
            flash(f"{count} dag runs were set to running")
        except Exception as ex:
            flash(str(ex), 'error')
            flash('Failed to set state', 'error')
        return redirect(self.get_default_url())

    @action(
        'set_failed',
        "Set state to 'failed'",
        "All running task instances would also be marked as failed, are you sure?",
        single=False,
    )
    @action_has_dag_edit_access
    @provide_session
    def action_set_failed(self, drs, session=None):
        """Set state to failed."""
        try:
            count = 0
            altered_tis = []
            for dr in session.query(DagRun).filter(DagRun.id.in_([dagrun.id for dagrun in drs])).all():
                count += 1
                altered_tis += set_dag_run_state_to_failed(
                    current_app.dag_bag.get_dag(dr.dag_id), dr.execution_date, commit=True, session=session
                )
            altered_ti_count = len(altered_tis)
            flash(
                "{count} dag runs and {altered_ti_count} task instances "
                "were set to failed".format(count=count, altered_ti_count=altered_ti_count)
            )
        except Exception:
            flash('Failed to set state', 'error')
        return redirect(self.get_default_url())

    @action(
        'set_success',
        "Set state to 'success'",
        "All task instances would also be marked as success, are you sure?",
        single=False,
    )
    @action_has_dag_edit_access
    @provide_session
    def action_set_success(self, drs, session=None):
        """Set state to success."""
        try:
            count = 0
            altered_tis = []
            for dr in session.query(DagRun).filter(DagRun.id.in_([dagrun.id for dagrun in drs])).all():
                count += 1
                altered_tis += set_dag_run_state_to_success(
                    current_app.dag_bag.get_dag(dr.dag_id), dr.execution_date, commit=True, session=session
                )
            altered_ti_count = len(altered_tis)
            flash(
                "{count} dag runs and {altered_ti_count} task instances "
                "were set to success".format(count=count, altered_ti_count=altered_ti_count)
            )
        except Exception:
            flash('Failed to set state', 'error')
        return redirect(self.get_default_url())

    @action('clear', "Clear the state", "All task instances would be cleared, are you sure?", single=False)
    @action_has_dag_edit_access
    @provide_session
    def action_clear(self, drs, session=None):
        """Clears the state."""
        try:
            count = 0
            cleared_ti_count = 0
            dag_to_tis = {}
            for dr in session.query(DagRun).filter(DagRun.id.in_([dagrun.id for dagrun in drs])).all():
                count += 1
                dag = current_app.dag_bag.get_dag(dr.dag_id)
                tis_to_clear = dag_to_tis.setdefault(dag, [])
                tis_to_clear += dr.get_task_instances()

            for dag, tis in dag_to_tis.items():
                cleared_ti_count += len(tis)
                models.clear_task_instances(tis, session, dag=dag)

            flash(f"{count} dag runs and {cleared_ti_count} task instances were cleared")
        except Exception:
            flash('Failed to clear state', 'error')
        return redirect(self.get_default_url())


class LogModelView(AirflowModelView):
    """View to show records from Log table"""

    route_base = '/log'

    datamodel = AirflowModelView.CustomSQLAInterface(Log)  # type:ignore

    class_permission_name = permissions.RESOURCE_AUDIT_LOG
    method_permission_name = {
        'list': 'read',
    }
    base_permissions = [
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    list_columns = ['id', 'dttm', 'dag_id', 'task_id', 'event', 'execution_date', 'owner', 'extra']
    search_columns = ['dag_id', 'task_id', 'event', 'execution_date', 'owner', 'extra']

    label_columns = {
        'execution_date': 'Logical Date',
    }

    base_order = ('dttm', 'desc')

    base_filters = [['dag_id', DagFilter, lambda: []]]

    formatters_columns = {
        'dttm': wwwutils.datetime_f('dttm'),
        'execution_date': wwwutils.datetime_f('execution_date'),
        'dag_id': wwwutils.dag_link,
    }


class TaskRescheduleModelView(AirflowModelView):
    """View to show records from Task Reschedule table"""

    route_base = '/taskreschedule'

    datamodel = AirflowModelView.CustomSQLAInterface(models.TaskReschedule)  # type: ignore
    related_views = [DagRunModelView]

    class_permission_name = permissions.RESOURCE_TASK_RESCHEDULE
    method_permission_name = {
        'list': 'read',
    }

    base_permissions = [
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    list_columns = [
        'id',
        'dag_id',
        'run_id',
        'dag_run.execution_date',
        'task_id',
        'try_number',
        'start_date',
        'end_date',
        'duration',
        'reschedule_date',
    ]

    label_columns = {
        'dag_run.execution_date': 'Logical Date',
    }

    search_columns = [
        'dag_id',
        'task_id',
        'run_id',
        'execution_date',
        'start_date',
        'end_date',
        'reschedule_date',
    ]

    base_order = ('id', 'desc')

    base_filters = [['dag_id', DagFilter, lambda: []]]

    def duration_f(self):
        """Duration calculation."""
        end_date = self.get('end_date')
        duration = self.get('duration')
        if end_date and duration:
            return timedelta(seconds=duration)
        return None

    formatters_columns = {
        'dag_id': wwwutils.dag_link,
        'task_id': wwwutils.task_instance_link,
        'start_date': wwwutils.datetime_f('start_date'),
        'end_date': wwwutils.datetime_f('end_date'),
        'dag_run.execution_date': wwwutils.datetime_f('dag_run.execution_date'),
        'reschedule_date': wwwutils.datetime_f('reschedule_date'),
        'duration': duration_f,
    }


class TriggerModelView(AirflowModelView):
    """View to show records from Task Reschedule table"""

    route_base = '/triggerview'

    datamodel = AirflowModelView.CustomSQLAInterface(models.Trigger)  # type: ignore

    class_permission_name = permissions.RESOURCE_TRIGGER
    method_permission_name = {
        'list': 'read',
    }

    base_permissions = [
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    list_columns = [
        'id',
        'classpath',
        'created_date',
        'triggerer_id',
    ]

    search_columns = [
        'id',
        'classpath',
        'created_date',
        'triggerer_id',
    ]

    # add_exclude_columns = ["kwargs"]

    base_order = ('id', 'created_date')

    formatters_columns = {
        'created_date': wwwutils.datetime_f('created_date'),
    }


class TaskInstanceModelView(AirflowPrivilegeVerifierModelView):
    """View to show records from TaskInstance table"""

    route_base = '/taskinstance'

    datamodel = AirflowModelView.CustomSQLAInterface(models.TaskInstance)  # type: ignore

    class_permission_name = permissions.RESOURCE_TASK_INSTANCE
    method_permission_name = {
        'list': 'read',
        'action_clear': 'edit',
        'action_muldelete': 'delete',
        'action_set_running': 'edit',
        'action_set_failed': 'edit',
        'action_set_success': 'edit',
        'action_set_retry': 'edit',
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
        'state',
        'dag_id',
        'task_id',
        'run_id',
        'dag_run.execution_date',
        'operator',
        'start_date',
        'end_date',
        'duration',
        'job_id',
        'hostname',
        'unixname',
        'priority_weight',
        'queue',
        'queued_dttm',
        'try_number',
        'pool',
        'queued_by_job_id',
        'external_executor_id',
        'log_url',
    ]

    order_columns = [
        item for item in list_columns if item not in ['try_number', 'log_url', 'external_executor_id']
    ]

    label_columns = {
        'dag_run.execution_date': 'Logical Date',
    }

    search_columns = [
        'state',
        'dag_id',
        'task_id',
        'run_id',
        'execution_date',
        'hostname',
        'queue',
        'pool',
        'operator',
        'start_date',
        'end_date',
        'queued_dttm',
    ]

    edit_columns = [
        'state',
        'start_date',
        'end_date',
    ]

    add_exclude_columns = ["next_method", "next_kwargs", "trigger_id"]

    edit_form = TaskInstanceEditForm

    base_order = ('job_id', 'asc')

    base_filters = [['dag_id', DagEditFilter, lambda: []]]

    def log_url_formatter(self):
        """Formats log URL."""
        log_url = self.get('log_url')
        return Markup(
            '<a href="{log_url}"><span class="material-icons" aria-hidden="true">reorder</span></a>'
        ).format(log_url=log_url)

    def duration_f(self):
        """Formats duration."""
        end_date = self.get('end_date')
        duration = self.get('duration')
        if end_date and duration:
            return timedelta(seconds=duration)
        return None

    formatters_columns = {
        'log_url': log_url_formatter,
        'task_id': wwwutils.task_instance_link,
        'run_id': wwwutils.dag_run_link,
        'hostname': wwwutils.nobr_f('hostname'),
        'state': wwwutils.state_f,
        'dag_run.execution_date': wwwutils.datetime_f('dag_run.execution_date'),
        'start_date': wwwutils.datetime_f('start_date'),
        'end_date': wwwutils.datetime_f('end_date'),
        'queued_dttm': wwwutils.datetime_f('queued_dttm'),
        'dag_id': wwwutils.dag_link,
        'duration': duration_f,
    }

    @action(
        'clear',
        lazy_gettext('Clear'),
        lazy_gettext(
            'Are you sure you want to clear the state of the selected task'
            ' instance(s) and set their dagruns to the QUEUED state?'
        ),
        single=False,
    )
    @action_has_dag_edit_access
    @provide_session
    def action_clear(self, task_instances, session=None):
        """Clears the action."""
        try:
            dag_to_tis = collections.defaultdict(list)

            for ti in task_instances:
                dag = current_app.dag_bag.get_dag(ti.dag_id)
                dag_to_tis[dag].append(ti)

            for dag, task_instances_list in dag_to_tis.items():
                models.clear_task_instances(task_instances_list, session, dag=dag)

            session.commit()
            flash(f"{len(task_instances)} task instances have been cleared")
        except Exception as e:
            flash(f'Failed to clear task instances: "{e}"', 'error')
        self.update_redirect()
        return redirect(self.get_redirect())

    @action('muldelete', 'Delete', "Are you sure you want to delete selected records?", single=False)
    @action_has_dag_edit_access
    def action_muldelete(self, items):
        self.datamodel.delete_all(items)
        self.update_redirect()
        return redirect(self.get_redirect())

    @provide_session
    def set_task_instance_state(self, tis, target_state, session=None):
        """Set task instance state."""
        try:
            count = len(tis)
            for ti in tis:
                ti.set_state(target_state, session)
            session.commit()
            flash(f"{count} task instances were set to '{target_state}'")
        except Exception:
            flash('Failed to set state', 'error')

    @action('set_running', "Set state to 'running'", '', single=False)
    @action_has_dag_edit_access
    def action_set_running(self, tis):
        """Set state to 'running'"""
        self.set_task_instance_state(tis, State.RUNNING)
        self.update_redirect()
        return redirect(self.get_redirect())

    @action('set_failed', "Set state to 'failed'", '', single=False)
    @action_has_dag_edit_access
    def action_set_failed(self, tis):
        """Set state to 'failed'"""
        self.set_task_instance_state(tis, State.FAILED)
        self.update_redirect()
        return redirect(self.get_redirect())

    @action('set_success', "Set state to 'success'", '', single=False)
    @action_has_dag_edit_access
    def action_set_success(self, tis):
        """Set state to 'success'"""
        self.set_task_instance_state(tis, State.SUCCESS)
        self.update_redirect()
        return redirect(self.get_redirect())

    @action('set_retry', "Set state to 'up_for_retry'", '', single=False)
    @action_has_dag_edit_access
    def action_set_retry(self, tis):
        """Set state to 'up_for_retry'"""
        self.set_task_instance_state(tis, State.UP_FOR_RETRY)
        self.update_redirect()
        return redirect(self.get_redirect())


class AutocompleteView(AirflowBaseView):
    """View to provide autocomplete results"""

    @auth.has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)])
    @provide_session
    @expose('/dagmodel/autocomplete')
    def autocomplete(self, session=None):
        """Autocomplete."""
        query = unquote(request.args.get('query', ''))

        if not query:
            return wwwutils.json_response([])

        # Provide suggestions of dag_ids and owners
        dag_ids_query = session.query(DagModel.dag_id.label('item')).filter(
            ~DagModel.is_subdag, DagModel.is_active, DagModel.dag_id.ilike('%' + query + '%')
        )

        owners_query = session.query(func.distinct(DagModel.owners).label('item')).filter(
            ~DagModel.is_subdag, DagModel.is_active, DagModel.owners.ilike('%' + query + '%')
        )

        # Hide DAGs if not showing status: "all"
        status = flask_session.get(FILTER_STATUS_COOKIE)
        if status == 'active':
            dag_ids_query = dag_ids_query.filter(~DagModel.is_paused)
            owners_query = owners_query.filter(~DagModel.is_paused)
        elif status == 'paused':
            dag_ids_query = dag_ids_query.filter(DagModel.is_paused)
            owners_query = owners_query.filter(DagModel.is_paused)

        filter_dag_ids = current_app.appbuilder.sm.get_accessible_dag_ids(g.user)

        dag_ids_query = dag_ids_query.filter(DagModel.dag_id.in_(filter_dag_ids))
        owners_query = owners_query.filter(DagModel.dag_id.in_(filter_dag_ids))

        payload = [row[0] for row in dag_ids_query.union(owners_query).limit(10).all()]

        return wwwutils.json_response(payload)


class DagDependenciesView(AirflowBaseView):
    """View to show dependencies between DAGs"""

    refresh_interval = timedelta(
        seconds=conf.getint(
            "webserver",
            "dag_dependencies_refresh_interval",
            fallback=conf.getint("scheduler", "dag_dir_list_interval"),
        )
    )
    last_refresh = timezone.utcnow() - refresh_interval
    nodes = []
    edges = []

    @expose('/dag-dependencies')
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_DEPENDENCIES),
        ]
    )
    @gzipped
    @action_logging
    def list(self):
        """Display DAG dependencies"""
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

        nodes = []
        edges = []

        for dag, dependencies in SerializedDagModel.get_dag_dependencies().items():
            dag_node_id = f"dag:{dag}"
            nodes.append(self._node_dict(dag_node_id, dag, "dag"))

            for dep in dependencies:

                nodes.append(self._node_dict(dep.node_id, dep.dependency_id, dep.dependency_type))
                edges.extend(
                    [
                        {"u": f"dag:{dep.source}", "v": dep.node_id},
                        {"u": dep.node_id, "v": f"dag:{dep.target}"},
                    ]
                )

        self.nodes = nodes
        self.edges = edges

    @staticmethod
    def _node_dict(node_id, label, node_class):
        return {
            "id": node_id,
            "value": {"label": label, "rx": 5, "ry": 5, "class": node_class},
        }


class CustomPermissionModelView(PermissionModelView):
    """Customize permission names for FAB's builtin PermissionModelView."""

    class_permission_name = permissions.RESOURCE_PERMISSION
    method_permission_name = {
        'list': 'read',
    }
    base_permissions = [
        permissions.ACTION_CAN_READ,
    ]


class CustomPermissionViewModelView(PermissionViewModelView):
    """Customize permission names for FAB's builtin PermissionViewModelView."""

    class_permission_name = permissions.RESOURCE_PERMISSION_VIEW
    method_permission_name = {
        'list': 'read',
    }
    base_permissions = [
        permissions.ACTION_CAN_READ,
    ]


class CustomResetMyPasswordView(ResetMyPasswordView):
    """Customize permission names for FAB's builtin ResetMyPasswordView."""

    class_permission_name = permissions.RESOURCE_MY_PASSWORD
    method_permission_name = {
        'this_form_get': 'read',
        'this_form_post': 'edit',
    }
    base_permissions = [permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ]


class CustomResetPasswordView(ResetPasswordView):
    """Customize permission names for FAB's builtin ResetPasswordView."""

    class_permission_name = permissions.RESOURCE_PASSWORD
    method_permission_name = {
        'this_form_get': 'read',
        'this_form_post': 'edit',
    }

    base_permissions = [permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ]


class CustomRoleModelView(RoleModelView):
    """Customize permission names for FAB's builtin RoleModelView."""

    class_permission_name = permissions.RESOURCE_ROLE
    method_permission_name = {
        'delete': 'delete',
        'download': 'read',
        'show': 'read',
        'list': 'read',
        'edit': 'edit',
        'add': 'create',
        'copy_role': 'create',
    }
    base_permissions = [
        permissions.ACTION_CAN_CREATE,
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_DELETE,
    ]


class CustomViewMenuModelView(ViewMenuModelView):
    """Customize permission names for FAB's builtin ViewMenuModelView."""

    class_permission_name = permissions.RESOURCE_VIEW_MENU
    method_permission_name = {
        'list': 'read',
    }
    base_permissions = [
        permissions.ACTION_CAN_READ,
    ]


class CustomUserInfoEditView(UserInfoEditView):
    """Customize permission names for FAB's builtin UserInfoEditView."""

    class_permission_name = permissions.RESOURCE_MY_PROFILE
    route_base = "/userinfoeditview"
    method_permission_name = {
        'this_form_get': 'edit',
        'this_form_post': 'edit',
    }
    base_permissions = [permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ]


class CustomUserStatsChartView(UserStatsChartView):
    """Customize permission names for FAB's builtin UserStatsChartView."""

    class_permission_name = permissions.RESOURCE_USER_STATS_CHART
    route_base = "/userstatschartview"
    method_permission_name = {
        'chart': 'read',
        'list': 'read',
    }
    base_permissions = [permissions.ACTION_CAN_READ]


class MultiResourceUserMixin:
    """Remaps UserModelView permissions to new resources and actions."""

    _class_permission_name = permissions.RESOURCE_USER

    class_permission_name_mapping = {
        'userinfoedit': permissions.RESOURCE_MY_PROFILE,
        'userinfo': permissions.RESOURCE_MY_PROFILE,
    }

    method_permission_name = {
        'userinfo': 'read',
        'download': 'read',
        'show': 'read',
        'list': 'read',
        'edit': 'edit',
        'userinfoedit': 'edit',
        'delete': 'delete',
    }

    base_permissions = [
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_DELETE,
    ]

    @expose("/show/<pk>", methods=["GET"])
    @has_access
    def show(self, pk):
        pk = self._deserialize_pk_if_composite(pk)
        widgets = self._show(pk)
        widgets['show'].template_args['actions'].pop('userinfoedit')
        return self.render_template(
            self.show_template,
            pk=pk,
            title=self.show_title,
            widgets=widgets,
            related_views=self._related_views,
        )


class CustomUserDBModelView(MultiResourceUserMixin, UserDBModelView):
    """Customize permission names for FAB's builtin UserDBModelView."""

    _class_permission_name = permissions.RESOURCE_USER

    class_permission_name_mapping = {
        'resetmypassword': permissions.RESOURCE_MY_PASSWORD,
        'resetpasswords': permissions.RESOURCE_PASSWORD,
        'userinfoedit': permissions.RESOURCE_MY_PROFILE,
        'userinfo': permissions.RESOURCE_MY_PROFILE,
    }

    method_permission_name = {
        'add': 'create',
        'download': 'read',
        'show': 'read',
        'list': 'read',
        'edit': 'edit',
        'delete': 'delete',
        'resetmypassword': 'read',
        'resetpasswords': 'read',
        'userinfo': 'read',
        'userinfoedit': 'read',
    }

    base_permissions = [
        permissions.ACTION_CAN_CREATE,
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_DELETE,
    ]

    @property
    def class_permission_name(self):
        """Returns appropriate permission name depending on request method name."""
        if request:
            action_name = request.view_args.get("name")
            _, method_name = request.url_rule.endpoint.rsplit(".", 1)
            if method_name == 'action' and action_name:
                return self.class_permission_name_mapping.get(action_name, self._class_permission_name)
            if method_name:
                return self.class_permission_name_mapping.get(method_name, self._class_permission_name)
        return self._class_permission_name

    @class_permission_name.setter
    def class_permission_name(self, name):
        self._class_permission_name = name


class CustomUserLDAPModelView(MultiResourceUserMixin, UserLDAPModelView):
    """Customize permission names for FAB's builtin UserLDAPModelView."""


class CustomUserOAuthModelView(MultiResourceUserMixin, UserOAuthModelView):
    """Customize permission names for FAB's builtin UserOAuthModelView."""


class CustomUserOIDModelView(MultiResourceUserMixin, UserOIDModelView):
    """Customize permission names for FAB's builtin UserOIDModelView."""


class CustomUserRemoteUserModelView(MultiResourceUserMixin, UserRemoteUserModelView):
    """Customize permission names for FAB's builtin UserRemoteUserModelView."""

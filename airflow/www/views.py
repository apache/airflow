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
import copy
import itertools
import json
import logging
import math
import socket
import traceback
from collections import defaultdict
from datetime import datetime, timedelta
from json import JSONDecodeError
from typing import Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse

import lazy_object_proxy
import nvd3
import sqlalchemy as sqla
from flask import (
    Markup, Response, current_app, escape, flash, jsonify, make_response, redirect, render_template, request,
    session as flask_session, url_for,
)
from flask_appbuilder import BaseView, ModelView, expose, has_access, permission_name
from flask_appbuilder.actions import action
from flask_appbuilder.models.sqla.filters import BaseFilter  # noqa
from flask_babel import lazy_gettext
from jinja2.utils import htmlsafe_json_dumps, pformat  # type: ignore
from pygments import highlight, lexers
from pygments.formatters import HtmlFormatter  # noqa pylint: disable=no-name-in-module
from sqlalchemy import and_, desc, func, or_, union_all
from sqlalchemy.orm import joinedload
from wtforms import SelectField, validators

import airflow
from airflow import models, settings
from airflow.api.common.experimental.mark_tasks import (
    set_dag_run_state_to_failed, set_dag_run_state_to_success,
)
from airflow.configuration import AIRFLOW_CONFIG, conf
from airflow.exceptions import AirflowException
from airflow.executors.executor_loader import ExecutorLoader
from airflow.jobs.base_job import BaseJob
from airflow.jobs.scheduler_job import SchedulerJob
from airflow.models import Connection, DagModel, DagTag, Log, SlaMiss, TaskFail, XCom, errors
from airflow.models.dagcode import DagCode
from airflow.models.dagrun import DagRun, DagRunType
from airflow.models.taskinstance import TaskInstance
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_deps import RUNNING_DEPS, SCHEDULER_QUEUED_DEPS
from airflow.utils import timezone
from airflow.utils.dates import infer_time_unit, scale_time_units
from airflow.utils.helpers import alchemy_to_dict
from airflow.utils.log.log_reader import TaskLogReader
from airflow.utils.platform import get_airflow_git_version
from airflow.utils.session import create_session, provide_session
from airflow.utils.state import State
from airflow.www import utils as wwwutils
from airflow.www.decorators import action_logging, gzipped, has_dag_access
from airflow.www.forms import (
    ConnectionForm, DagRunForm, DateTimeForm, DateTimeWithNumRunsForm, DateTimeWithNumRunsWithDagRunsForm,
)
from airflow.www.widgets import AirflowModelListWidget

PAGE_SIZE = conf.getint('webserver', 'page_size')
FILTER_TAGS_COOKIE = 'tags_filter'
FILTER_STATUS_COOKIE = 'dag_status_filter'


def get_safe_url(url):
    """Given a user-supplied URL, ensure it points to our web server"""
    valid_schemes = ['http', 'https', '']
    valid_netlocs = [request.host, '']

    parsed = urlparse(url)

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
    num_runs = www_request.args.get('num_runs')
    num_runs = int(num_runs) if num_runs else default_dag_run

    drs = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == dag.dag_id,
            DagRun.execution_date <= base_date)
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


######################################################################################
#                                    Error handlers
######################################################################################

def circles(error):  # pylint: disable=unused-argument
    """Show Circles on screen for any error in the Webserver"""
    return render_template(
        'airflow/circles.html', hostname=socket.getfqdn() if conf.getboolean(  # noqa
            'webserver',
            'EXPOSE_HOSTNAME',
            fallback=True) else 'redact'), 404


def show_traceback(error):  # pylint: disable=unused-argument
    """Show Traceback for a given error"""
    from airflow.utils import asciiart as ascii_

    return render_template(
        'airflow/traceback.html',  # noqa
        hostname=socket.getfqdn() if conf.getboolean(
            'webserver',
            'EXPOSE_HOSTNAME',
            fallback=True) else 'redact',
        nukular=ascii_.nukular,
        info=traceback.format_exc() if conf.getboolean(
            'webserver',
            'EXPOSE_STACKTRACE',
            fallback=True) else 'Error! Please contact server admin'), 500

######################################################################################
#                                    BaseViews
######################################################################################


class AirflowBaseView(BaseView):  # noqa: D101
    """Base View to set Airflow related properties"""
    from airflow import macros
    route_base = ''

    # Make our macros available to our UI templates too.
    extra_args = {
        'macros': macros,
    }

    def render_template(self, *args, **kwargs):
        return super().render_template(
            *args,
            # Cache this at most once per request, not for the lifetime of the view instance
            scheduler_job=lazy_object_proxy.Proxy(SchedulerJob.most_recent_job),
            **kwargs
        )


class Airflow(AirflowBaseView):  # noqa: D101  pylint: disable=too-many-public-methods
    """
    Main Airflow application.
    """
    @expose('/health')
    def health(self):
        """
        An endpoint helping check the health status of the Airflow instance,
        including metadatabase and scheduler.
        """

        payload = {
            'metadatabase': {'status': 'unhealthy'}
        }

        latest_scheduler_heartbeat = None
        scheduler_status = 'unhealthy'
        payload['metadatabase'] = {'status': 'healthy'}
        try:
            scheduler_job = SchedulerJob.most_recent_job()

            if scheduler_job:
                latest_scheduler_heartbeat = scheduler_job.latest_heartbeat.isoformat()
                if scheduler_job.is_alive():
                    scheduler_status = 'healthy'
        except Exception:  # noqa pylint: disable=broad-except
            payload['metadatabase']['status'] = 'unhealthy'

        payload['scheduler'] = {'status': scheduler_status,
                                'latest_scheduler_heartbeat': latest_scheduler_heartbeat}

        return wwwutils.json_response(payload)

    @expose('/home')
    @has_access
    def index(self):  # pylint: disable=too-many-locals,too-many-statements
        """Home view."""
        hide_paused_dags_by_default = conf.getboolean('webserver',
                                                      'hide_paused_dags_by_default')

        default_dag_run = conf.getint('webserver', 'default_dag_run_display_number')
        num_runs = request.args.get('num_runs')
        num_runs = int(num_runs) if num_runs else default_dag_run

        def get_int_arg(value, default=0):
            try:
                return int(value)
            except ValueError:
                return default

        arg_current_page = request.args.get('page', '0')
        arg_search_query = request.args.get('search', None)
        arg_tags_filter = request.args.getlist('tags', None)
        arg_status_filter = request.args.get('status', None)

        if request.args.get('reset_tags') is not None:
            flask_session[FILTER_TAGS_COOKIE] = None
            arg_tags_filter = None
        else:
            cookie_val = flask_session.get(FILTER_TAGS_COOKIE)
            if arg_tags_filter:
                flask_session[FILTER_TAGS_COOKIE] = ','.join(arg_tags_filter)
            elif cookie_val:
                arg_tags_filter = cookie_val.split(',')

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
        current_page = get_int_arg(arg_current_page, default=0)

        start = current_page * dags_per_page
        end = start + dags_per_page

        # Get all the dag id the user could access
        filter_dag_ids = current_app.appbuilder.sm.get_accessible_dag_ids()

        with create_session() as session:
            # read orm_dags from the db
            dags_query = session.query(DagModel).filter(
                ~DagModel.is_subdag, DagModel.is_active
            )

            # pylint: disable=no-member
            if arg_search_query:
                dags_query = dags_query.filter(
                    DagModel.dag_id.ilike('%' + arg_search_query + '%') |  # noqa
                    DagModel.owners.ilike('%' + arg_search_query + '%')  # noqa
                )

            if arg_tags_filter:
                dags_query = dags_query.filter(DagModel.tags.any(DagTag.name.in_(arg_tags_filter)))

            if 'all_dags' not in filter_dag_ids:
                dags_query = dags_query.filter(DagModel.dag_id.in_(filter_dag_ids))
                # pylint: enable=no-member

            all_dags = dags_query
            active_dags = dags_query.filter(~DagModel.is_paused)
            paused_dags = dags_query.filter(DagModel.is_paused)

            is_paused_count = dict(
                all_dags.with_entities(DagModel.is_paused, func.count(DagModel.dag_id))
                .group_by(DagModel.is_paused).all()
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

            dags = current_dags.order_by(DagModel.dag_id).options(
                joinedload(DagModel.tags)).offset(start).limit(dags_per_page).all()

            dagtags = session.query(DagTag.name).distinct(DagTag.name).all()
            tags = [
                {"name": name, "selected": bool(arg_tags_filter and name in arg_tags_filter)}
                for name, in dagtags
            ]

            import_errors = session.query(errors.ImportError).all()

        for import_error in import_errors:
            flash(
                "Broken DAG: [{ie.filename}] {ie.stacktrace}".format(ie=import_error),
                "dag_import_error")

        from airflow.plugins_manager import import_errors as plugin_import_errors
        for filename, stacktrace in plugin_import_errors.items():
            flash(
                "Broken plugin: [{filename}] {stacktrace}".format(
                    stacktrace=stacktrace,
                    filename=filename),
                "error")

        num_of_pages = int(math.ceil(num_of_all_dags / float(dags_per_page)))

        state_color_mapping = State.state_color.copy()
        state_color_mapping["null"] = state_color_mapping.pop(None)

        return self.render_template(
            'airflow/dags.html',
            dags=dags,
            current_page=current_page,
            search_query=arg_search_query if arg_search_query else '',
            page_size=dags_per_page,
            num_of_pages=num_of_pages,
            num_dag_from=min(start + 1, num_of_all_dags),
            num_dag_to=min(end, num_of_all_dags),
            num_of_all_dags=num_of_all_dags,
            paging=wwwutils.generate_pages(current_page,
                                           num_of_pages,
                                           search=escape(arg_search_query) if arg_search_query else None,
                                           status=arg_status_filter if arg_status_filter else None),
            num_runs=num_runs,
            tags=tags,
            state_color=state_color_mapping,
            status_filter=arg_status_filter,
            status_count_all=all_dags_count,
            status_count_active=status_count_active,
            status_count_paused=status_count_paused)

    @expose('/dag_stats', methods=['POST'])
    @has_access
    @provide_session
    def dag_stats(self, session=None):
        """Dag statistics."""
        dr = models.DagRun

        allowed_dag_ids = current_app.appbuilder.sm.get_accessible_dag_ids()
        if 'all_dags' in allowed_dag_ids:
            allowed_dag_ids = [dag_id for dag_id, in session.query(models.DagModel.dag_id)]

        dag_state_stats = session.query(dr.dag_id, dr.state, sqla.func.count(dr.state))\
            .group_by(dr.dag_id, dr.state)

        # Filter by post parameters
        selected_dag_ids = {
            unquote(dag_id) for dag_id in request.form.getlist('dag_ids') if dag_id
        }

        if selected_dag_ids:
            filter_dag_ids = selected_dag_ids.intersection(allowed_dag_ids)
        else:
            filter_dag_ids = allowed_dag_ids

        if not filter_dag_ids:
            return wwwutils.json_response({})

        payload = {}
        dag_state_stats = dag_state_stats.filter(dr.dag_id.in_(filter_dag_ids))  # pylint: disable=no-member
        data = {}

        for dag_id, state, count in dag_state_stats:
            if dag_id not in data:
                data[dag_id] = {}
            data[dag_id][state] = count

        for dag_id in filter_dag_ids:
            payload[dag_id] = []
            for state in State.dag_states:
                count = data.get(dag_id, {}).get(state, 0)
                payload[dag_id].append({
                    'state': state,
                    'count': count
                })

        return wwwutils.json_response(payload)

    @expose('/task_stats', methods=['POST'])
    @has_access
    @provide_session
    def task_stats(self, session=None):
        """Task Statistics"""
        allowed_dag_ids = set(current_app.appbuilder.sm.get_accessible_dag_ids())

        if not allowed_dag_ids:
            return wwwutils.json_response({})

        if 'all_dags' in allowed_dag_ids:
            allowed_dag_ids = {dag_id for dag_id, in session.query(models.DagModel.dag_id)}

        # Filter by post parameters
        selected_dag_ids = {
            unquote(dag_id) for dag_id in request.form.getlist('dag_ids') if dag_id
        }

        if selected_dag_ids:
            filter_dag_ids = selected_dag_ids.intersection(allowed_dag_ids)
        else:
            filter_dag_ids = allowed_dag_ids

        # pylint: disable=comparison-with-callable
        running_dag_run_query_result = (
            session.query(DagRun.dag_id, DagRun.execution_date)
                   .join(DagModel, DagModel.dag_id == DagRun.dag_id)
                   .filter(DagRun.state == State.RUNNING, DagModel.is_active)
        )
        # pylint: enable=comparison-with-callable

        # pylint: disable=no-member
        if selected_dag_ids:
            running_dag_run_query_result = \
                running_dag_run_query_result.filter(DagRun.dag_id.in_(filter_dag_ids))
        # pylint: enable=no-member

        running_dag_run_query_result = running_dag_run_query_result.subquery('running_dag_run')

        # pylint: disable=no-member
        # Select all task_instances from active dag_runs.
        running_task_instance_query_result = (
            session.query(TaskInstance.dag_id.label('dag_id'), TaskInstance.state.label('state'))
                   .join(running_dag_run_query_result,
                         and_(running_dag_run_query_result.c.dag_id == TaskInstance.dag_id,
                              running_dag_run_query_result.c.execution_date == TaskInstance.execution_date))
        )
        if selected_dag_ids:
            running_task_instance_query_result = \
                running_task_instance_query_result.filter(TaskInstance.dag_id.in_(filter_dag_ids))
        # pylint: enable=no-member

        if conf.getboolean('webserver', 'SHOW_RECENT_STATS_FOR_COMPLETED_RUNS', fallback=True):
            # pylint: disable=comparison-with-callable
            last_dag_run = (
                session.query(
                    DagRun.dag_id,
                    sqla.func.max(DagRun.execution_date).label('execution_date')
                )
                .join(DagModel, DagModel.dag_id == DagRun.dag_id)
                .filter(DagRun.state != State.RUNNING, DagModel.is_active)
                .group_by(DagRun.dag_id)
            )
            # pylint: enable=comparison-with-callable
            # pylint: disable=no-member
            if selected_dag_ids:
                last_dag_run = last_dag_run.filter(DagRun.dag_id.in_(filter_dag_ids))
            last_dag_run = last_dag_run.subquery('last_dag_run')
            # pylint: enable=no-member

            # Select all task_instances from active dag_runs.
            # If no dag_run is active, return task instances from most recent dag_run.
            last_task_instance_query_result = (
                session.query(TaskInstance.dag_id.label('dag_id'), TaskInstance.state.label('state'))
                       .join(last_dag_run,
                             and_(last_dag_run.c.dag_id == TaskInstance.dag_id,
                                  last_dag_run.c.execution_date == TaskInstance.execution_date))
            )
            # pylint: disable=no-member
            if selected_dag_ids:
                last_task_instance_query_result = \
                    last_task_instance_query_result.filter(TaskInstance.dag_id.in_(filter_dag_ids))
            # pylint: enable=no-member

            final_task_instance_query_result = union_all(
                last_task_instance_query_result,
                running_task_instance_query_result).alias('final_ti')
        else:
            final_task_instance_query_result = running_task_instance_query_result.subquery('final_ti')

        qry = (
            session.query(final_task_instance_query_result.c.dag_id,
                          final_task_instance_query_result.c.state, sqla.func.count())
                   .group_by(final_task_instance_query_result.c.dag_id,
                             final_task_instance_query_result.c.state)
        )

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
                payload[dag_id].append({
                    'state': state,
                    'count': count
                })
        return wwwutils.json_response(payload)

    @expose('/last_dagruns', methods=['POST'])
    @has_access
    @provide_session
    def last_dagruns(self, session=None):
        """Last DAG runs"""
        allowed_dag_ids = current_app.appbuilder.sm.get_accessible_dag_ids()

        if 'all_dags' in allowed_dag_ids:
            allowed_dag_ids = [dag_id for dag_id, in session.query(models.DagModel.dag_id)]

        # Filter by post parameters
        selected_dag_ids = {
            unquote(dag_id) for dag_id in request.form.getlist('dag_ids') if dag_id
        }

        if selected_dag_ids:
            filter_dag_ids = selected_dag_ids.intersection(allowed_dag_ids)
        else:
            filter_dag_ids = allowed_dag_ids

        if not filter_dag_ids:
            return wwwutils.json_response({})

        query = session.query(
            DagRun.dag_id, sqla.func.max(DagRun.execution_date).label('last_run')
        ).group_by(DagRun.dag_id)

        # Filter to only ask for accessible and selected dags
        query = query.filter(DagRun.dag_id.in_(filter_dag_ids))  # pylint: enable=no-member

        resp = {
            r.dag_id.replace('.', '__dot__'): {
                'dag_id': r.dag_id,
                'last_run': r.last_run.isoformat(),
            } for r in query
        }
        return wwwutils.json_response(resp)

    @expose('/code')
    @has_dag_access(can_dag_read=True)
    @has_access
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
            html_code = Markup(highlight(
                code, lexers.PythonLexer(), HtmlFormatter(linenos=True)))  # pylint: disable=no-member

        except Exception as e:  # pylint: disable=broad-except
            all_errors += (
                "Exception encountered during " +
                "dag_id retrieval/dag retrieval fallback/code highlighting:\n\n{}\n".format(e)
            )
            html_code = Markup('<p>Failed to load file.</p><p>Details: {}</p>').format(  # noqa
                escape(all_errors))

        return self.render_template(
            'airflow/dag_code.html', html_code=html_code, dag=dag_orm, title=dag_id,
            root=request.args.get('root'),
            demo_mode=conf.getboolean('webserver', 'demo_mode'),
            wrapped=conf.getboolean('webserver', 'default_wrap'))

    @expose('/dag_details')
    @has_dag_access(can_dag_read=True)
    @has_access
    @provide_session
    def dag_details(self, session=None):
        """Get Dag details."""
        dag_id = request.args.get('dag_id')
        dag = current_app.dag_bag.get_dag(dag_id)
        title = "DAG details"
        root = request.args.get('root', '')

        states = (
            session.query(TaskInstance.state, sqla.func.count(TaskInstance.dag_id))
                   .filter(TaskInstance.dag_id == dag_id)
                   .group_by(TaskInstance.state)
                   .all()
        )

        active_runs = models.DagRun.find(
            dag_id=dag_id,
            state=State.RUNNING,
            external_trigger=False
        )

        return self.render_template(
            'airflow/dag_details.html',
            dag=dag, title=title, root=root, states=states, State=State, active_runs=active_runs)

    @expose('/rendered')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    def rendered(self):
        """Get rendered Dag."""
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        dttm = timezone.parse(execution_date)
        form = DateTimeForm(data={'execution_date': dttm})
        root = request.args.get('root', '')

        logging.info("Retrieving rendered templates.")
        dag = current_app.dag_bag.get_dag(dag_id)

        task = copy.copy(dag.get_task(task_id))
        ti = models.TaskInstance(task=task, execution_date=dttm)
        try:
            ti.get_rendered_template_fields()
        except AirflowException as e:  # pylint: disable=broad-except
            msg = "Error rendering template: " + escape(e)
            if e.__cause__:  # pylint: disable=using-constant-test
                msg += Markup("<br/><br/>OriginalError: ") + escape(e.__cause__)
            flash(msg, "error")
        except Exception as e:  # pylint: disable=broad-except
            flash("Error rendering template: " + str(e), "error")
        title = "Rendered Template"
        html_dict = {}
        for template_field in task.template_fields:
            content = getattr(task, template_field)
            if template_field in wwwutils.get_attr_renderer():
                html_dict[template_field] = wwwutils.get_attr_renderer()[template_field](content)
            else:
                html_dict[template_field] = \
                    Markup("<pre><code>{}</pre></code>").format(pformat(content))  # noqa

        return self.render_template(
            'airflow/ti_code.html',
            html_dict=html_dict,
            dag=dag,
            task_id=task_id,
            execution_date=execution_date,
            form=form,
            root=root,
            title=title)

    @expose('/get_logs_with_metadata')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    @provide_session
    def get_logs_with_metadata(self, session=None):
        """Retrieve logs including metadata."""
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        if request.args.get('try_number') is not None:
            try_number = int(request.args.get('try_number'))
        else:
            try_number = None
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
                'as a date. Example date format: 2015-11-16T14:34:15+00:00'.format(
                    execution_date))
            response = jsonify({'error': error_message})
            response.status_code = 400

            return response

        task_log_reader = TaskLogReader()
        if not task_log_reader.supports_read:
            return jsonify(
                message="Task log handler does not support read logs.",
                error=True,
                metadata={
                    "end_of_log": True
                }
            )

        ti = session.query(models.TaskInstance).filter(
            models.TaskInstance.dag_id == dag_id,
            models.TaskInstance.task_id == task_id,
            models.TaskInstance.execution_date == execution_date).first()

        if ti is None:
            return jsonify(
                message="*** Task instance did not exist in the DB\n",
                error=True,
                metadata={
                    "end_of_log": True
                }
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
                headers={
                    "Content-Disposition": f"attachment; filename={attachment_filename}"
                })
        except AttributeError as e:
            error_message = [
                f"Task log handler does not support read logs.\n{str(e)}\n"
            ]
            metadata['end_of_log'] = True
            return jsonify(message=error_message, error=True, metadata=metadata)

    @expose('/log')
    @has_dag_access(can_dag_read=True)
    @has_access
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

        ti = session.query(models.TaskInstance).filter(
            models.TaskInstance.dag_id == dag_id,
            models.TaskInstance.task_id == task_id,
            models.TaskInstance.execution_date == dttm).first()

        num_logs = 0
        if ti is not None:
            num_logs = ti.next_try_number - 1
            if ti.state == State.UP_FOR_RESCHEDULE:
                # Tasks in reschedule state decremented the try number
                num_logs += 1
        logs = [''] * num_logs
        root = request.args.get('root', '')
        return self.render_template(
            'airflow/ti_log.html',
            logs=logs, dag=dag_model, title="Log by attempts",
            dag_id=dag_id, task_id=task_id,
            execution_date=execution_date, form=form,
            root=root, wrapped=conf.getboolean('webserver', 'default_wrap'))

    @expose('/redirect_to_external_log')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    @provide_session
    def redirect_to_external_log(self, session=None):
        """Redirects to external log."""
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        dttm = timezone.parse(execution_date)
        try_number = request.args.get('try_number', 1)

        ti = session.query(models.TaskInstance).filter(
            models.TaskInstance.dag_id == dag_id,
            models.TaskInstance.task_id == task_id,
            models.TaskInstance.execution_date == dttm).first()

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
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    def task(self):
        """Retrieve task."""
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        # Carrying execution_date through, even though it's irrelevant for
        # this context
        execution_date = request.args.get('execution_date')
        dttm = timezone.parse(execution_date)
        form = DateTimeForm(data={'execution_date': dttm})
        root = request.args.get('root', '')
        dag = current_app.dag_bag.get_dag(dag_id)

        if not dag or task_id not in dag.task_ids:
            flash(
                "Task [{}.{}] doesn't seem to exist"
                " at the moment".format(dag_id, task_id),
                "error")
            return redirect(url_for('Airflow.index'))
        task = copy.copy(dag.get_task(task_id))
        task.resolve_template_files()
        ti = TaskInstance(task=task, execution_date=dttm)
        ti.refresh_from_db()

        ti_attrs = []
        for attr_name in dir(ti):
            if not attr_name.startswith('_'):
                attr = getattr(ti, attr_name)
                if type(attr) != type(self.task):  # noqa pylint: disable=unidiomatic-typecheck
                    ti_attrs.append((attr_name, str(attr)))

        task_attrs = []
        for attr_name in dir(task):
            if not attr_name.startswith('_'):
                attr = getattr(task, attr_name)
                # pylint: disable=unidiomatic-typecheck
                if type(attr) != type(self.task) and \
                        attr_name not in wwwutils.get_attr_renderer():  # noqa
                    task_attrs.append((attr_name, str(attr)))
                # pylint: enable=unidiomatic-typecheck

        # Color coding the special attributes that are code
        special_attrs_rendered = {}
        for attr_name in wwwutils.get_attr_renderer():
            if hasattr(task, attr_name):
                source = getattr(task, attr_name)
                special_attrs_rendered[attr_name] = \
                    wwwutils.get_attr_renderer()[attr_name](source)

        no_failed_deps_result = [(
            "Unknown",
            "All dependencies are met but the task instance is not running. In most "
            "cases this just means that the task will probably be scheduled soon "
            "unless:<br/>\n- The scheduler is down or under heavy load<br/>\n{}\n"
            "<br/>\nIf this task instance does not start soon please contact your "
            "Airflow administrator for assistance.".format(
                "- This task instance already ran and had it's state changed manually "
                "(e.g. cleared in the UI)<br/>" if ti.state == State.NONE else ""))]

        # Use the scheduler's context to figure out which dependencies are not met
        dep_context = DepContext(SCHEDULER_QUEUED_DEPS)
        failed_dep_reasons = [(dep.dep_name, dep.reason) for dep in
                              ti.get_failed_dep_statuses(
                                  dep_context=dep_context)]

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
            dag=dag, title=title)

    @expose('/xcom')
    @has_dag_access(can_dag_read=True)
    @has_access
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
        dm_db = models.DagModel
        ti_db = models.TaskInstance
        dag = session.query(dm_db).filter(dm_db.dag_id == dag_id).first()
        ti = session.query(ti_db).filter(ti_db.dag_id == dag_id and ti_db.task_id == task_id).first()

        if not ti:
            flash(
                "Task [{}.{}] doesn't seem to exist"
                " at the moment".format(dag_id, task_id),
                "error")
            return redirect(url_for('Airflow.index'))

        xcomlist = session.query(XCom).filter(
            XCom.dag_id == dag_id, XCom.task_id == task_id,
            XCom.execution_date == dttm).all()

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
            dag=dag, title=title)

    @expose('/run', methods=['POST'])
    @has_dag_access(can_dag_edit=True)
    @has_access
    @action_logging
    def run(self):
        """Retrieves Run."""
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

        try:
            from airflow.executors.celery_executor import CeleryExecutor  # noqa
            valid_celery_config = isinstance(executor, CeleryExecutor)
        except ImportError:
            pass

        try:
            from airflow.executors.kubernetes_executor import KubernetesExecutor  # noqa
            valid_kubernetes_config = isinstance(executor, KubernetesExecutor)
        except ImportError:
            pass

        if not valid_celery_config and not valid_kubernetes_config:
            flash("Only works with the Celery or Kubernetes executors, sorry", "error")
            return redirect(origin)

        ti = models.TaskInstance(task=task, execution_date=execution_date)
        ti.refresh_from_db()

        # Make sure the task instance can be run
        dep_context = DepContext(
            deps=RUNNING_DEPS,
            ignore_all_deps=ignore_all_deps,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state)
        failed_deps = list(ti.get_failed_dep_statuses(dep_context=dep_context))
        if failed_deps:
            failed_deps_str = ", ".join(
                ["{}: {}".format(dep.dep_name, dep.reason) for dep in failed_deps])
            flash("Could not queue task instance for execution, dependencies not met: "
                  "{}".format(failed_deps_str),
                  "error")
            return redirect(origin)

        executor.start()
        executor.queue_task_instance(
            ti,
            ignore_all_deps=ignore_all_deps,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state)
        executor.heartbeat()
        flash(
            "Sent {} to the message queue, "
            "it should start any moment now.".format(ti))
        return redirect(origin)

    @expose('/delete', methods=['POST'])
    @has_dag_access(can_dag_edit=True)
    @has_access
    @action_logging
    def delete(self):
        """Deletes DAG."""
        from airflow.api.common.experimental import delete_dag
        from airflow.exceptions import DagFileExists, DagNotFound

        dag_id = request.values.get('dag_id')
        origin = get_safe_url(request.values.get('origin'))

        try:
            delete_dag.delete_dag(dag_id)
        except DagNotFound:
            flash("DAG with id {} not found. Cannot delete".format(dag_id), 'error')
            return redirect(request.referrer)
        except DagFileExists:
            flash("Dag id {} is still in DagBag. "
                  "Remove the DAG file first.".format(dag_id),
                  'error')
            return redirect(request.referrer)

        flash("Deleting DAG with id {}. May take a couple minutes to fully"
              " disappear.".format(dag_id))

        # Upon success return to origin.
        return redirect(origin)

    @expose('/trigger', methods=['POST', 'GET'])
    @has_dag_access(can_dag_edit=True)
    @has_access
    @action_logging
    @provide_session
    def trigger(self, session=None):
        """Triggers DAG Run."""
        dag_id = request.values.get('dag_id')
        origin = get_safe_url(request.values.get('origin'))

        if request.method == 'GET':
            return self.render_template(
                'airflow/trigger.html',
                dag_id=dag_id,
                origin=origin,
                conf=''
            )

        dag_orm = session.query(models.DagModel).filter(models.DagModel.dag_id == dag_id).first()
        if not dag_orm:
            flash("Cannot find dag {}".format(dag_id))
            return redirect(origin)

        execution_date = timezone.utcnow()

        dr = DagRun.find(dag_id=dag_id, execution_date=execution_date, run_type=DagRunType.MANUAL)
        if dr:
            flash(f"This run_id {dr.run_id} already exists")  # noqa
            return redirect(origin)

        run_conf = {}
        request_conf = request.values.get('conf')
        if request_conf:
            try:
                run_conf = json.loads(request_conf)
            except json.decoder.JSONDecodeError:
                flash("Invalid JSON configuration", "error")
                return self.render_template(
                    'airflow/trigger.html',
                    dag_id=dag_id,
                    origin=origin,
                    conf=request_conf
                )

        dag = current_app.dag_bag.get_dag(dag_id)
        dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=execution_date,
            state=State.RUNNING,
            conf=run_conf,
            external_trigger=True,
        )

        flash(
            "Triggered {}, "
            "it should start any moment now.".format(dag_id))
        return redirect(origin)

    def _clear_dag_tis(self, dag, start_date, end_date, origin,
                       recursive=False, confirmed=False, only_failed=False):
        if confirmed:
            count = dag.clear(
                start_date=start_date,
                end_date=end_date,
                include_subdags=recursive,
                include_parentdag=recursive,
                only_failed=only_failed,
            )

            flash("{0} task instances have been cleared".format(count))
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
            details = "\n".join([str(t) for t in tis])

            response = self.render_template(
                'airflow/confirm.html',
                message=("Here's the list of task instances you are about "
                         "to clear:"),
                details=details)

        return response

    @expose('/clear', methods=['POST'])
    @has_dag_access(can_dag_edit=True)
    @has_access
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

        dag = dag.sub_dag(
            task_regex=r"^{0}$".format(task_id),
            include_downstream=downstream,
            include_upstream=upstream)

        end_date = execution_date if not future else None
        start_date = execution_date if not past else None

        return self._clear_dag_tis(dag, start_date, end_date, origin,
                                   recursive=recursive, confirmed=confirmed, only_failed=only_failed)

    @expose('/dagrun_clear', methods=['POST'])
    @has_dag_access(can_dag_edit=True)
    @has_access
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

        return self._clear_dag_tis(dag, start_date, end_date, origin,
                                   recursive=True, confirmed=confirmed)

    @expose('/blocked', methods=['POST'])
    @has_access
    @provide_session
    def blocked(self, session=None):
        """Mark Dag Blocked."""
        allowed_dag_ids = current_app.appbuilder.sm.get_accessible_dag_ids()

        if 'all_dags' in allowed_dag_ids:
            allowed_dag_ids = [dag_id for dag_id, in session.query(models.DagModel.dag_id)]

        # Filter by post parameters
        selected_dag_ids = {
            unquote(dag_id) for dag_id in request.form.getlist('dag_ids') if dag_id
        }

        if selected_dag_ids:
            filter_dag_ids = selected_dag_ids.intersection(allowed_dag_ids)
        else:
            filter_dag_ids = allowed_dag_ids

        if not filter_dag_ids:
            return wwwutils.json_response([])

        # pylint: disable=comparison-with-callable
        dags = (
            session.query(DagRun.dag_id, sqla.func.count(DagRun.id))
                   .filter(DagRun.state == State.RUNNING)
                   .filter(DagRun.dag_id.in_(filter_dag_ids))
                   .group_by(DagRun.dag_id)
        )
        # pylint: enable=comparison-with-callable

        payload = []
        for dag_id, active_dag_runs in dags:
            max_active_runs = 0
            dag = current_app.dag_bag.get_dag(dag_id)
            if dag:
                # TODO: Make max_active_runs a column so we can query for it directly
                max_active_runs = dag.max_active_runs
            payload.append({
                'dag_id': dag_id,
                'active_dag_run': active_dag_runs,
                'max_active_runs': max_active_runs,
            })
        return wwwutils.json_response(payload)

    def _mark_dagrun_state_as_failed(self, dag_id, execution_date, confirmed, origin):
        if not execution_date:
            flash('Invalid execution date', 'error')
            return redirect(origin)

        execution_date = timezone.parse(execution_date)
        dag = current_app.dag_bag.get_dag(dag_id)

        if not dag:
            flash('Cannot find DAG: {}'.format(dag_id), 'error')
            return redirect(origin)

        new_dag_state = set_dag_run_state_to_failed(dag, execution_date, commit=confirmed)

        if confirmed:
            flash('Marked failed on {} task instances'.format(len(new_dag_state)))
            return redirect(origin)

        else:
            details = '\n'.join([str(t) for t in new_dag_state])

            response = self.render_template(
                'airflow/confirm.html',
                message="Here's the list of task instances you are about to mark as failed",
                details=details)

            return response

    def _mark_dagrun_state_as_success(self, dag_id, execution_date, confirmed, origin):
        if not execution_date:
            flash('Invalid execution date', 'error')
            return redirect(origin)

        execution_date = timezone.parse(execution_date)
        dag = current_app.dag_bag.get_dag(dag_id)

        if not dag:
            flash('Cannot find DAG: {}'.format(dag_id), 'error')
            return redirect(origin)

        new_dag_state = set_dag_run_state_to_success(dag, execution_date,
                                                     commit=confirmed)

        if confirmed:
            flash('Marked success on {} task instances'.format(len(new_dag_state)))
            return redirect(origin)

        else:
            details = '\n'.join([str(t) for t in new_dag_state])

            response = self.render_template(
                'airflow/confirm.html',
                message="Here's the list of task instances you are about to mark as success",
                details=details)

            return response

    @expose('/dagrun_failed', methods=['POST'])
    @has_dag_access(can_dag_edit=True)
    @has_access
    @action_logging
    def dagrun_failed(self):
        """Mark DagRun failed."""
        dag_id = request.form.get('dag_id')
        execution_date = request.form.get('execution_date')
        confirmed = request.form.get('confirmed') == 'true'
        origin = get_safe_url(request.form.get('origin'))
        return self._mark_dagrun_state_as_failed(dag_id, execution_date,
                                                 confirmed, origin)

    @expose('/dagrun_success', methods=['POST'])
    @has_dag_access(can_dag_edit=True)
    @has_access
    @action_logging
    def dagrun_success(self):
        """Mark DagRun success"""
        dag_id = request.form.get('dag_id')
        execution_date = request.form.get('execution_date')
        confirmed = request.form.get('confirmed') == 'true'
        origin = get_safe_url(request.form.get('origin'))
        return self._mark_dagrun_state_as_success(dag_id, execution_date,
                                                  confirmed, origin)

    def _mark_task_instance_state(self,  # pylint: disable=too-many-arguments
                                  dag_id, task_id, origin, execution_date,
                                  confirmed, upstream, downstream,
                                  future, past, state):
        dag = current_app.dag_bag.get_dag(dag_id)
        task = dag.get_task(task_id)
        task.dag = dag

        latest_execution_date = dag.get_latest_execution_date()
        if not latest_execution_date:
            flash(f"Cannot make {state}, seem that dag {dag_id} has never run", "error")
            return redirect(origin)

        execution_date = timezone.parse(execution_date)

        from airflow.api.common.experimental.mark_tasks import set_state

        if confirmed:
            altered = set_state(tasks=[task], execution_date=execution_date,
                                upstream=upstream, downstream=downstream,
                                future=future, past=past, state=state,
                                commit=True)

            flash("Marked {} on {} task instances".format(state, len(altered)))
            return redirect(origin)

        to_be_altered = set_state(tasks=[task], execution_date=execution_date,
                                  upstream=upstream, downstream=downstream,
                                  future=future, past=past, state=state,
                                  commit=False)

        details = "\n".join([str(t) for t in to_be_altered])

        response = self.render_template(
            "airflow/confirm.html",
            message=("Here's the list of task instances you are about to mark as {}:".format(state)),
            details=details)

        return response

    @expose('/failed', methods=['POST'])
    @has_dag_access(can_dag_edit=True)
    @has_access
    @action_logging
    def failed(self):
        """Mark task as failed."""
        dag_id = request.form.get('dag_id')
        task_id = request.form.get('task_id')
        origin = get_safe_url(request.form.get('origin'))
        execution_date = request.form.get('execution_date')

        confirmed = request.form.get('confirmed') == "true"
        upstream = request.form.get('failed_upstream') == "true"
        downstream = request.form.get('failed_downstream') == "true"
        future = request.form.get('failed_future') == "true"
        past = request.form.get('failed_past') == "true"

        return self._mark_task_instance_state(dag_id, task_id, origin, execution_date,
                                              confirmed, upstream, downstream,
                                              future, past, State.FAILED)

    @expose('/success', methods=['POST'])
    @has_dag_access(can_dag_edit=True)
    @has_access
    @action_logging
    def success(self):
        """Mark task as success."""
        dag_id = request.form.get('dag_id')
        task_id = request.form.get('task_id')
        origin = get_safe_url(request.form.get('origin'))
        execution_date = request.form.get('execution_date')

        confirmed = request.form.get('confirmed') == "true"
        upstream = request.form.get('success_upstream') == "true"
        downstream = request.form.get('success_downstream') == "true"
        future = request.form.get('success_future') == "true"
        past = request.form.get('success_past') == "true"

        return self._mark_task_instance_state(dag_id, task_id, origin, execution_date,
                                              confirmed, upstream, downstream,
                                              future, past, State.SUCCESS)

    @expose('/tree')
    @has_dag_access(can_dag_read=True)
    @has_access
    @gzipped
    @action_logging
    def tree(self):  # pylint: disable=too-many-locals
        """Get Dag as tree."""
        dag_id = request.args.get('dag_id')
        blur = conf.getboolean('webserver', 'demo_mode')
        dag = current_app.dag_bag.get_dag(dag_id)
        if not dag:
            flash('DAG "{0}" seems to be missing from DagBag.'.format(dag_id), "error")
            return redirect(url_for('Airflow.index'))

        root = request.args.get('root')
        if root:
            dag = dag.sub_dag(
                task_regex=root,
                include_downstream=False,
                include_upstream=True)

        base_date = request.args.get('base_date')
        num_runs = request.args.get('num_runs')
        if num_runs:
            num_runs = int(num_runs)
        else:
            num_runs = conf.getint('webserver', 'default_dag_run_display_number')

        if base_date:
            base_date = timezone.parse(base_date)
        else:
            base_date = dag.get_latest_execution_date() or timezone.utcnow()

        with create_session() as session:
            dag_runs = (
                session.query(DagRun)
                .filter(
                    DagRun.dag_id == dag.dag_id,
                    DagRun.execution_date <= base_date)
                .order_by(DagRun.execution_date.desc())
                .limit(num_runs)
                .all()
            )
        dag_runs = {
            dr.execution_date: alchemy_to_dict(dr) for dr in dag_runs
        }

        dates = sorted(list(dag_runs.keys()))
        max_date = max(dates) if dates else None
        min_date = min(dates) if dates else None

        tis = dag.get_task_instances(start_date=min_date, end_date=base_date)
        task_instances: Dict[Tuple[str, datetime], models.TaskInstance] = {}
        for ti in tis:
            task_instances[(ti.task_id, ti.execution_date)] = ti

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
                    task_instance_data[3] = int(task_instance.duration)

            return task_instance_data

        def recurse_nodes(task, visited):
            nonlocal node_count
            node_count += 1
            visited.add(task)
            task_id = task.task_id

            node = {
                'name': task.task_id,
                'instances': [
                    encode_ti(task_instances.get((task_id, d)))
                    for d in dates
                ],
                'num_dep': len(task.downstream_list),
                'operator': task.task_type,
                'retries': task.retries,
                'owner': task.owner,
                'ui_color': task.ui_color,
            }

            if task.downstream_list:
                children = [
                    recurse_nodes(t, visited) for t in task.downstream_list
                    if node_count < node_limit or t not in visited]

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

        data = {
            'name': '[DAG]',
            'children': [recurse_nodes(t, set()) for t in dag.roots],
            'instances': [
                dag_runs.get(d) or {'execution_date': d.isoformat()}
                for d in dates
            ],
        }

        form = DateTimeWithNumRunsForm(data={'base_date': max_date,
                                             'num_runs': num_runs})

        doc_md = wwwutils.wrapped_markdown(getattr(dag, 'doc_md', None), css_class='dag-doc')

        task_log_reader = TaskLogReader()
        if task_log_reader.supports_external_link:
            external_log_name = task_log_reader.log_handler.log_name
        else:
            external_log_name = None

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
            blur=blur, num_runs=num_runs,
            show_external_log_redirect=task_log_reader.supports_external_link,
            external_log_name=external_log_name)

    @expose('/graph')
    @has_dag_access(can_dag_read=True)
    @has_access
    @gzipped
    @action_logging
    @provide_session
    def graph(self, session=None):
        """Get DAG as Graph."""
        dag_id = request.args.get('dag_id')
        blur = conf.getboolean('webserver', 'demo_mode')
        dag = current_app.dag_bag.get_dag(dag_id)
        if not dag:
            flash('DAG "{0}" seems to be missing.'.format(dag_id), "error")
            return redirect(url_for('Airflow.index'))

        root = request.args.get('root')
        if root:
            dag = dag.sub_dag(
                task_regex=root,
                include_upstream=True,
                include_downstream=False)

        arrange = request.args.get('arrange', dag.orientation)

        nodes = []
        edges = []
        for dag_task in dag.tasks:
            nodes.append({
                'id': dag_task.task_id,
                'value': {
                    'label': dag_task.task_id,
                    'labelStyle': "fill:{0};".format(dag_task.ui_fgcolor),
                    'style': "fill:{0};".format(dag_task.ui_color),
                    'rx': 5,
                    'ry': 5,
                }
            })

        def get_downstream(task):
            for downstream_task in task.downstream_list:
                edge = {
                    'source_id': task.task_id,
                    'target_id': downstream_task.task_id,
                }
                if edge not in edges:
                    edges.append(edge)
                    get_downstream(downstream_task)

        for dag_task in dag.roots:
            get_downstream(dag_task)

        dt_nr_dr_data = get_date_time_num_runs_dag_runs_form_data(request, session, dag)
        dt_nr_dr_data['arrange'] = arrange
        dttm = dt_nr_dr_data['dttm']

        class GraphForm(DateTimeWithNumRunsWithDagRunsForm):
            """Graph Form class."""
            arrange = SelectField("Layout", choices=(
                ('LR', "Left->Right"),
                ('RL', "Right->Left"),
                ('TB', "Top->Bottom"),
                ('BT', "Bottom->Top"),
            ))

        form = GraphForm(data=dt_nr_dr_data)
        form.execution_date.choices = dt_nr_dr_data['dr_choices']

        task_instances = {
            ti.task_id: alchemy_to_dict(ti)
            for ti in dag.get_task_instances(dttm, dttm)}
        tasks = {
            t.task_id: {
                'dag_id': t.dag_id,
                'task_type': t.task_type,
                'extra_links': t.extra_links,
            }
            for t in dag.tasks}
        if not tasks:
            flash("No tasks found", "error")
        session.commit()
        doc_md = wwwutils.wrapped_markdown(getattr(dag, 'doc_md', None), css_class='dag-doc')

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
            blur=blur,
            root=root or '',
            task_instances=task_instances,
            tasks=tasks,
            nodes=nodes,
            edges=edges,
            show_external_log_redirect=task_log_reader.supports_external_link,
            external_log_name=external_log_name)

    @expose('/duration')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    @provide_session
    def duration(self, session=None):  # pylint: disable=too-many-locals
        """Get Dag as duration graph."""
        default_dag_run = conf.getint('webserver', 'default_dag_run_display_number')
        dag_id = request.args.get('dag_id')
        dag = current_app.dag_bag.get_dag(dag_id)
        base_date = request.args.get('base_date')
        num_runs = request.args.get('num_runs')
        num_runs = int(num_runs) if num_runs else default_dag_run

        if dag is None:
            flash('DAG "{0}" seems to be missing.'.format(dag_id), "error")
            return redirect(url_for('Airflow.index'))

        if base_date:
            base_date = timezone.parse(base_date)
        else:
            base_date = dag.get_latest_execution_date() or timezone.utcnow()

        dates = dag.date_range(base_date, num=-abs(num_runs))
        min_date = dates[0] if dates else timezone.utc_epoch()

        root = request.args.get('root')
        if root:
            dag = dag.sub_dag(
                task_regex=root,
                include_upstream=True,
                include_downstream=False)

        chart_height = wwwutils.get_chart_height(dag)
        chart = nvd3.lineChart(
            name="lineChart", x_is_date=True, height=chart_height, width="1200")
        cum_chart = nvd3.lineChart(
            name="cumLineChart", x_is_date=True, height=chart_height, width="1200")

        y_points = defaultdict(list)
        x_points = defaultdict(list)
        cumulative_y = defaultdict(list)

        task_instances = dag.get_task_instances(start_date=min_date, end_date=base_date)
        ti_fails = (
            session.query(TaskFail)
            .filter(TaskFail.dag_id == dag.dag_id,
                    TaskFail.execution_date >= min_date,
                    TaskFail.execution_date <= base_date,
                    TaskFail.task_id.in_([t.task_id for t in dag.tasks]))
            .all()
        )

        fails_totals = defaultdict(int)
        for failed_task_instance in ti_fails:
            dict_key = (failed_task_instance.dag_id, failed_task_instance.task_id,
                        failed_task_instance.execution_date)
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
        chart.create_y_axis('yAxis', format='.02f', custom_format=False,
                            label='Duration ({})'.format(y_unit))
        chart.axislist['yAxis']['axisLabelDistance'] = '-15'
        cum_chart.create_y_axis('yAxis', format='.02f', custom_format=False,
                                label='Duration ({})'.format(cum_y_unit))
        cum_chart.axislist['yAxis']['axisLabelDistance'] = '-15'

        for task in dag.tasks:
            if x_points[task.task_id]:
                chart.add_serie(name=task.task_id, x=x_points[task.task_id],
                                y=scale_time_units(y_points[task.task_id], y_unit))
                cum_chart.add_serie(name=task.task_id, x=x_points[task.task_id],
                                    y=scale_time_units(cumulative_y[task.task_id],
                                                       cum_y_unit))

        dates = sorted(list({ti.execution_date for ti in task_instances}))
        max_date = max([ti.execution_date for ti in task_instances]) if dates else None

        session.commit()

        form = DateTimeWithNumRunsForm(data={'base_date': max_date,
                                             'num_runs': num_runs})
        chart.buildcontent()
        cum_chart.buildcontent()
        s_index = cum_chart.htmlcontent.rfind('});')
        cum_chart.htmlcontent = (cum_chart.htmlcontent[:s_index] +
                                 "$( document ).trigger('chartload')" +
                                 cum_chart.htmlcontent[s_index:])

        return self.render_template(
            'airflow/duration_chart.html',
            dag=dag,
            demo_mode=conf.getboolean('webserver', 'demo_mode'),
            root=root,
            form=form,
            chart=Markup(chart.htmlcontent),
            cum_chart=Markup(cum_chart.htmlcontent),
        )

    @expose('/tries')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    @provide_session
    def tries(self, session=None):
        """Shows all tries."""
        default_dag_run = conf.getint('webserver', 'default_dag_run_display_number')
        dag_id = request.args.get('dag_id')
        dag = current_app.dag_bag.get_dag(dag_id)
        base_date = request.args.get('base_date')
        num_runs = request.args.get('num_runs')
        num_runs = int(num_runs) if num_runs else default_dag_run

        if base_date:
            base_date = timezone.parse(base_date)
        else:
            base_date = dag.get_latest_execution_date() or timezone.utcnow()

        dates = dag.date_range(base_date, num=-abs(num_runs))
        min_date = dates[0] if dates else timezone.utc_epoch()

        root = request.args.get('root')
        if root:
            dag = dag.sub_dag(
                task_regex=root,
                include_upstream=True,
                include_downstream=False)

        chart_height = wwwutils.get_chart_height(dag)
        chart = nvd3.lineChart(
            name="lineChart", x_is_date=True, y_axis_format='d', height=chart_height,
            width="1200")

        for task in dag.tasks:
            y_points = []
            x_points = []
            for ti in task.get_task_instances(start_date=min_date, end_date=base_date):
                dttm = wwwutils.epoch(ti.execution_date)
                x_points.append(dttm)
                # y value should reflect completed tries to have a 0 baseline.
                y_points.append(ti.prev_attempted_tries)
            if x_points:
                chart.add_serie(name=task.task_id, x=x_points, y=y_points)

        tis = dag.get_task_instances(start_date=min_date, end_date=base_date)
        tries = sorted(list({ti.try_number for ti in tis}))
        max_date = max([ti.execution_date for ti in tis]) if tries else None

        session.commit()

        form = DateTimeWithNumRunsForm(data={'base_date': max_date,
                                             'num_runs': num_runs})

        chart.buildcontent()

        return self.render_template(
            'airflow/chart.html',
            dag=dag,
            demo_mode=conf.getboolean('webserver', 'demo_mode'),
            root=root,
            form=form,
            chart=Markup(chart.htmlcontent),
            tab_title='Tries',
        )

    @expose('/landing_times')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    @provide_session
    def landing_times(self, session=None):
        """Shows landing times."""
        default_dag_run = conf.getint('webserver', 'default_dag_run_display_number')
        dag_id = request.args.get('dag_id')
        dag = current_app.dag_bag.get_dag(dag_id)
        base_date = request.args.get('base_date')
        num_runs = request.args.get('num_runs')
        num_runs = int(num_runs) if num_runs else default_dag_run

        if base_date:
            base_date = timezone.parse(base_date)
        else:
            base_date = dag.get_latest_execution_date() or timezone.utcnow()

        dates = dag.date_range(base_date, num=-abs(num_runs))
        min_date = dates[0] if dates else timezone.utc_epoch()

        root = request.args.get('root')
        if root:
            dag = dag.sub_dag(
                task_regex=root,
                include_upstream=True,
                include_downstream=False)

        chart_height = wwwutils.get_chart_height(dag)
        chart = nvd3.lineChart(
            name="lineChart", x_is_date=True, height=chart_height, width="1200")
        y_points = {}
        x_points = {}
        for task in dag.tasks:
            task_id = task.task_id
            y_points[task_id] = []
            x_points[task_id] = []
            for ti in task.get_task_instances(start_date=min_date, end_date=base_date):
                ts = ti.execution_date
                if dag.schedule_interval and dag.following_schedule(ts):
                    ts = dag.following_schedule(ts)
                if ti.end_date:
                    dttm = wwwutils.epoch(ti.execution_date)
                    secs = (ti.end_date - ts).total_seconds()
                    x_points[task_id].append(dttm)
                    y_points[task_id].append(secs)

        # determine the most relevant time unit for the set of landing times
        # for the DAG
        y_unit = infer_time_unit([d for t in y_points.values() for d in t])
        # update the y Axis to have the correct time units
        chart.create_y_axis('yAxis', format='.02f', custom_format=False,
                            label='Landing Time ({})'.format(y_unit))
        chart.axislist['yAxis']['axisLabelDistance'] = '-15'
        for task in dag.tasks:
            if x_points[task.task_id]:
                chart.add_serie(name=task.task_id, x=x_points[task.task_id],
                                y=scale_time_units(y_points[task.task_id], y_unit))

        tis = dag.get_task_instances(start_date=min_date, end_date=base_date)
        dates = sorted(list({ti.execution_date for ti in tis}))
        max_date = max([ti.execution_date for ti in tis]) if dates else None

        session.commit()

        form = DateTimeWithNumRunsForm(data={'base_date': max_date,
                                             'num_runs': num_runs})
        chart.buildcontent()
        return self.render_template(
            'airflow/chart.html',
            dag=dag,
            chart=Markup(chart.htmlcontent),
            height=str(chart_height + 100) + "px",
            demo_mode=conf.getboolean('webserver', 'demo_mode'),
            root=root,
            form=form,
            tab_title='Landing times',
        )

    @expose('/paused', methods=['POST'])
    @has_dag_access(can_dag_edit=True)
    @has_access
    @action_logging
    def paused(self):
        """Toggle paused."""
        dag_id = request.args.get('dag_id')
        is_paused = request.args.get('is_paused') == 'false'
        models.DagModel.get_dagmodel(dag_id).set_is_paused(
            is_paused=is_paused)
        return "OK"

    @expose('/refresh', methods=['POST'])
    @has_dag_access(can_dag_edit=True)
    @has_access
    @action_logging
    @provide_session
    def refresh(self, session=None):
        """Refresh DAG."""
        dag_id = request.values.get('dag_id')
        orm_dag = session.query(
            DagModel).filter(DagModel.dag_id == dag_id).first()

        if orm_dag:
            orm_dag.last_expired = timezone.utcnow()
            session.merge(orm_dag)
        session.commit()

        dag = current_app.dag_bag.get_dag(dag_id)
        # sync dag permission
        current_app.appbuilder.sm.sync_perm_for_dag(dag_id, dag.access_control)

        flash("DAG [{}] is now fresh as a daisy".format(dag_id))
        return redirect(request.referrer)

    @expose('/refresh_all', methods=['POST'])
    @has_access
    @action_logging
    def refresh_all(self):
        """Refresh everything"""
        if settings.STORE_SERIALIZED_DAGS:
            current_app.dag_bag.collect_dags_from_db()
        else:
            current_app.dag_bag.collect_dags(only_if_updated=False)

        # sync permissions for all dags
        for dag_id, dag in current_app.dag_bag.dags.items():
            current_app.appbuilder.sm.sync_perm_for_dag(dag_id, dag.access_control)
        flash("All DAGs are now up to date")
        return redirect(url_for('Airflow.index'))

    @expose('/gantt')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    @provide_session
    def gantt(self, session=None):
        """Show GANTT chart."""
        dag_id = request.args.get('dag_id')
        dag = current_app.dag_bag.get_dag(dag_id)
        demo_mode = conf.getboolean('webserver', 'demo_mode')

        root = request.args.get('root')
        if root:
            dag = dag.sub_dag(
                task_regex=root,
                include_upstream=True,
                include_downstream=False)

        dt_nr_dr_data = get_date_time_num_runs_dag_runs_form_data(request, session, dag)
        dttm = dt_nr_dr_data['dttm']

        form = DateTimeWithNumRunsWithDagRunsForm(data=dt_nr_dr_data)
        form.execution_date.choices = dt_nr_dr_data['dr_choices']

        tis = [
            ti for ti in dag.get_task_instances(dttm, dttm)
            if ti.start_date and ti.state]
        tis = sorted(tis, key=lambda ti: ti.start_date)
        ti_fails = list(itertools.chain(*[(
            session
            .query(TaskFail)
            .filter(TaskFail.dag_id == ti.dag_id,
                    TaskFail.task_id == ti.task_id,
                    TaskFail.execution_date == ti.execution_date)
            .all()
        ) for ti in tis]))

        # determine bars to show in the gantt chart
        gantt_bar_items = []

        tasks = []
        for ti in tis:
            end_date = ti.end_date or timezone.utcnow()
            # prev_attempted_tries will reflect the currently running try_number
            # or the try_number of the last complete run
            # https://issues.apache.org/jira/browse/AIRFLOW-2143
            try_count = ti.prev_attempted_tries
            gantt_bar_items.append((ti.task_id, ti.start_date, end_date, ti.state, try_count))
            task_dict = alchemy_to_dict(ti)
            task_dict['extraLinks'] = dag.get_task(ti.task_id).extra_links
            tasks.append(task_dict)

        tf_count = 0
        try_count = 1
        prev_task_id = ""
        for failed_task_instance in ti_fails:
            end_date = failed_task_instance.end_date or timezone.utcnow()
            start_date = failed_task_instance.start_date or end_date
            if tf_count != 0 and failed_task_instance.task_id == prev_task_id:
                try_count = try_count + 1
            else:
                try_count = 1
            prev_task_id = failed_task_instance.task_id
            gantt_bar_items.append((failed_task_instance.task_id, start_date, end_date, State.FAILED,
                                    try_count))
            tf_count = tf_count + 1
            task = dag.get_task(failed_task_instance.task_id)
            task_dict = alchemy_to_dict(failed_task_instance)
            task_dict['state'] = State.FAILED
            task_dict['operator'] = task.task_type
            task_dict['try_number'] = try_count
            task_dict['extraLinks'] = task.extra_links
            tasks.append(task_dict)

        data = {
            'taskNames': [ti.task_id for ti in tis],
            'tasks': tasks,
            'height': len(tis) * 25 + 25,
        }

        session.commit()

        return self.render_template(
            'airflow/gantt.html',
            dag=dag,
            execution_date=dttm.isoformat(),
            form=form,
            data=data,
            base_date='',
            demo_mode=demo_mode,
            root=root,
        )

    @expose('/extra_links')
    @has_dag_access(can_dag_read=True)
    @has_access
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
                {'url': None,
                 'error': "can't find dag {dag} or task_id {task_id}".format(
                     dag=dag,
                     task_id=task_id
                 )}
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
            response = jsonify(
                {'url': None, 'error': 'No URL found for {dest}'.format(dest=link_name)})
            response.status_code = 404
            return response

    @expose('/object/task_instances')
    @has_dag_access(can_dag_read=True)
    @has_access
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

        task_instances = {
            ti.task_id: alchemy_to_dict(ti)
            for ti in dag.get_task_instances(dttm, dttm)}

        return json.dumps(task_instances)


class VersionView(AirflowBaseView):
    """View to show Airflow Version and optionally Git commit SHA"""
    default_view = 'version'

    @expose('/version')
    @has_access
    def version(self):
        """Shows Airflow version."""
        try:
            airflow_version = airflow.__version__
        except Exception as e:  # pylint: disable=broad-except
            airflow_version = None
            logging.error(e)

        git_version = get_airflow_git_version()
        # Render information
        title = "Version Info"
        return self.render_template(
            'airflow/version.html',
            title=title,
            airflow_version=airflow_version,
            git_version=git_version)


class ConfigurationView(AirflowBaseView):
    """View to show Airflow Configurations"""
    default_view = 'conf'

    @expose('/configuration')
    @has_access
    def conf(self):
        """Shows configuration."""
        raw = request.args.get('raw') == "true"
        title = "Airflow Configuration"
        subtitle = AIRFLOW_CONFIG
        # Don't show config when expose_config variable is False in airflow config
        if conf.getboolean("webserver", "expose_config"):
            with open(AIRFLOW_CONFIG, 'r') as file:
                config = file.read()
            table = [(section, key, value, source)
                     for section, parameters in conf.as_dict(True, True).items()
                     for key, (value, source) in parameters.items()]
        else:
            config = (
                "# Your Airflow administrator chose not to expose the "
                "configuration, most likely for security reasons.")
            table = None

        if raw:
            return Response(
                response=config,
                status=200,
                mimetype="application/text")
        else:
            code_html = Markup(highlight(
                config,
                lexers.IniLexer(),  # Lexer call pylint: disable=no-member
                HtmlFormatter(noclasses=True))
            )
            return self.render_template(
                'airflow/config.html',
                pre_subtitle=settings.HEADER + "  v" + airflow.__version__,
                code_html=code_html, title=title, subtitle=subtitle,
                table=table)


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
    def apply(self, query, func): # noqa pylint: disable=redefined-outer-name,unused-argument
        if current_app.appbuilder.sm.has_all_dags_access():
            return query
        filter_dag_ids = current_app.appbuilder.sm.get_accessible_dag_ids()
        return query.filter(self.model.dag_id.in_(filter_dag_ids))


class AirflowModelView(ModelView):  # noqa: D101
    """Airflow Mode View."""
    list_widget = AirflowModelListWidget
    page_size = PAGE_SIZE

    CustomSQLAInterface = wwwutils.CustomSQLAInterface


class SlaMissModelView(AirflowModelView):
    """View to show SlaMiss table"""
    route_base = '/slamiss'

    datamodel = AirflowModelView.CustomSQLAInterface(SlaMiss)  # noqa # type: ignore

    base_permissions = ['can_list']

    list_columns = ['dag_id', 'task_id', 'execution_date', 'email_sent', 'timestamp']
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

    datamodel = AirflowModelView.CustomSQLAInterface(XCom)

    base_permissions = ['can_list', 'can_delete']

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

    @action('muldelete', 'Delete', "Are you sure you want to delete selected records?",
            single=False)
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


class ConnectionModelView(AirflowModelView):
    """View to show records from Connections table"""
    route_base = '/connection'

    datamodel = AirflowModelView.CustomSQLAInterface(Connection)  # noqa # type: ignore

    base_permissions = ['can_add', 'can_list', 'can_edit', 'can_delete']

    extra_fields = ['extra__jdbc__drv_path', 'extra__jdbc__drv_clsname',
                    'extra__google_cloud_platform__project',
                    'extra__google_cloud_platform__key_path',
                    'extra__google_cloud_platform__keyfile_dict',
                    'extra__google_cloud_platform__scope',
                    'extra__google_cloud_platform__num_retries',
                    'extra__grpc__auth_type',
                    'extra__grpc__credential_pem_file',
                    'extra__grpc__scopes',
                    'extra__yandexcloud__service_account_json',
                    'extra__yandexcloud__service_account_json_path',
                    'extra__yandexcloud__oauth',
                    'extra__yandexcloud__public_ssh_key',
                    'extra__yandexcloud__folder_id',
                    'extra__kubernetes__in_cluster',
                    'extra__kubernetes__kube_config',
                    'extra__kubernetes__namespace']
    list_columns = ['conn_id', 'conn_type', 'host', 'port', 'is_encrypted',
                    'is_extra_encrypted']
    add_columns = edit_columns = ['conn_id', 'conn_type', 'host', 'schema',
                                  'login', 'password', 'port', 'extra'] + extra_fields
    add_form = edit_form = ConnectionForm
    add_template = 'airflow/conn_create.html'
    edit_template = 'airflow/conn_edit.html'

    base_order = ('conn_id', 'asc')

    @action('muldelete', 'Delete', 'Are you sure you want to delete selected records?',
            single=False)
    @has_dag_access(can_dag_edit=True)
    def action_muldelete(self, items):
        """Multiple delete."""
        self.datamodel.delete_all(items)
        self.update_redirect()
        return redirect(self.get_redirect())

    def process_form(self, form, is_created):
        """Process form data."""
        formdata = form.data
        if formdata['conn_type'] in ['jdbc', 'google_cloud_platform', 'grpc', 'yandexcloud', 'kubernetes']:
            extra = {
                key: formdata[key]
                for key in self.extra_fields if key in formdata}
            form.extra.data = json.dumps(extra)

    def prefill_form(self, form, pk):
        """Prefill the form."""
        try:
            extra_dictionary = json.loads(form.data.get('extra', '{}'))
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


class PoolModelView(AirflowModelView):
    """View to show records from Pool table"""
    route_base = '/pool'

    datamodel = AirflowModelView.CustomSQLAInterface(models.Pool)  # noqa # type: ignore

    base_permissions = ['can_add', 'can_list', 'can_edit', 'can_delete']

    list_columns = ['pool', 'slots', 'running_slots', 'queued_slots']
    add_columns = ['pool', 'slots', 'description']
    edit_columns = ['pool', 'slots', 'description']

    base_order = ('pool', 'asc')

    @action('muldelete', 'Delete', 'Are you sure you want to delete selected records?',
            single=False)
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
        pool_id = self.get('pool')  # noqa pylint: disable=no-member
        if pool_id is not None:
            url = url_for('TaskInstanceModelView.list', _flt_3_pool=pool_id)
            return Markup("<a href='{url}'>{pool_id}</a>").format(url=url, pool_id=pool_id)  # noqa
        else:
            return Markup('<span class="label label-danger">Invalid</span>')

    def frunning_slots(self):
        """Running slots rendering."""
        pool_id = self.get('pool')  # noqa pylint: disable=no-member
        running_slots = self.get('running_slots')  # noqa pylint: disable=no-member
        if pool_id is not None and running_slots is not None:
            url = url_for('TaskInstanceModelView.list', _flt_3_pool=pool_id, _flt_3_state='running')
            return Markup("<a href='{url}'>{running_slots}</a>").format(  # noqa
                url=url,
                running_slots=running_slots)
        else:
            return Markup('<span class="label label-danger">Invalid</span>')

    def fqueued_slots(self):
        """Queued slots rendering."""
        pool_id = self.get('pool')  # noqa pylint: disable=no-member
        queued_slots = self.get('queued_slots')  # noqa pylint: disable=no-member
        if pool_id is not None and queued_slots is not None:
            url = url_for('TaskInstanceModelView.list', _flt_3_pool=pool_id, _flt_3_state='queued')
            return Markup("<a href='{url}'>{queued_slots}</a>").format(  # noqa
                url=url, queued_slots=queued_slots)
        else:
            return Markup('<span class="label label-danger">Invalid</span>')

    formatters_columns = {
        'pool': pool_link,
        'running_slots': frunning_slots,
        'queued_slots': fqueued_slots
    }

    validators_columns = {
        'pool': [validators.DataRequired()],
        'slots': [validators.NumberRange(min=-1)]
    }


class VariableModelView(AirflowModelView):
    """View to show records from Variable table"""
    route_base = '/variable'

    list_template = 'airflow/variable_list.html'
    edit_template = 'airflow/variable_edit.html'

    datamodel = AirflowModelView.CustomSQLAInterface(models.Variable)  # noqa # type: ignore

    base_permissions = ['can_add', 'can_list', 'can_edit', 'can_delete', 'can_varimport']

    list_columns = ['key', 'val', 'is_encrypted']
    add_columns = ['key', 'val']
    edit_columns = ['key', 'val']
    search_columns = ['key', 'val']

    base_order = ('key', 'asc')

    def hidden_field_formatter(self):
        """Formats hidden fields"""
        key = self.get('key')  # noqa pylint: disable=no-member
        val = self.get('val')  # noqa pylint: disable=no-member
        if wwwutils.should_hide_value_for_key(key):
            return Markup('*' * 8)
        if val:
            return val
        else:
            return Markup('<span class="label label-danger">Invalid</span>')

    formatters_columns = {
        'val': hidden_field_formatter,
    }

    validators_columns = {
        'key': [validators.DataRequired()]
    }

    def prefill_form(self, form, request_id):  # pylint: disable=unused-argument
        if wwwutils.should_hide_value_for_key(form.key.data):
            form.val.data = '*' * 8

    @action('muldelete', 'Delete', 'Are you sure you want to delete selected records?',
            single=False)
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
            except Exception:  # noqa pylint: disable=broad-except
                val = var.val
            var_dict[var.key] = val

        response = make_response(json.dumps(var_dict, sort_keys=True, indent=4))
        response.headers["Content-Disposition"] = "attachment; filename=variables.json"
        response.headers["Content-Type"] = "application/json; charset=utf-8"
        return response

    @expose('/varimport', methods=["POST"])
    @has_access
    @action_logging
    def varimport(self):
        """Import variables"""
        try:
            out = request.files['file'].read()
            if isinstance(out, bytes):
                variable_dict = json.loads(out.decode('utf-8'))
            else:
                variable_dict = json.loads(out)
        except Exception:  # noqa pylint: disable=broad-except
            self.update_redirect()
            flash("Missing file or syntax error.", 'error')
            return redirect(self.get_redirect())
        else:
            suc_count = fail_count = 0
            for k, v in variable_dict.items():
                try:
                    models.Variable.set(k, v, serialize_json=not isinstance(v, str))
                except Exception as e:  # pylint: disable=broad-except
                    logging.info('Variable import failed: %s', repr(e))
                    fail_count += 1
                else:
                    suc_count += 1
            flash("{} variable(s) successfully updated.".format(suc_count))
            if fail_count:
                flash("{} variable(s) failed to be updated.".format(fail_count), 'error')
            self.update_redirect()
            return redirect(self.get_redirect())


class JobModelView(AirflowModelView):
    """View to show records from Job table"""
    route_base = '/job'

    datamodel = AirflowModelView.CustomSQLAInterface(BaseJob)  # noqa # type: ignore

    base_permissions = ['can_list']

    list_columns = ['id', 'dag_id', 'state', 'job_type', 'start_date',
                    'end_date', 'latest_heartbeat',
                    'executor_class', 'hostname', 'unixname']
    search_columns = ['id', 'dag_id', 'state', 'job_type', 'start_date',
                      'end_date', 'latest_heartbeat', 'executor_class',
                      'hostname', 'unixname']

    base_order = ('start_date', 'desc')

    base_filters = [['dag_id', DagFilter, lambda: []]]

    formatters_columns = {
        'start_date': wwwutils.datetime_f('start_date'),
        'end_date': wwwutils.datetime_f('end_date'),
        'hostname': wwwutils.nobr_f('hostname'),
        'state': wwwutils.state_f,
        'latest_heartbeat': wwwutils.datetime_f('latest_heartbeat'),
    }


class DagRunModelView(AirflowModelView):
    """View to show records from DagRun table"""
    route_base = '/dagrun'

    datamodel = AirflowModelView.CustomSQLAInterface(models.DagRun)  # noqa # type: ignore

    base_permissions = ['can_list', 'can_add']

    add_columns = ['state', 'dag_id', 'execution_date', 'run_id', 'external_trigger', 'conf']
    list_columns = ['state', 'dag_id', 'execution_date', 'run_id', 'run_type', 'external_trigger', 'conf']
    search_columns = ['state', 'dag_id', 'execution_date', 'run_id', 'run_type', 'external_trigger', 'conf']

    base_order = ('execution_date', 'desc')

    base_filters = [['dag_id', DagFilter, lambda: []]]

    add_form = edit_form = DagRunForm

    formatters_columns = {
        'execution_date': wwwutils.datetime_f('execution_date'),
        'state': wwwutils.state_f,
        'start_date': wwwutils.datetime_f('start_date'),
        'dag_id': wwwutils.dag_link,
        'run_id': wwwutils.dag_run_link,
    }

    @action('muldelete', "Delete", "Are you sure you want to delete selected records?",
            single=False)
    @has_dag_access(can_dag_edit=True)
    @provide_session
    def action_muldelete(self, items, session=None):  # noqa # pylint: disable=unused-argument
        """Multiple delete."""
        self.datamodel.delete_all(items)
        self.update_redirect()
        dirty_ids = []
        for item in items:
            dirty_ids.append(item.dag_id)
        return redirect(self.get_redirect())

    @action('set_running', "Set state to 'running'", '', single=False)
    @provide_session
    def action_set_running(self, drs, session=None):
        """Set state to running."""
        try:
            count = 0
            dirty_ids = []
            for dr in session.query(DagRun).filter(
                    DagRun.id.in_([dagrun.id for dagrun in drs])).all():  # noqa pylint: disable=no-member
                dirty_ids.append(dr.dag_id)
                count += 1
                dr.start_date = timezone.utcnow()
                dr.state = State.RUNNING
            session.commit()
            flash("{count} dag runs were set to running".format(count=count))
        except Exception as ex:  # pylint: disable=broad-except
            flash(str(ex), 'error')
            flash('Failed to set state', 'error')
        return redirect(self.get_default_url())

    @action('set_failed', "Set state to 'failed'",
            "All running task instances would also be marked as failed, are you sure?",
            single=False)
    @provide_session
    def action_set_failed(self, drs, session=None):
        """Set state to failed."""
        try:
            count = 0
            dirty_ids = []
            altered_tis = []
            for dr in session.query(DagRun).filter(
                    DagRun.id.in_([dagrun.id for dagrun in drs])).all():  # noqa pylint: disable=no-member
                dirty_ids.append(dr.dag_id)
                count += 1
                altered_tis += \
                    set_dag_run_state_to_failed(current_app.dag_bag.get_dag(dr.dag_id),
                                                dr.execution_date,
                                                commit=True,
                                                session=session)
            altered_ti_count = len(altered_tis)
            flash(
                "{count} dag runs and {altered_ti_count} task instances "
                "were set to failed".format(count=count, altered_ti_count=altered_ti_count))
        except Exception:  # noqa pylint: disable=broad-except
            flash('Failed to set state', 'error')
        return redirect(self.get_default_url())

    @action('set_success', "Set state to 'success'",
            "All task instances would also be marked as success, are you sure?",
            single=False)
    @provide_session
    def action_set_success(self, drs, session=None):
        """Set state to success."""
        try:
            count = 0
            dirty_ids = []
            altered_tis = []
            for dr in session.query(DagRun).filter(
                    DagRun.id.in_([dagrun.id for dagrun in drs])).all():  # noqa pylint: disable=no-member
                dirty_ids.append(dr.dag_id)
                count += 1
                altered_tis += \
                    set_dag_run_state_to_success(current_app.dag_bag.get_dag(dr.dag_id),
                                                 dr.execution_date,
                                                 commit=True,
                                                 session=session)
            altered_ti_count = len(altered_tis)
            flash(
                "{count} dag runs and {altered_ti_count} task instances "
                "were set to success".format(count=count, altered_ti_count=altered_ti_count))
        except Exception:  # noqa pylint: disable=broad-except
            flash('Failed to set state', 'error')
        return redirect(self.get_default_url())


class LogModelView(AirflowModelView):
    """View to show records from Log table"""
    route_base = '/log'

    datamodel = AirflowModelView.CustomSQLAInterface(Log)  # noqa # type:ignore

    base_permissions = ['can_list']

    list_columns = ['id', 'dttm', 'dag_id', 'task_id', 'event', 'execution_date',
                    'owner', 'extra']
    search_columns = ['dag_id', 'task_id', 'event', 'execution_date', 'owner', 'extra']

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

    datamodel = AirflowModelView.CustomSQLAInterface(models.TaskReschedule)  # noqa # type: ignore

    base_permissions = ['can_list']

    list_columns = ['id', 'dag_id', 'task_id', 'execution_date', 'try_number',
                    'start_date', 'end_date', 'duration', 'reschedule_date']

    search_columns = ['dag_id', 'task_id', 'execution_date', 'start_date', 'end_date',
                      'reschedule_date']

    base_order = ('id', 'desc')

    base_filters = [['dag_id', DagFilter, lambda: []]]

    def duration_f(self):
        """Duration calculation."""
        end_date = self.get('end_date')  # noqa pylint: disable=no-member
        duration = self.get('duration')  # noqa pylint: disable=no-member
        if end_date and duration:
            return timedelta(seconds=duration)
        return None

    formatters_columns = {
        'dag_id': wwwutils.dag_link,
        'task_id': wwwutils.task_instance_link,
        'start_date': wwwutils.datetime_f('start_date'),
        'end_date': wwwutils.datetime_f('end_date'),
        'execution_date': wwwutils.datetime_f('execution_date'),
        'reschedule_date': wwwutils.datetime_f('reschedule_date'),
        'duration': duration_f,
    }


class TaskInstanceModelView(AirflowModelView):
    """View to show records from TaskInstance table"""
    route_base = '/taskinstance'

    datamodel = AirflowModelView.CustomSQLAInterface(models.TaskInstance)  # noqa # type: ignore

    base_permissions = ['can_list']

    page_size = PAGE_SIZE

    list_columns = ['state', 'dag_id', 'task_id', 'execution_date', 'operator',
                    'start_date', 'end_date', 'duration', 'job_id', 'hostname',
                    'unixname', 'priority_weight', 'queue', 'queued_dttm', 'try_number',
                    'pool', 'log_url']

    order_columns = [item for item in list_columns if item not in ['try_number', 'log_url']]

    search_columns = ['state', 'dag_id', 'task_id', 'execution_date', 'hostname',
                      'queue', 'pool', 'operator', 'start_date', 'end_date']

    base_order = ('job_id', 'asc')

    base_filters = [['dag_id', DagFilter, lambda: []]]

    def log_url_formatter(self):
        """Formats log URL."""
        log_url = self.get('log_url')  # noqa pylint: disable=no-member
        return Markup(  # noqa
            '<a href="{log_url}">'
            '    <span class="glyphicon glyphicon-book" aria-hidden="true">'
            '</span></a>').format(log_url=log_url)

    def duration_f(self):
        """Formats duration."""
        end_date = self.get('end_date')  # noqa pylint: disable=no-member
        duration = self.get('duration')  # noqa pylint: disable=no-member
        if end_date and duration:
            return timedelta(seconds=duration)
        return None

    formatters_columns = {
        'log_url': log_url_formatter,
        'task_id': wwwutils.task_instance_link,
        'hostname': wwwutils.nobr_f('hostname'),
        'state': wwwutils.state_f,
        'execution_date': wwwutils.datetime_f('execution_date'),
        'start_date': wwwutils.datetime_f('start_date'),
        'end_date': wwwutils.datetime_f('end_date'),
        'queued_dttm': wwwutils.datetime_f('queued_dttm'),
        'dag_id': wwwutils.dag_link,
        'duration': duration_f,
    }

    @provide_session
    @action('clear', lazy_gettext('Clear'),
            lazy_gettext('Are you sure you want to clear the state of the selected task'
                         ' instance(s) and set their dagruns to the running state?'),
            single=False)
    def action_clear(self, task_instances, session=None):
        """Clears the action."""
        try:
            dag_to_tis = {}

            for ti in task_instances:
                dag = current_app.dag_bag.get_dag(ti.dag_id)
                task_instances_to_clean = dag_to_tis.setdefault(dag, [])
                task_instances_to_clean.append(ti)

            for dag, task_instances_list in dag_to_tis.items():
                models.clear_task_instances(task_instances_list, session, dag=dag)

            session.commit()
            flash("{0} task instances have been cleared".format(len(task_instances)))
            self.update_redirect()
            return redirect(self.get_redirect())
        except Exception:  # noqa pylint: disable=broad-except
            flash('Failed to clear task instances', 'error')

    @provide_session
    def set_task_instance_state(self, tis, target_state, session=None):
        """Set task instance state."""
        try:
            count = len(tis)
            for ti in tis:
                ti.set_state(target_state, session)
            session.commit()
            flash("{count} task instances were set to '{target_state}'".format(
                count=count, target_state=target_state))
        except Exception:  # noqa pylint: disable=broad-except
            flash('Failed to set state', 'error')

    @action('set_running', "Set state to 'running'", '', single=False)
    @has_dag_access(can_dag_edit=True)
    def action_set_running(self, tis):
        """Set state to 'running'"""
        self.set_task_instance_state(tis, State.RUNNING)
        self.update_redirect()
        return redirect(self.get_redirect())

    @action('set_failed', "Set state to 'failed'", '', single=False)
    @has_dag_access(can_dag_edit=True)
    def action_set_failed(self, tis):
        """Set state to 'failed'"""
        self.set_task_instance_state(tis, State.FAILED)
        self.update_redirect()
        return redirect(self.get_redirect())

    @action('set_success', "Set state to 'success'", '', single=False)
    @has_dag_access(can_dag_edit=True)
    def action_set_success(self, tis):
        """Set state to 'success'"""
        self.set_task_instance_state(tis, State.SUCCESS)
        self.update_redirect()
        return redirect(self.get_redirect())

    @action('set_retry', "Set state to 'up_for_retry'", '', single=False)
    @has_dag_access(can_dag_edit=True)
    def action_set_retry(self, tis):
        """Set state to 'up_for_retry'"""
        self.set_task_instance_state(tis, State.UP_FOR_RETRY)
        self.update_redirect()
        return redirect(self.get_redirect())


class DagModelView(AirflowModelView):
    """View to show records from DAG table"""
    route_base = '/dagmodel'

    datamodel = AirflowModelView.CustomSQLAInterface(DagModel)  # noqa # type: ignore

    base_permissions = ['can_list', 'can_show']

    list_columns = ['dag_id', 'is_paused', 'last_scheduler_run',
                    'last_expired', 'scheduler_lock', 'fileloc', 'owners']

    formatters_columns = {
        'dag_id': wwwutils.dag_link
    }

    base_filters = [['dag_id', DagFilter, lambda: []]]

    def get_query(self):
        """
        Default filters for model
        """
        return (
            super().get_query()  # noqa pylint: disable=no-member
            .filter(or_(models.DagModel.is_active,
                        models.DagModel.is_paused))
            .filter(~models.DagModel.is_subdag)
        )

    def get_count_query(self):
        """
        Default filters for model
        """
        return (
            super().get_count_query()  # noqa pylint: disable=no-member
            .filter(models.DagModel.is_active)
            .filter(~models.DagModel.is_subdag)
        )

    @has_access
    @permission_name("list")
    @provide_session
    @expose('/autocomplete')
    def autocomplete(self, session=None):
        """Autocomplete."""
        query = unquote(request.args.get('query', ''))

        if not query:
            wwwutils.json_response([])

        # Provide suggestions of dag_ids and owners
        dag_ids_query = session.query(DagModel.dag_id.label('item')).filter(  # pylint: disable=no-member
            ~DagModel.is_subdag, DagModel.is_active,
            DagModel.dag_id.ilike('%' + query + '%'))  # noqa pylint: disable=no-member

        owners_query = session.query(func.distinct(DagModel.owners).label('item')).filter(
            ~DagModel.is_subdag, DagModel.is_active,
            DagModel.owners.ilike('%' + query + '%'))  # noqa pylint: disable=no-member

        # Hide DAGs if not showing status: "all"
        status = flask_session.get(FILTER_STATUS_COOKIE)
        if status == 'active':
            dag_ids_query = dag_ids_query.filter(~DagModel.is_paused)
            owners_query = owners_query.filter(~DagModel.is_paused)
        elif status == 'paused':
            dag_ids_query = dag_ids_query.filter(DagModel.is_paused)
            owners_query = owners_query.filter(DagModel.is_paused)

        filter_dag_ids = current_app.appbuilder.sm.get_accessible_dag_ids()
        # pylint: disable=no-member
        if 'all_dags' not in filter_dag_ids:
            dag_ids_query = dag_ids_query.filter(DagModel.dag_id.in_(filter_dag_ids))
            owners_query = owners_query.filter(DagModel.dag_id.in_(filter_dag_ids))
        # pylint: enable=no-member

        payload = [row[0] for row in dag_ids_query.union(owners_query).limit(10).all()]

        return wwwutils.json_response(payload)

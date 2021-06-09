# -*- coding: utf-8 -*-
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

import uuid
import copy
import itertools
import json
import logging
import math
import os
import socket
import traceback
from flask_login import current_user
from collections import defaultdict
from datetime import timedelta
from urllib.parse import unquote
from airflow.settings import TIMEZONE
from datetime import datetime

import six
from six.moves.urllib.parse import quote
import pprint
import markdown
import pendulum
import sqlalchemy as sqla
from flask import (
    Markup, Response, escape, flash, jsonify, make_response, redirect, render_template, request,
    session as flask_session, url_for,
)
from flask._compat import PY2
from flask_appbuilder import BaseView, ModelView, expose, has_access, permission_name
from flask_appbuilder.baseviews import BaseCRUDView
from flask_appbuilder.urltools import get_filter_args, get_page_args
from flask_appbuilder.actions import action
from flask_appbuilder.models.sqla.filters import BaseFilter, get_field_setup_query
from flask_babel import lazy_gettext
import lazy_object_proxy
from jinja2.utils import htmlsafe_json_dumps  # type: ignore
from pygments import highlight, lexers
from pygments.formatters import HtmlFormatter
from sqlalchemy import and_, desc, func, or_, union_all
from sqlalchemy.orm import joinedload
from wtforms import SelectField, validators

import airflow
from airflow import models, jobs
from airflow import settings, configuration
from airflow.configuration import conf
from airflow.api.common.experimental.mark_tasks import (set_dag_run_state_to_success,
                                                        set_dag_run_state_to_failed)
from airflow.models import Variable, Connection, DagModel, DagRun, DagTag, Log, SlaMiss, TaskFail, XCom, errors, \
    TaskInstance
from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.models.dagcode import DagCode
from airflow.models.error_tag import ErrorTag
from airflow.models.tightening_controller import TighteningController
from airflow.settings import STORE_SERIALIZED_DAGS
from airflow.ti_deps.dep_context import RUNNING_DEPS, SCHEDULER_QUEUED_DEPS, DepContext
from airflow.utils import timezone
from airflow.utils.dates import infer_time_unit, scale_time_units
from airflow.utils.db import provide_session
from airflow.utils.helpers import alchemy_to_dict, render_log_filename
from airflow.utils.state import State
from airflow._vendor import nvd3
from airflow.www_rbac import utils as wwwutils
from airflow.www_rbac.app import app, appbuilder
from airflow.www_rbac.decorators import action_logging, gzipped, has_dag_access
from airflow.www_rbac.forms import (DateTimeForm, DateTimeWithNumRunsForm,
                                    DateTimeWithNumRunsWithDagRunsForm, VariableForm,
                                    DagRunForm, ConnectionForm, ErrorTagForm, TighteningControllerForm)
from airflow.www_rbac.widgets import AirflowModelListWidget, AirflowControllerListWidget
from flask_wtf.csrf import CSRFProtect
from airflow.utils.curve import get_curve, get_result, get_task_instances_by_entity_ids
from airflow.api.common.experimental.get_task_instance import get_task_instance
from airflow.www_rbac.api.experimental.endpoints import do_remove_curve_from_curve_template
from flask_appbuilder.models.sqla.filters import FilterEqualFunction, FilterInFunction
from airflow.utils.log.custom_log import CUSTOM_LOG_FORMAT, CUSTOM_EVENT_NAME_MAP, CUSTOM_PAGE_NAME_MAP
import logging
import os

FACTORY_CODE = os.getenv('FACTORY_CODE', 'DEFAULT_FACTORY_CODE')

_logger = logging.getLogger(__name__)
csrf = CSRFProtect()

PAGE_SIZE = conf.getint('webserver', 'page_size')
FILTER_TAGS_COOKIE = 'tags_filter'
if os.environ.get('SKIP_DAGS_PARSING') != 'True':
    dagbag = models.DagBag(settings.DAGS_FOLDER, store_serialized_dags=STORE_SERIALIZED_DAGS)
else:
    dagbag = models.DagBag(os.devnull, include_examples=False)


def get_date_time_num_runs_dag_runs_form_data(request, session, dag):
    dttm = request.args.get('execution_date')
    if dttm:
        dttm = pendulum.parse(dttm)
    else:
        dttm = dag.latest_execution_date or timezone.utcnow()

    base_date = request.args.get('base_date')
    if base_date:
        base_date = timezone.parse(base_date)
    else:
        # The DateTimeField widget truncates milliseconds and would loose
        # the first dag run. Round to next second.
        base_date = (dttm + timedelta(seconds=1)).replace(microsecond=0)

    default_dag_run = conf.getint('webserver', 'default_dag_run_display_number')
    num_runs = request.args.get('num_runs')
    num_runs = int(num_runs) if num_runs else default_dag_run

    DR = models.DagRun
    drs = (
        session.query(DR)
            .filter(
            DR.dag_id == dag.dag_id,
            DR.execution_date <= base_date)
            .order_by(desc(DR.execution_date))
            .limit(num_runs)
            .all()
    )
    dr_choices = []
    dr_state = None
    for dr in drs:
        dr_choices.append((dr.execution_date.isoformat(), dr.run_id))
        if dttm == dr.execution_date:
            dr_state = dr.state

    # Happens if base_date was changed and the selected dag run is not in result
    if not dr_state and drs:
        dr = drs[0]
        dttm = dr.execution_date
        dr_state = dr.state

    return {
        'dttm': dttm,
        'base_date': base_date,
        'num_runs': num_runs,
        'execution_date': dttm.isoformat(),
        'dr_choices': dr_choices,
        'dr_state': dr_state,
    }


######################################################################################
#                                    BaseViews
######################################################################################
@app.errorhandler(404)
def circles(error):
    return render_template(
        'airflow/circles.html', hostname=socket.getfqdn() if conf.getboolean(
            'webserver',
            'EXPOSE_HOSTNAME',
            fallback=True) else 'redact'), 404


@app.errorhandler(500)
def show_traceback(error):
    from airflow.utils import asciiart as ascii_
    return render_template(
        'airflow/traceback.html',
        hostname=socket.getfqdn() if conf.getboolean(
            'webserver',
            'EXPOSE_HOSTNAME',
            fallback=True) else 'redact',
        nukular=ascii_.nukular,
        info=traceback.format_exc() if conf.getboolean(
            'webserver',
            'EXPOSE_STACKTRACE',
            fallback=True) else 'Error! Please contact server admin'), 500


class AirflowBaseView(BaseView):
    route_base = ''

    # Make our macros available to our UI templates too.
    extra_args = {
        'macros': airflow.macros,
    }

    def render_template(self, *args, **kwargs):
        return super(AirflowBaseView, self).render_template(
            *args,
            # Cache this at most once per request, not for the lifetime of the view instanc
            scheduler_job=lazy_object_proxy.Proxy(jobs.SchedulerJob.most_recent_job),
            **kwargs
        )


class Airflow(AirflowBaseView):
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
            scheduler_job = jobs.SchedulerJob.most_recent_job()

            if scheduler_job:
                latest_scheduler_heartbeat = scheduler_job.latest_heartbeat.isoformat()
                if scheduler_job.is_alive():
                    scheduler_status = 'healthy'
        except Exception:
            payload['metadatabase']['status'] = 'unhealthy'

        payload['scheduler'] = {'status': scheduler_status,
                                'latest_scheduler_heartbeat': latest_scheduler_heartbeat}

        return wwwutils.json_response(payload)

    @expose('/home')
    @has_access
    @provide_session
    def index(self, session=None):
        DM = models.DagModel

        hide_paused_dags_by_default = conf.getboolean('webserver',
                                                      'hide_paused_dags_by_default')
        show_paused_arg = request.args.get('showPaused', 'None')

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

        if request.args.get('reset_tags') is not None:
            flask_session[FILTER_TAGS_COOKIE] = None
            arg_tags_filter = None
        else:
            cookie_val = flask_session.get(FILTER_TAGS_COOKIE)
            if arg_tags_filter:
                flask_session[FILTER_TAGS_COOKIE] = ','.join(arg_tags_filter)
            elif cookie_val:
                arg_tags_filter = cookie_val.split(',')

        dags_per_page = PAGE_SIZE
        current_page = get_int_arg(arg_current_page, default=0)

        if show_paused_arg.strip().lower() == 'false':
            hide_paused = True
        elif show_paused_arg.strip().lower() == 'true':
            hide_paused = False
        else:
            hide_paused = hide_paused_dags_by_default

        # read orm_dags from the db
        query = session.query(DM).filter(
            ~DM.is_subdag, DM.is_active
        )

        # optionally filter out "paused" dags
        if hide_paused:
            query = query.filter(~DM.is_paused)

        if arg_search_query:
            query = query.filter(
                DagModel.dag_id.ilike('%' + arg_search_query + '%') |
                DagModel.owners.ilike('%' + arg_search_query + '%')
            )

        import_errors = session.query(errors.ImportError).all()
        for ie in import_errors:
            flash(
                "Broken DAG: [{ie.filename}] {ie.stacktrace}".format(ie=ie),
                "dag_import_error")

        from airflow.plugins_manager import import_errors as plugin_import_errors
        for filename, stacktrace in plugin_import_errors.items():
            flash(
                "Broken plugin: [{filename}] {stacktrace}".format(
                    stacktrace=stacktrace,
                    filename=filename),
                "error")

        # Get all the dag id the user could access
        filter_dag_ids = appbuilder.sm.get_accessible_dag_ids()

        if arg_tags_filter:
            query = query.filter(DagModel.tags.any(DagTag.name.in_(arg_tags_filter)))

        if 'all_dags' not in filter_dag_ids:
            query = query.filter(DM.dag_id.in_(filter_dag_ids))

        start = current_page * dags_per_page
        end = start + dags_per_page

        dags = query.order_by(DagModel.dag_id).options(
            joinedload(DagModel.tags)).offset(start).limit(dags_per_page).all()
        tags = []

        dagtags = session.query(DagTag.name).distinct(DagTag.name).all()
        tags = [
            {"name": name, "selected": bool(arg_tags_filter and name in arg_tags_filter)}
            for name, in dagtags
        ]

        num_of_all_dags = query.count()
        num_of_pages = int(math.ceil(num_of_all_dags / float(dags_per_page)))

        return self.render_template(
            'airflow/dags.html',
            dags=dags,
            hide_paused=hide_paused,
            current_page=current_page,
            search_query=arg_search_query if arg_search_query else '',
            page_size=dags_per_page,
            num_of_pages=num_of_pages,
            num_dag_from=min(start + 1, num_of_all_dags),
            num_dag_to=min(end, num_of_all_dags),
            num_of_all_dags=num_of_all_dags,
            paging=wwwutils.generate_pages(current_page, num_of_pages,
                                           search=arg_search_query,
                                           showPaused=not hide_paused),
            num_runs=num_runs,
            tags=tags)

    @expose('/dag_stats', methods=['POST'])
    @has_access
    @provide_session
    def dag_stats(self, session=None):
        dr = models.DagRun

        allowed_dag_ids = appbuilder.sm.get_accessible_dag_ids()

        if 'all_dags' in allowed_dag_ids:
            allowed_dag_ids = [dag_id for dag_id, in session.query(models.DagModel.dag_id)]

        dag_state_stats = session.query(dr.dag_id, dr.state, sqla.func.count(dr.state)) \
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
                payload[dag_id].append({
                    'state': state,
                    'count': count,
                    'dag_id': dag_id,
                    'color': State.color(state)
                })

        return wwwutils.json_response(payload)

    # /view_curve/curve_anay/dag_runs/2020-04-07 10:04:48.786247 +00:00/tasks/trigger_anay_task
    @expose('/view_curve/<string:dag_id>/dag_runs/<string:execution_date>/tasks/<string:task_id>')
    @has_access
    def view_curve_page(self, dag_id, execution_date, task_id):
        _has_access = self.appbuilder.sm.has_access
        execution_date = timezone.parse(execution_date)
        ti = get_task_instance(dag_id, task_id, execution_date)
        if not ti.entity_id:
            return self.render_template('airflow/curve.html', task_instance=ti)
        try:
            result = get_result(ti.entity_id)
        except Exception as e:
            logging.error(e)
            result = {}
        try:
            curve = get_curve(ti.entity_id)
        except Exception as e:
            logging.error(e)
            curve = {}

        analysis_error_message_mapping = Variable.get('analysis_error_message_mapping', deserialize_json=True,
                                                      default_var={})

        verify_error_map = Variable.get('verify_error_map', deserialize_json=True,
                                        default_var={})

        result_error_message_mapping = Variable.get('result_error_message_mapping', deserialize_json=True,
                                                    default_var={})
        result_keys_translation_mapping = Variable.get('result_keys_translation_mapping', deserialize_json=True,
                                                       default_var={})
        controller_name = ti.controller_name.split('@')[0] if ti.controller_name else ''
        controller = TighteningController.find_controller(controller_name)
        error_tags = ErrorTag.get_all()
        ENV_CURVE_GRAPH_SHOW_RANGE = os.environ.get('CURVE_GRAPH_SHOW_RANGE')
        show_range = (ENV_CURVE_GRAPH_SHOW_RANGE == True) or (ENV_CURVE_GRAPH_SHOW_RANGE == 'True')
        can_verify = _has_access('set_final_state_ok', 'TaskInstanceModelView') \
                     and _has_access('set_final_state_nok', 'TaskInstanceModelView')
        display_keys = Variable.get('view_curve_page_keys', deserialize_json=True,
                                    default_var={})

        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['VIEW'], CUSTOM_PAGE_NAME_MAP['CURVE'], '查看单条曲线')
        logging.info(msg)

        return self.render_template('airflow/curve.html', task_instance=ti, result=result,
                                    curve=curve, analysisErrorMessageMapping=analysis_error_message_mapping,
                                    resultErrorMessageMapping=result_error_message_mapping,
                                    resultKeysTranslationMapping=result_keys_translation_mapping,
                                    verify_error_map=verify_error_map,
                                    can_verify=can_verify,
                                    controller=controller,
                                    errorTags=error_tags,
                                    show_range=show_range,
                                    display_keys=display_keys
                                    )

    @expose('/curve_template/<string:bolt_no>/<string:craft_type>')
    @has_access
    def view_curve_template(self, bolt_no, craft_type):
        curve_template = Variable.get_fuzzy_active('{}/{}'.format(bolt_no, craft_type),
                                                   deserialize_json=True,
                                                   default_var=None
                                                   )[1]
        _has_access = self.appbuilder.sm.has_access
        can_delete = _has_access('can_edit', 'VariableModelView') and _has_access('can_remove_curve_template',
                                                                                  'Airflow')

        if curve_template is None:
            return None

        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['VIEW'], CUSTOM_PAGE_NAME_MAP['CURVE_TEMPLATE'],
                                       '查看曲线模板页面')
        logging.info(msg)

        return self.render_template('airflow/curve_template.html', can_delete=can_delete,
                                    curve_template=curve_template, bolt_no=bolt_no,
                                    craft_type=craft_type)

    @expose('/curve_template/<string:bolt_no>/<string:craft_type>/remove_curve', methods=['PUT'])
    @has_access
    def remove_curve_template(self, bolt_no, craft_type):
        _has_access = self.appbuilder.sm.has_access
        can_delete = _has_access('can_edit', 'VariableModelView')
        if not can_delete:
            return {'error': u'没有权限'}

        params = request.get_json(force=True)
        version = params.get('version', None)
        mode = params.get('mode', None)
        group_center_idx = params.get('group_center_idx', None)
        curve_idx = params.get('curve_idx', None)
        try:
            new_template = do_remove_curve_from_curve_template(bolt_no, craft_type, version, mode, group_center_idx,
                                                               curve_idx)
            msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                           current_user, getattr(current_user, 'last_name', ''),
                                           CUSTOM_EVENT_NAME_MAP['DELETE'],
                                           CUSTOM_PAGE_NAME_MAP['CURVE_TEMPLATE'], '删除曲线模板')
            logging.info(msg)
            return {'data': new_template}
        except Exception as e:
            return {'error': str(e)}

    @expose('/task_stats', methods=['POST'])
    @has_access
    @provide_session
    def task_stats(self, session=None):
        TI = models.TaskInstance
        DagRun = models.DagRun
        Dag = models.DagModel
        allowed_dag_ids = set(appbuilder.sm.get_accessible_dag_ids())

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

        LastDagRun = (
            session.query(
                DagRun.dag_id,
                sqla.func.max(DagRun.execution_date).label('execution_date'))
                .join(Dag, Dag.dag_id == DagRun.dag_id)
                .filter(DagRun.state != State.RUNNING)
                .filter(Dag.is_active == True)  # noqa
                .group_by(DagRun.dag_id)
        )

        RunningDagRun = (
            session.query(DagRun.dag_id, DagRun.execution_date)
                .join(Dag, Dag.dag_id == DagRun.dag_id)
                .filter(DagRun.state == State.RUNNING)
                .filter(Dag.is_active == True)  # noqa
        )

        if selected_dag_ids:
            LastDagRun = LastDagRun.filter(DagRun.dag_id.in_(filter_dag_ids))
            RunningDagRun = RunningDagRun.filter(DagRun.dag_id.in_(filter_dag_ids))

        LastDagRun = LastDagRun.subquery('last_dag_run')
        RunningDagRun = RunningDagRun.subquery('running_dag_run')

        # Select all task_instances from active dag_runs.
        # If no dag_run is active, return task instances from most recent dag_run.
        LastTI = (
            session.query(TI.dag_id.label('dag_id'), TI.state.label('state'))
                .join(LastDagRun,
                      and_(LastDagRun.c.dag_id == TI.dag_id,
                           LastDagRun.c.execution_date == TI.execution_date))
        )
        RunningTI = (
            session.query(TI.dag_id.label('dag_id'), TI.state.label('state'))
                .join(RunningDagRun,
                      and_(RunningDagRun.c.dag_id == TI.dag_id,
                           RunningDagRun.c.execution_date == TI.execution_date))
        )

        if selected_dag_ids:
            LastTI = LastTI.filter(TI.dag_id.in_(filter_dag_ids))
            RunningTI = RunningTI.filter(TI.dag_id.in_(filter_dag_ids))

        UnionTI = union_all(LastTI, RunningTI).alias('union_ti')

        qry = (
            session.query(UnionTI.c.dag_id, UnionTI.c.state, sqla.func.count())
                .group_by(UnionTI.c.dag_id, UnionTI.c.state)
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
                    'count': count,
                    'dag_id': dag_id,
                    'color': State.color(state)
                })
        return wwwutils.json_response(payload)

    @expose('/last_dagruns', methods=['POST'])
    @has_access
    @provide_session
    def last_dagruns(self, session=None):
        DagRun = models.DagRun

        allowed_dag_ids = appbuilder.sm.get_accessible_dag_ids()

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
        query = query.filter(DagRun.dag_id.in_(filter_dag_ids))

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
        all_errors = ""

        try:
            dag_id = request.args.get('dag_id')
            dag_orm = DagModel.get_dagmodel(dag_id, session=session)
            code = DagCode.get_code_by_fileloc(dag_orm.fileloc)
            html_code = highlight(
                code, lexers.PythonLexer(), HtmlFormatter(linenos=True))

        except Exception as e:
            all_errors += (
                "Exception encountered during " +
                "dag_id retrieval/dag retrieval fallback/code highlighting:\n\n{}\n".format(e)
            )
            html_code = '<p>Failed to load file.</p><p>Details: {}</p>'.format(
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
        dag_id = request.args.get('dag_id')
        dag_orm = DagModel.get_dagmodel(dag_id, session=session)
        # FIXME: items needed for this view should move to the database
        dag = dag_orm.get_dag(STORE_SERIALIZED_DAGS)
        title = "DAG details"
        root = request.args.get('root', '')

        TI = models.TaskInstance
        states = (
            session.query(TI.state, sqla.func.count(TI.dag_id))
                .filter(TI.dag_id == dag_id)
                .group_by(TI.state)
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

    @expose('/pickle_info')
    @has_access
    def pickle_info(self):
        d = {}
        filter_dag_ids = appbuilder.sm.get_accessible_dag_ids()
        if not filter_dag_ids:
            return wwwutils.json_response({})
        dag_id = request.args.get('dag_id')
        dags = [dagbag.dags.get(dag_id)] if dag_id else dagbag.dags.values()
        for dag in dags:
            if 'all_dags' in filter_dag_ids or dag.dag_id in filter_dag_ids:
                if not dag.is_subdag:
                    d[dag.dag_id] = dag.pickle_info()
        return wwwutils.json_response(d)

    @expose('/rendered')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    @provide_session
    def rendered(self, session=None):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        dttm = pendulum.parse(execution_date)
        form = DateTimeForm(data={'execution_date': dttm})
        root = request.args.get('root', '')

        logging.info("Retrieving rendered templates.")
        dag = dagbag.get_dag(dag_id)

        task = copy.copy(dag.get_task(task_id))
        ti = models.TaskInstance(task=task, execution_date=dttm)
        try:
            ti.get_rendered_template_fields()
        except AirflowException as e:
            msg = "Error rendering template: " + escape(e)
            if not PY2:
                if e.__cause__:
                    msg += Markup("<br/><br/>OriginalError: ") + escape(e.__cause__)
            flash(msg, "error")
        except Exception as e:
            flash("Error rendering template: " + str(e), "error")
        title = "Rendered Template"
        html_dict = {}
        for template_field in task.template_fields:
            content = getattr(task, template_field)
            if template_field in wwwutils.get_attr_renderer():
                html_dict[template_field] = \
                    wwwutils.get_attr_renderer()[template_field](content)
            else:
                html_dict[template_field] = (
                    "<pre><code>" + str(content) + "</pre></code>")

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
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        dttm = pendulum.parse(execution_date)
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

        logger = logging.getLogger('airflow.task')
        task_log_reader = conf.get('core', 'task_log_reader')
        handler = next((handler for handler in logger.handlers
                        if handler.name == task_log_reader), None)

        ti = session.query(models.TaskInstance).filter(
            models.TaskInstance.dag_id == dag_id,
            models.TaskInstance.task_id == task_id,
            models.TaskInstance.execution_date == dttm).first()

        def _get_logs_with_metadata(try_number, metadata):
            if ti is None:
                logs = ["*** Task instance did not exist in the DB\n"]
                metadata['end_of_log'] = True
            else:
                logs, metadatas = handler.read(ti, try_number, metadata=metadata)
                metadata = metadatas[0]
            return logs, metadata

        try:
            if ti is not None:
                dag = dagbag.get_dag(dag_id)
                ti.task = dag.get_task(ti.task_id)
            if response_format == 'json':
                logs, metadata = _get_logs_with_metadata(try_number, metadata)
                message = logs[0] if try_number is not None else logs
                return jsonify(message=message, metadata=metadata)

            filename_template = conf.get('core', 'LOG_FILENAME_TEMPLATE')
            attachment_filename = render_log_filename(
                ti=ti,
                try_number="all" if try_number is None else try_number,
                filename_template=filename_template)
            metadata['download_logs'] = True

            def _generate_log_stream(try_number, metadata):
                if try_number is None and ti is not None:
                    next_try = ti.next_try_number
                    try_numbers = list(range(1, next_try))
                else:
                    try_numbers = [try_number]
                for try_number in try_numbers:
                    metadata.pop('end_of_log', None)
                    metadata.pop('max_offset', None)
                    metadata.pop('offset', None)
                    while 'end_of_log' not in metadata or not metadata['end_of_log']:
                        logs, metadata = _get_logs_with_metadata(try_number, metadata)
                        yield "\n".join(logs) + "\n"

            return Response(_generate_log_stream(try_number, metadata),
                            mimetype="text/plain",
                            headers={"Content-Disposition": "attachment; filename={}".format(
                                attachment_filename)})
        except AttributeError as e:
            error_message = ["Task log handler {} does not support read logs.\n{}\n"
                                 .format(task_log_reader, str(e))]
            metadata['end_of_log'] = True
            return jsonify(message=error_message, error=True, metadata=metadata)

    @expose('/log')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    @provide_session
    def log(self, session=None):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        dttm = pendulum.parse(execution_date)
        form = DateTimeForm(data={'execution_date': dttm})
        dag = dagbag.get_dag(dag_id)

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
            logs=logs, dag=dag, title="Log by attempts",
            dag_id=dag.dag_id, task_id=task_id,
            execution_date=execution_date, form=form,
            root=root, wrapped=conf.getboolean('webserver', 'default_wrap'))

    @expose('/elasticsearch')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    @provide_session
    def elasticsearch(self, session=None):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        try_number = request.args.get('try_number', 1)
        elasticsearch_frontend = conf.get('elasticsearch', 'frontend')
        log_id_template = conf.get('elasticsearch', 'log_id_template')
        log_id = log_id_template.format(
            dag_id=dag_id, task_id=task_id,
            execution_date=execution_date, try_number=try_number)
        url = 'https://' + elasticsearch_frontend.format(log_id=quote(log_id))
        return redirect(url)

    @expose('/task')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    def task(self):
        TI = models.TaskInstance

        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        # Carrying execution_date through, even though it's irrelevant for
        # this context
        execution_date = request.args.get('execution_date')
        dttm = pendulum.parse(execution_date)
        form = DateTimeForm(data={'execution_date': dttm})
        root = request.args.get('root', '')
        dag = dagbag.get_dag(dag_id)

        if not dag or task_id not in dag.task_ids:
            flash(
                "Task [{}.{}] doesn't seem to exist"
                " at the moment".format(dag_id, task_id),
                "error")
            return redirect(url_for('Airflow.index'))
        task = copy.copy(dag.get_task(task_id))
        task.resolve_template_files()
        ti = TI(task=task, execution_date=dttm)
        ti.refresh_from_db()

        ti_attrs = []
        for attr_name in dir(ti):
            if not attr_name.startswith('_'):
                attr = getattr(ti, attr_name)
                if type(attr) != type(self.task):  # noqa
                    ti_attrs.append((attr_name, str(attr)))

        task_attrs = []
        for attr_name in dir(task):
            if not attr_name.startswith('_'):
                attr = getattr(task, attr_name)
                if type(attr) != type(self.task) and \
                    attr_name not in wwwutils.get_attr_renderer():  # noqa
                    task_attrs.append((attr_name, str(attr)))

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
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        # Carrying execution_date through, even though it's irrelevant for
        # this context
        execution_date = request.args.get('execution_date')
        dttm = pendulum.parse(execution_date)
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
        dag_id = request.form.get('dag_id')
        task_id = request.form.get('task_id')
        origin = request.form.get('origin')
        dag = dagbag.get_dag(dag_id)
        task = dag.get_task(task_id)

        execution_date = request.form.get('execution_date')
        execution_date = pendulum.parse(execution_date)
        ignore_all_deps = request.form.get('ignore_all_deps') == "true"
        ignore_task_deps = request.form.get('ignore_task_deps') == "true"
        ignore_ti_state = request.form.get('ignore_ti_state') == "true"

        from airflow.executors import get_default_executor
        executor = get_default_executor()
        valid_celery_config = False
        valid_kubernetes_config = False

        try:
            from airflow.executors.celery_executor import CeleryExecutor
            valid_celery_config = isinstance(executor, CeleryExecutor)
        except ImportError:
            pass

        try:
            from airflow.contrib.executors.kubernetes_executor import KubernetesExecutor
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
        from airflow.api.common.experimental import delete_dag
        from airflow.exceptions import DagNotFound, DagFileExists

        dag_id = request.values.get('dag_id')
        origin = request.values.get('origin') or url_for('Airflow.index')

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

        dag_id = request.values.get('dag_id')
        origin = request.values.get('origin') or url_for('Airflow.index')

        if request.method == 'GET':
            return self.render_template(
                'airflow/trigger.html',
                dag_id=dag_id,
                origin=origin,
                conf=''
            )

        dag = session.query(models.DagModel).filter(models.DagModel.dag_id == dag_id).first()
        if not dag:
            flash("Cannot find dag {}".format(dag_id))
            return redirect(origin)

        execution_date = timezone.utcnow()
        run_id = "manual__{0}".format(execution_date.isoformat())

        dr = DagRun.find(dag_id=dag_id, run_id=run_id)
        if dr:
            flash("This run_id {} already exists".format(run_id))
            return redirect(origin)

        run_conf = {}
        conf = request.values.get('conf')
        if conf:
            try:
                run_conf = json.loads(conf)
            except ValueError:
                flash("Invalid JSON configuration", "error")
                return self.render_template(
                    'airflow/trigger.html',
                    dag_id=dag_id,
                    origin=origin,
                    conf=conf
                )

        dag.create_dagrun(
            run_id=run_id,
            execution_date=execution_date,
            state=State.RUNNING,
            conf=run_conf,
            external_trigger=True
        )

        flash(
            "Triggered {}, "
            "it should start any moment now.".format(dag_id))
        return redirect(origin)

    def _clear_dag_tis(self, dag, start_date, end_date, origin,
                       recursive=False, confirmed=False, only_failed=False):
        from airflow.exceptions import AirflowException

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
        dag_id = request.form.get('dag_id')
        task_id = request.form.get('task_id')
        origin = request.form.get('origin')
        dag = dagbag.get_dag(dag_id)

        execution_date = request.form.get('execution_date')
        execution_date = pendulum.parse(execution_date)
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
        dag_id = request.form.get('dag_id')
        origin = request.form.get('origin')
        execution_date = request.form.get('execution_date')
        confirmed = request.form.get('confirmed') == "true"

        dag = dagbag.get_dag(dag_id)
        execution_date = pendulum.parse(execution_date)
        start_date = execution_date
        end_date = execution_date

        return self._clear_dag_tis(dag, start_date, end_date, origin,
                                   recursive=True, confirmed=confirmed)

    @expose('/blocked', methods=['POST'])
    @has_access
    @provide_session
    def blocked(self, session=None):
        allowed_dag_ids = appbuilder.sm.get_accessible_dag_ids()

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

        DR = models.DagRun

        dags = (
            session.query(DR.dag_id, sqla.func.count(DR.id))
                .filter(DR.state == State.RUNNING)
                .filter(DR.dag_id.in_(filter_dag_ids))
                .group_by(DR.dag_id)
        )

        payload = []
        for dag_id, active_dag_runs in dags:
            max_active_runs = 0
            dag = dagbag.get_dag(dag_id)
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

        execution_date = pendulum.parse(execution_date)
        dag = dagbag.get_dag(dag_id)

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
                message=("Here's the list of task instances you are about to mark as failed"),
                details=details)

            return response

    def _mark_dagrun_state_as_success(self, dag_id, execution_date, confirmed, origin):
        if not execution_date:
            flash('Invalid execution date', 'error')
            return redirect(origin)

        execution_date = pendulum.parse(execution_date)
        dag = dagbag.get_dag(dag_id)

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
                message=("Here's the list of task instances you are about to mark as success"),
                details=details)

            return response

    @expose('/dagrun_failed', methods=['POST'])
    @has_dag_access(can_dag_edit=True)
    @has_access
    @action_logging
    def dagrun_failed(self):
        dag_id = request.form.get('dag_id')
        execution_date = request.form.get('execution_date')
        confirmed = request.form.get('confirmed') == 'true'
        origin = request.form.get('origin')
        return self._mark_dagrun_state_as_failed(dag_id, execution_date,
                                                 confirmed, origin)

    @expose('/dagrun_success', methods=['POST'])
    @has_dag_access(can_dag_edit=True)
    @has_access
    @action_logging
    def dagrun_success(self):
        dag_id = request.form.get('dag_id')
        execution_date = request.form.get('execution_date')
        confirmed = request.form.get('confirmed') == 'true'
        origin = request.form.get('origin')
        return self._mark_dagrun_state_as_success(dag_id, execution_date,
                                                  confirmed, origin)

    def _mark_task_instance_state(self, dag_id, task_id, origin, execution_date,
                                  confirmed, upstream, downstream,
                                  future, past, state):
        dag = dagbag.get_dag(dag_id)
        task = dag.get_task(task_id)
        task.dag = dag

        execution_date = pendulum.parse(execution_date)

        if not dag:
            flash("Cannot find DAG: {}".format(dag_id))
            return redirect(origin)

        if not task:
            flash("Cannot find task {} in DAG {}".format(task_id, dag.dag_id))
            return redirect(origin)

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
        dag_id = request.form.get('dag_id')
        task_id = request.form.get('task_id')
        origin = request.form.get('origin')
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
        dag_id = request.form.get('dag_id')
        task_id = request.form.get('task_id')
        origin = request.form.get('origin')
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
    @provide_session
    def tree(self, session=None):
        dag_id = request.args.get('dag_id')
        blur = conf.getboolean('webserver', 'demo_mode')
        dag = dagbag.get_dag(dag_id)
        if not dag:
            flash('DAG "{0}" seems to be missing.'.format(dag_id), "error")
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
            base_date = dag.latest_execution_date or timezone.utcnow()

        DR = models.DagRun
        dag_runs = (
            session.query(DR)
                .filter(
                DR.dag_id == dag.dag_id,
                DR.execution_date <= base_date)
                .order_by(DR.execution_date.desc())
                .limit(num_runs)
                .all()
        )
        dag_runs = {
            dr.execution_date: alchemy_to_dict(dr) for dr in dag_runs
        }

        dates = sorted(list(dag_runs.keys()))
        max_date = max(dates) if dates else None
        min_date = min(dates) if dates else None

        tis = dag.get_task_instances(
            start_date=min_date, end_date=base_date, session=session)
        task_instances = {}
        for ti in tis:
            task_instances[(ti.task_id, ti.execution_date)] = ti

        expanded = set()
        # The default recursion traces every path so that tree view has full
        # expand/collapse functionality. After 5,000 nodes we stop and fall
        # back on a quick DFS search for performance. See PR #320.
        node_count = [0]
        node_limit = 5000 / max(1, len(dag.leaves))

        def encode_ti(ti):
            if not ti:
                return None

            # NOTE: order of entry is important here because client JS relies on it for
            # tree node reconstruction. Remember to change JS code in tree.html
            # whenever order is altered.
            data = [
                ti.state,
                ti.try_number,
                None,  # start_ts
                None,  # duration
            ]

            if ti.start_date:
                # round to seconds to reduce payload size
                if six.PY2:
                    data[2] = int(pendulum.instance(ti.start_date).timestamp())
                else:
                    data[2] = int(ti.start_date.timestamp())
                if ti.duration is not None:
                    data[3] = int(ti.duration)

            return data

        def recurse_nodes(task, visited):
            node_count[0] += 1
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
                    if node_count[0] < node_limit or t not in visited]

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
                if six.PY2:
                    node['start_ts'] = int(pendulum.instance(task.start_date).timestamp())
                else:
                    node['start_ts'] = int(task.start_date.timestamp())
                if task.end_date:
                    # round to seconds to reduce payload size
                    if six.PY2:
                        node['end_ts'] = int(pendulum.instance(task.end_date).timestamp())
                    else:
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
        external_logs = conf.get('elasticsearch', 'frontend')

        return self.render_template(
            'airflow/tree.html',
            operators=sorted({op.task_type: op for op in dag.tasks}.values(),
                             key=lambda x: x.task_type),
            root=root,
            form=form,
            dag=dag,
            # avoid spaces to reduce payload size
            data=htmlsafe_json_dumps(data, separators=(',', ':')),
            blur=blur, num_runs=num_runs,
            show_external_logs=bool(external_logs))

    @expose('/graph')
    @has_dag_access(can_dag_read=True)
    @has_access
    @gzipped
    @action_logging
    @provide_session
    def graph(self, session=None):
        dag_id = request.args.get('dag_id')
        blur = conf.getboolean('webserver', 'demo_mode')
        dag = dagbag.get_dag(dag_id)
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
        for task in dag.tasks:
            nodes.append({
                'id': task.task_id,
                'value': {
                    'label': task.task_id,
                    'labelStyle': "fill:{0};".format(task.ui_fgcolor),
                    'style': "fill:{0};".format(task.ui_color),
                    'rx': 5,
                    'ry': 5,
                }
            })

        def get_downstream(task):
            for t in task.downstream_list:
                edge = {
                    'source_id': task.task_id,
                    'target_id': t.task_id,
                }
                if edge not in edges:
                    edges.append(edge)
                    get_downstream(t)

        for t in dag.roots:
            get_downstream(t)

        dt_nr_dr_data = get_date_time_num_runs_dag_runs_form_data(request, session, dag)
        dt_nr_dr_data['arrange'] = arrange
        dttm = dt_nr_dr_data['dttm']

        class GraphForm(DateTimeWithNumRunsWithDagRunsForm):
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
            for ti in dag.get_task_instances(dttm, dttm, session=session)}
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
        doc_md = markdown.markdown(dag.doc_md) \
            if hasattr(dag, 'doc_md') and dag.doc_md else ''

        external_logs = conf.get('elasticsearch', 'frontend')
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
            operators=sorted({op.task_type: op for op in dag.tasks}.values(),
                             key=lambda x: x.task_type),
            blur=blur,
            root=root or '',
            task_instances=task_instances,
            tasks=tasks,
            nodes=nodes,
            edges=edges,
            show_external_logs=bool(external_logs))

    @expose('/duration')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    @provide_session
    def duration(self, session=None):
        default_dag_run = conf.getint('webserver', 'default_dag_run_display_number')
        dag_id = request.args.get('dag_id')
        dag = dagbag.get_dag(dag_id)
        base_date = request.args.get('base_date')
        num_runs = request.args.get('num_runs')
        num_runs = int(num_runs) if num_runs else default_dag_run

        if dag is None:
            flash('DAG "{0}" seems to be missing.'.format(dag_id), "error")
            return redirect(url_for('Airflow.index'))

        if base_date:
            base_date = pendulum.parse(base_date)
        else:
            base_date = dag.latest_execution_date or timezone.utcnow()

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

        y = defaultdict(list)
        x = defaultdict(list)
        cum_y = defaultdict(list)

        tis = dag.get_task_instances(
            start_date=min_date, end_date=base_date, session=session)
        TF = TaskFail
        ti_fails = (
            session.query(TF)
                .filter(TF.dag_id == dag.dag_id,  # noqa
                        TF.execution_date >= min_date,
                        TF.execution_date <= base_date,
                        TF.task_id.in_([t.task_id for t in dag.tasks]))
                .all()  # noqa
        )

        fails_totals = defaultdict(int)
        for tf in ti_fails:
            dict_key = (tf.dag_id, tf.task_id, tf.execution_date)
            if tf.duration:
                fails_totals[dict_key] += tf.duration

        for ti in tis:
            if ti.duration:
                dttm = wwwutils.epoch(ti.execution_date)
                x[ti.task_id].append(dttm)
                y[ti.task_id].append(float(ti.duration))
                fails_dict_key = (ti.dag_id, ti.task_id, ti.execution_date)
                fails_total = fails_totals[fails_dict_key]
                cum_y[ti.task_id].append(float(ti.duration + fails_total))

        # determine the most relevant time unit for the set of task instance
        # durations for the DAG
        y_unit = infer_time_unit([d for t in y.values() for d in t])
        cum_y_unit = infer_time_unit([d for t in cum_y.values() for d in t])
        # update the y Axis on both charts to have the correct time units
        chart.create_y_axis('yAxis', format='.02f', custom_format=False,
                            label='Duration ({})'.format(y_unit))
        chart.axislist['yAxis']['axisLabelDistance'] = '40'
        cum_chart.create_y_axis('yAxis', format='.02f', custom_format=False,
                                label='Duration ({})'.format(cum_y_unit))
        cum_chart.axislist['yAxis']['axisLabelDistance'] = '40'

        for task in dag.tasks:
            if x[task.task_id]:
                chart.add_serie(name=task.task_id, x=x[task.task_id],
                                y=scale_time_units(y[task.task_id], y_unit))
                cum_chart.add_serie(name=task.task_id, x=x[task.task_id],
                                    y=scale_time_units(cum_y[task.task_id],
                                                       cum_y_unit))

        dates = sorted(list({ti.execution_date for ti in tis}))
        max_date = max([ti.execution_date for ti in tis]) if dates else None

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
            chart=chart.htmlcontent,
            cum_chart=cum_chart.htmlcontent
        )

    @expose('/tries')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    @provide_session
    def tries(self, session=None):
        default_dag_run = conf.getint('webserver', 'default_dag_run_display_number')
        dag_id = request.args.get('dag_id')
        dag = dagbag.get_dag(dag_id)
        base_date = request.args.get('base_date')
        num_runs = request.args.get('num_runs')
        num_runs = int(num_runs) if num_runs else default_dag_run

        if base_date:
            base_date = pendulum.parse(base_date)
        else:
            base_date = dag.latest_execution_date or timezone.utcnow()

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
            y = []
            x = []
            for ti in task.get_task_instances(start_date=min_date,
                                              end_date=base_date,
                                              session=session):
                dttm = wwwutils.epoch(ti.execution_date)
                x.append(dttm)
                # y value should reflect completed tries to have a 0 baseline.
                y.append(ti.prev_attempted_tries)
            if x:
                chart.add_serie(name=task.task_id, x=x, y=y)

        tis = dag.get_task_instances(
            start_date=min_date, end_date=base_date, session=session)
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
            chart=chart.htmlcontent
        )

    @expose('/landing_times')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    @provide_session
    def landing_times(self, session=None):
        default_dag_run = conf.getint('webserver', 'default_dag_run_display_number')
        dag_id = request.args.get('dag_id')
        dag = dagbag.get_dag(dag_id)
        base_date = request.args.get('base_date')
        num_runs = request.args.get('num_runs')
        num_runs = int(num_runs) if num_runs else default_dag_run

        if base_date:
            base_date = pendulum.parse(base_date)
        else:
            base_date = dag.latest_execution_date or timezone.utcnow()

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
        y = {}
        x = {}
        for task in dag.tasks:
            task_id = task.task_id
            y[task_id] = []
            x[task_id] = []
            for ti in task.get_task_instances(start_date=min_date, end_date=base_date):
                ts = ti.execution_date
                if dag.schedule_interval and dag.following_schedule(ts):
                    ts = dag.following_schedule(ts)
                if ti.end_date:
                    dttm = wwwutils.epoch(ti.execution_date)
                    secs = (ti.end_date - ts).total_seconds()
                    x[task_id].append(dttm)
                    y[task_id].append(secs)

        # determine the most relevant time unit for the set of landing times
        # for the DAG
        y_unit = infer_time_unit([d for t in y.values() for d in t])
        # update the y Axis to have the correct time units
        chart.create_y_axis('yAxis', format='.02f', custom_format=False,
                            label='Landing Time ({})'.format(y_unit))
        chart.axislist['yAxis']['axisLabelDistance'] = '40'
        for task in dag.tasks:
            if x[task.task_id]:
                chart.add_serie(name=task.task_id, x=x[task.task_id],
                                y=scale_time_units(y[task.task_id], y_unit))

        tis = dag.get_task_instances(
            start_date=min_date, end_date=base_date, session=session)
        dates = sorted(list({ti.execution_date for ti in tis}))
        max_date = max([ti.execution_date for ti in tis]) if dates else None

        session.commit()

        form = DateTimeWithNumRunsForm(data={'base_date': max_date,
                                             'num_runs': num_runs})
        chart.buildcontent()
        return self.render_template(
            'airflow/chart.html',
            dag=dag,
            chart=chart.htmlcontent,
            height=str(chart_height + 100) + "px",
            demo_mode=conf.getboolean('webserver', 'demo_mode'),
            root=root,
            form=form,
        )

    @expose('/paused', methods=['POST'])
    @has_dag_access(can_dag_edit=True)
    @has_access
    @action_logging
    @provide_session
    def paused(self, session=None):
        dag_id = request.args.get('dag_id')
        is_paused = True if request.args.get('is_paused') == 'false' else False
        models.DagModel.get_dagmodel(dag_id).set_is_paused(
            is_paused=is_paused,
            store_serialized_dags=STORE_SERIALIZED_DAGS)
        return "OK"

    @expose('/refresh', methods=['POST'])
    @has_dag_access(can_dag_edit=True)
    @has_access
    @action_logging
    @provide_session
    def refresh(self, session=None):
        DagModel = models.DagModel
        dag_id = request.values.get('dag_id')
        orm_dag = session.query(
            DagModel).filter(DagModel.dag_id == dag_id).first()

        if orm_dag:
            orm_dag.last_expired = timezone.utcnow()
            session.merge(orm_dag)
        session.commit()

        dag = dagbag.get_dag(dag_id)
        # sync dag permission
        appbuilder.sm.sync_perm_for_dag(dag_id, dag.access_control)

        flash("DAG [{}] is now fresh as a daisy".format(dag_id))
        return redirect(request.referrer)

    @expose('/gantt')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    @provide_session
    def gantt(self, session=None):
        dag_id = request.args.get('dag_id')
        dag = dagbag.get_dag(dag_id)
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
            ti for ti in dag.get_task_instances(dttm, dttm, session=session)
            if ti.start_date and ti.state]
        tis = sorted(tis, key=lambda ti: ti.start_date)
        TF = TaskFail
        ti_fails = list(itertools.chain(*[(
            session
                .query(TF)
                .filter(TF.dag_id == ti.dag_id,
                        TF.task_id == ti.task_id,
                        TF.execution_date == ti.execution_date)
                .all()
        ) for ti in tis]))

        # determine bars to show in the gantt chart
        # all reschedules of one attempt are combinded into one bar
        gantt_bar_items = []
        for ti in tis:
            end_date = ti.end_date or timezone.utcnow()
            # prev_attempted_tries will reflect the currently running try_number
            # or the try_number of the last complete run
            # https://issues.apache.org/jira/browse/AIRFLOW-2143
            try_count = ti.prev_attempted_tries
            gantt_bar_items.append((ti.task_id, ti.start_date, end_date, ti.state, try_count))

        tf_count = 0
        try_count = 1
        prev_task_id = ""
        for tf in ti_fails:
            end_date = tf.end_date or timezone.utcnow()
            start_date = tf.start_date or end_date
            if tf_count != 0 and tf.task_id == prev_task_id:
                try_count = try_count + 1
            else:
                try_count = 1
            prev_task_id = tf.task_id
            gantt_bar_items.append((tf.task_id, start_date, end_date, State.FAILED, try_count))
            tf_count = tf_count + 1

        task_types = {}
        extra_links = {}
        for t in dag.tasks:
            task_types[t.task_id] = t.task_type
            extra_links[t.task_id] = t.extra_links

        tasks = []
        for gantt_bar_item in gantt_bar_items:
            task_id = gantt_bar_item[0]
            start_date = gantt_bar_item[1]
            end_date = gantt_bar_item[2]
            state = gantt_bar_item[3]
            try_count = gantt_bar_item[4]
            tasks.append({
                'startDate': wwwutils.epoch(start_date),
                'endDate': wwwutils.epoch(end_date),
                'isoStart': start_date.isoformat()[:-4],
                'isoEnd': end_date.isoformat()[:-4],
                'taskName': task_id,
                'taskType': task_types[ti.task_id],
                'duration': (end_date - start_date).total_seconds(),
                'status': state,
                'executionDate': dttm.isoformat(),
                'try_number': try_count,
                'extraLinks': extra_links[ti.task_id],
            })

        states = {task['status']: task['status'] for task in tasks}
        data = {
            'taskNames': [ti.task_id for ti in tis],
            'tasks': tasks,
            'taskStatus': states,
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
        dttm = airflow.utils.timezone.parse(execution_date)
        dag = dagbag.get_dag(dag_id)

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
    @provide_session
    def task_instances(self, session=None):
        dag_id = request.args.get('dag_id')
        dag = dagbag.get_dag(dag_id)

        dttm = request.args.get('execution_date')
        if dttm:
            dttm = pendulum.parse(dttm)
        else:
            return "Error: Invalid execution_date"

        task_instances = {
            ti.task_id: alchemy_to_dict(ti)
            for ti in dag.get_task_instances(dttm, dttm, session=session)}

        return json.dumps(task_instances)


class CurvesView(BaseCRUDView):
    list_template = "airflow/curves.html"
    CustomSQLAInterface = wwwutils.CustomSQLAInterface
    route_base = '/curves'
    datamodel = CustomSQLAInterface(models.TaskInstance)
    search_columns = ['execution_date', 'car_code', 'error_tag', 'measure_result', 'result', 'final_state']
    label_columns = {
        'error_tag': lazy_gettext('Error Tags'),
        'execution_date': lazy_gettext('Execution Date'), 'car_code': lazy_gettext('Car Code'),
        'measure_result': lazy_gettext('Measure Result'), 'result': lazy_gettext('Result'),
        'final_state': lazy_gettext('Final State')
    }

    def do_render(self, track_no=None, bolt_no=None, controller=None, craft_type=None):
        view_name = 'curves'
        curves = request.args.get('curves')
        curves_list = curves.replace('@', '/').split(',') if curves is not None else []
        _has_access = self.appbuilder.sm.has_access
        pages = get_page_args()
        page = pages.get(view_name, 0)
        get_filter_args(self._filters)
        if bolt_no:
            self._filters.add_filter(column_name='bolt_number', filter_class=self.datamodel.FilterEqual, value=bolt_no)
        if craft_type:
            self._filters.add_filter(column_name='craft_type', filter_class=self.datamodel.FilterEqual,
                                     value=int(craft_type))
        if track_no:
            self._filters.add_filter(column_name='car_code', filter_class=self.datamodel.FilterEqual, value=track_no)
        if controller:
            self._filters.add_filter(column_name='controller_name', filter_class=self.datamodel.FilterContains, value=controller)

        joined_filters = self._filters.get_joined_filters(self._base_filters)
        order_column, order_direction = "execution_date", "desc"
        page_size = PAGE_SIZE
        count, lst = self.datamodel.query(
            joined_filters,
            order_column,
            order_direction,
            page=page,
            page_size=page_size,
        )

        error_tag_vals = ErrorTag.get_all_dict() or {}
        for t in lst:
            ret = []
            try:
                error_tags = json.loads(t.error_tag or '[]')
                if not error_tags:
                    t.view_error_tags = u'无异常标签'
                    continue
                for tag in error_tags:
                    v = error_tag_vals.get(str(tag), '')
                    if not v:
                        continue
                    ret.append(v)
            except Exception as e:
                t.view_error_tags = ','.join(ret)
            t.view_error_tags = ','.join(ret)

        selected_tasks = {}
        tasks = list(get_task_instances_by_entity_ids(curves_list))
        for ti in tasks:
            selected_tasks[ti.entity_id] = {
                'carCode': ti.car_code,
                'value': ti.entity_id,
                'task': ti.task_id,
                'dag': ti.dag_id,
                'date': str(ti.execution_date)
            }
        widgets = self._list()

        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['VIEW'], CUSTOM_PAGE_NAME_MAP['CURVES'], '查看曲线对比页面')
        logging.info(msg)

        return self.render_template('airflow/curves.html', tasks=lst, page=page, page_size=page_size, count=count,
                                    modelview_name=view_name,
                                    selected_curves=curves_list,
                                    selected_tasks=selected_tasks,
                                    widgets=widgets)

    @expose('/analysis')
    @has_access
    def view_curves_analysis(self):
        track_no = request.args.get('track_no', default=None)
        bolt_no = request.args.get('bolt_no', default=None)
        controller = request.args.get('controller', default=None)
        analysis_type = request.args.get('analysis_type', default=None)
        ret = None
        if not analysis_type:
            raise AirflowNotFoundException
        if analysis_type == 'track_no' and track_no:
            ret = self.do_render(track_no=track_no)
        elif analysis_type == 'bolt_no' and bolt_no:
            ret = self.do_render(bolt_no=bolt_no)
        elif analysis_type == 'controller' and controller:
            ret = self.do_render(controller=controller)
        if not ret:
            raise AirflowNotFoundException
        return ret

    @expose('/<string:bolt_no>/<string:craft_type>')
    @has_access
    def view_curves(self, bolt_no, craft_type):
        ret = self.do_render(bolt_no=bolt_no, craft_type=craft_type)
        return ret


class VersionView(AirflowBaseView):
    default_view = 'version'

    @expose('/version')
    @has_access
    def version(self):
        try:
            airflow_version = airflow.__version__
        except Exception as e:
            airflow_version = None
            logging.error(e)

        # Get the Git repo and git hash
        git_version = None
        try:
            with open(os.path.join(*[settings.AIRFLOW_HOME,
                                     'airflow', 'git_version'])) as f:
                git_version = f.readline()
        except Exception as e:
            logging.error(e)

        # Render information
        title = "Version Info"
        return self.render_template(
            'airflow/version.html',
            title=title,
            airflow_version=airflow_version,
            git_version=git_version)


class ConfigurationView(AirflowBaseView):
    default_view = 'conf'

    @expose('/configuration')
    @has_access
    def conf(self):
        raw = request.args.get('raw') == "true"
        title = "Airflow Configuration"
        subtitle = configuration.AIRFLOW_CONFIG
        # Don't show config when expose_config variable is False in airflow config
        if conf.getboolean("webserver", "expose_config"):
            with open(configuration.AIRFLOW_CONFIG, 'r') as f:
                config = f.read()
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
                lexers.IniLexer(),  # Lexer call
                HtmlFormatter(noclasses=True))
            )
            return self.render_template(
                'airflow/config.html',
                pre_subtitle=settings.HEADER + "  v" + airflow.__version__,
                code_html=code_html, title=title, subtitle=subtitle,
                table=table)


######################################################################################
#                                    ModelViews
######################################################################################


class DagFilter(BaseFilter):
    def apply(self, query, func):  # noqa
        if appbuilder.sm.has_all_dags_access():
            return query
        filter_dag_ids = appbuilder.sm.get_accessible_dag_ids()
        return query.filter(self.model.dag_id.in_(filter_dag_ids))


class TrackNoNotNullFilter(BaseFilter):
    def apply(self, query, func):  # noqa
        ti = self.model
        ret = query.filter(ti.car_code.isnot(None)).distinct(ti.car_code).group_by(ti)
        return ret

class BoltNoNotNullFilter(BaseFilter):
    def apply(self, query, func):  # noqa
        ti = self.model
        return query.filter(ti.bolt_number.isnot(None)).distinct(ti.bolt_number).group_by(ti)


class ErrorTagFilter(BaseFilter):

    def apply(self, query, func):  # noqa
        _logger.info("ErrorTagFilter: {}".format(pprint.pformat(func)))
        query, field = get_field_setup_query(query, self.model, self.column_name)
        return query


class AirflowModelView(ModelView):
    list_widget = AirflowModelListWidget
    page_size = PAGE_SIZE

    CustomSQLAInterface = wwwutils.CustomSQLAInterface


class SlaMissModelView(AirflowModelView):
    route_base = '/slamiss'

    datamodel = AirflowModelView.CustomSQLAInterface(SlaMiss)

    base_permissions = ['can_list']

    list_columns = ['dag_id', 'task_id', 'execution_date', 'email_sent', 'timestamp']
    add_columns = ['dag_id', 'task_id', 'execution_date', 'email_sent', 'timestamp']
    edit_columns = ['dag_id', 'task_id', 'execution_date', 'email_sent', 'timestamp']
    search_columns = ['dag_id', 'task_id', 'email_sent', 'timestamp', 'execution_date']
    base_order = ('execution_date', 'desc')
    base_filters = [['dag_id', DagFilter, lambda: []]]

    label_columns = {
        'dag_id': lazy_gettext('Dag Id'),
        'task_id': lazy_gettext('Task Id'),
        'execution_date': lazy_gettext('Execution Date'),
        'email_sent': lazy_gettext('Email Sent'),
        'timestamp': lazy_gettext('Timestamp'),
    }
    formatters_columns = {
        'task_id': wwwutils.task_instance_link,
        'execution_date': wwwutils.datetime_f('execution_date'),
        'timestamp': wwwutils.datetime_f('timestamp'),
        'dag_id': wwwutils.dag_link,
    }


class XComModelView(AirflowModelView):
    route_base = '/xcom'

    datamodel = AirflowModelView.CustomSQLAInterface(XCom)

    base_permissions = ['can_add', 'can_list', 'can_edit', 'can_delete']

    search_columns = ['key', 'value', 'timestamp', 'execution_date', 'task_id', 'dag_id']
    list_columns = ['key', 'value', 'timestamp', 'execution_date', 'task_id', 'dag_id']
    add_columns = ['key', 'value', 'execution_date', 'task_id', 'dag_id']
    edit_columns = ['key', 'value', 'execution_date', 'task_id', 'dag_id']
    base_order = ('execution_date', 'desc')
    label_columns = {
        'key': lazy_gettext('Key'), 'value': lazy_gettext('Value'),
        'timestamp': lazy_gettext('Timestamp'), 'execution_date': lazy_gettext('Execution Date'),
        'task_id': lazy_gettext('Task Id'), 'dag_id': lazy_gettext('DAG Id')
    }
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
        self.datamodel.delete_all(items)
        self.update_redirect()
        return redirect(self.get_redirect())

    def pre_add(self, item):
        item.execution_date = timezone.make_aware(item.execution_date)
        item.value = XCom.serialize_value(item.value)

    def pre_update(self, item):
        item.execution_date = timezone.make_aware(item.execution_date)
        item.value = XCom.serialize_value(item.value)


class ConnectionModelView(AirflowModelView):
    route_base = '/connection'

    datamodel = AirflowModelView.CustomSQLAInterface(Connection)

    base_permissions = ['can_add', 'can_list', 'can_edit', 'can_delete']

    extra_fields = ['extra__jdbc__drv_path', 'extra__jdbc__drv_clsname',
                    'extra__google_cloud_platform__project',
                    'extra__google_cloud_platform__key_path',
                    'extra__google_cloud_platform__keyfile_dict',
                    'extra__google_cloud_platform__scope',
                    'extra__google_cloud_platform__num_retries',
                    'extra__grpc__auth_type',
                    'extra__grpc__credential_pem_file',
                    'extra__grpc__scopes']
    list_columns = ['conn_id', 'conn_type', 'host', 'port', 'is_encrypted',
                    'is_extra_encrypted']
    add_columns = edit_columns = ['conn_id', 'conn_type', 'host', 'schema',
                                  'login', 'password', 'port', 'extra'] + extra_fields
    add_form = edit_form = ConnectionForm
    add_template = 'airflow/conn_create.html'
    edit_template = 'airflow/conn_edit.html'
    label_columns = {
        'conn_id': lazy_gettext('Conn Id'), 'conn_type': lazy_gettext('Conn Type'),
        'host': lazy_gettext('Host'), 'port': lazy_gettext('Port'),
        'is_encrypted': lazy_gettext('Is Encrypted'), 'is_extra_encrypted': lazy_gettext('Is Extra Encrypted')
    }
    base_order = ('conn_id', 'asc')

    @action('muldelete', 'Delete', 'Are you sure you want to delete selected records?',
            single=False)
    @has_dag_access(can_dag_edit=True)
    def action_muldelete(self, items):
        self.datamodel.delete_all(items)
        self.update_redirect()
        return redirect(self.get_redirect())

    def process_form(self, form, is_created):
        formdata = form.data
        if formdata['conn_type'] in ['jdbc', 'google_cloud_platform', 'grpc']:
            extra = {
                key: formdata[key]
                for key in self.extra_fields if key in formdata}
            form.extra.data = json.dumps(extra)

    def prefill_form(self, form, pk):
        try:
            d = json.loads(form.data.get('extra', '{}'))
        except Exception:
            d = {}

        if not hasattr(d, 'get'):
            logging.warning('extra field for {} is not iterable'.format(
                form.data.get('conn_id', '<unknown>')))
            return

        for field in self.extra_fields:
            value = d.get(field, '')
            if value:
                field = getattr(form, field)
                field.data = value


class ErrorTagModelView(AirflowModelView):
    route_base = '/error_tag'

    datamodel = AirflowModelView.CustomSQLAInterface(ErrorTag)

    base_permissions = ['can_add', 'can_list', 'can_edit', 'can_delete']

    extra_fields = []
    list_columns = ['value', 'label']
    add_columns = edit_columns = ['value', 'label'] + extra_fields
    add_form = edit_form = ErrorTagForm
    add_template = 'airflow/error_tag_create.html'
    edit_template = 'airflow/error_tag_edit.html'
    label_columns = {
        'value': lazy_gettext('Value'), 'label': lazy_gettext('Label')

    }
    base_order = ('id', 'asc')

    def post_add(self, item):
        super(ErrorTagModelView, self).post_add(item)
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['ADD'], CUSTOM_PAGE_NAME_MAP['ERROR_TAG'], '增加错误标签')
        logging.info(msg)

    def post_update(self, item):
        super(ErrorTagModelView, self).post_update(item)
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['UPDATE'], CUSTOM_PAGE_NAME_MAP['ERROR_TAG'], '修改错误标签')
        logging.info(msg)

    def post_delete(self, item):
        super(ErrorTagModelView, self).post_delete(item)
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['DELETE'], CUSTOM_PAGE_NAME_MAP['ERROR_TAG'], '删除错误标签')
        logging.info(msg)

    @action('export_analysis', "Export Statistics", '', single=False)
    @provide_session
    def action_export_error_tag_statistics(self, error_tags, session=None):
        ret = {}
        d = json.JSONDecoder()
        for var in error_tags:
            try:
                val = d.decode(var.val)
            except Exception:
                val = var.val
            ret[var.key] = val

        response = make_response(json.dumps(ret, sort_keys=True, indent=4, ensure_ascii=False))
        response.headers["Content-Disposition"] = "attachment; filename=错误标签分析.json"
        response.headers["Content-Type"] = "application/json; charset=utf-8"
        return response

    @action('muldelete', 'Delete', 'Are you sure you want to delete selected records?',
            single=False)
    @has_dag_access(can_dag_edit=True)
    def action_muldelete(self, items):
        self.datamodel.delete_all(items)
        self.update_redirect()
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['DELETE'], CUSTOM_PAGE_NAME_MAP['ERROR_TAG'], '删除选中错误标签')
        logging.info(msg)
        return redirect(self.get_redirect())

    # 重写list
    @expose("/list/")
    @has_access
    def list(self):
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['VIEW'], CUSTOM_PAGE_NAME_MAP['ERROR_TAG'], '查看错误标签')
        logging.info(msg)
        return super(ErrorTagModelView, self).list()


class TighteningControllerView(AirflowModelView):
    route_base = '/tightening_controller'

    datamodel = AirflowModelView.CustomSQLAInterface(TighteningController)

    base_permissions = ['can_show', 'can_add', 'can_list', 'can_edit', 'can_delete']

    extra_fields = []
    list_columns = ['controller_name', 'line_code', 'line_name', 'work_center_code', 'work_center_name']
    add_columns = edit_columns = ['controller_name', 'line_code', 'line_name', 'work_center_code',
                                  'work_center_name'] + extra_fields
    add_form = edit_form = TighteningControllerForm
    add_template = 'airflow/tightening_controller_create.html'
    edit_template = 'airflow/tightening_controller_edit.html'
    list_widget = AirflowControllerListWidget
    label_columns = {
        'controller_name': lazy_gettext('Controller Name'),
        'line_code': lazy_gettext('Line Code'),
        'line_name': lazy_gettext('Line Name'),
        'work_center_code': lazy_gettext('Work Center Code'),
        'work_center_name': lazy_gettext('Work Center Name')
    }

    base_order = ('id', 'asc')

    def post_add(self, item):
        super(TighteningControllerView, self).post_add(item)
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['ADD'], CUSTOM_PAGE_NAME_MAP['TIGHTENING_CONTROLLER'],
                                       '增加控制器')
        logging.info(msg)

    def post_update(self, item):
        super(TighteningControllerView, self).post_update(item)
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['UPDATE'], CUSTOM_PAGE_NAME_MAP['TIGHTENING_CONTROLLER'],
                                       '修改控制器')
        logging.info(msg)

    def post_delete(self, item):
        super(TighteningControllerView, self).post_delete(item)
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['DELETE'], CUSTOM_PAGE_NAME_MAP['TIGHTENING_CONTROLLER'],
                                       '删除控制器')
        logging.info(msg)

    @action('muldelete', 'Delete', 'Are you sure you want to delete selected records?',
            single=False)
    @has_dag_access(can_dag_edit=True)
    def action_muldelete(self, items):
        self.datamodel.delete_all(items)
        self.update_redirect()
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['DELETE'], CUSTOM_PAGE_NAME_MAP['TIGHTENING_CONTROLLER'],
                                       '删除选中控制器')
        logging.info(msg)
        return redirect(self.get_redirect())

    @expose("/list/")
    @has_access
    def list(self):
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['VIEW'], CUSTOM_PAGE_NAME_MAP['TIGHTENING_CONTROLLER'],
                                       '查看控制器')
        logging.info(msg)
        return super(TighteningControllerView, self).list()


class PoolModelView(AirflowModelView):
    route_base = '/pool'

    datamodel = AirflowModelView.CustomSQLAInterface(models.Pool)

    base_permissions = ['can_add', 'can_list', 'can_edit', 'can_delete']

    list_columns = ['pool', 'slots', 'used_slots', 'queued_slots']
    add_columns = ['pool', 'slots', 'description']
    edit_columns = ['pool', 'slots', 'description']
    label_columns = {
        'pool': lazy_gettext('Pool'), 'slots': lazy_gettext('Slots'),
        'used_slots': lazy_gettext('Used Slots'), 'queued_slots': lazy_gettext('Queued Slots')
    }
    base_order = ('pool', 'asc')

    @action('muldelete', 'Delete', 'Are you sure you want to delete selected records?',
            single=False)
    def action_muldelete(self, items):
        if any(item.pool == models.Pool.DEFAULT_POOL_NAME for item in items):
            flash("default_pool cannot be deleted", 'error')
            self.update_redirect()
            return redirect(self.get_redirect())
        self.datamodel.delete_all(items)
        self.update_redirect()
        return redirect(self.get_redirect())

    def pool_link(attr):
        pool_id = attr.get('pool')
        if pool_id is not None:
            url = url_for('TaskInstanceModelView.list', _flt_3_pool=pool_id)
            return Markup("<a href='{url}'>{pool_id}</a>").format(url=url, pool_id=pool_id)
        else:
            return Markup('<span class="label label-danger">Invalid</span>')

    def fused_slots(attr):
        pool_id = attr.get('pool')
        used_slots = attr.get('used_slots')
        if pool_id is not None and used_slots is not None:
            url = url_for('TaskInstanceModelView.list', _flt_3_pool=pool_id, _flt_3_state='running')
            return Markup("<a href='{url}'>{used_slots}</a>").format(url=url, used_slots=used_slots)
        else:
            return Markup('<span class="label label-danger">Invalid</span>')

    def fqueued_slots(attr):
        pool_id = attr.get('pool')
        queued_slots = attr.get('queued_slots')
        if pool_id is not None and queued_slots is not None:
            url = url_for('TaskInstanceModelView.list', _flt_3_pool=pool_id, _flt_3_state='queued')
            return Markup("<a href='{url}'>{queued_slots}</a>").format(url=url, queued_slots=queued_slots)
        else:
            return Markup('<span class="label label-danger">Invalid</span>')

    formatters_columns = {
        'pool': pool_link,
        'used_slots': fused_slots,
        'queued_slots': fqueued_slots
    }

    validators_columns = {
        'pool': [validators.DataRequired()],
        'slots': [validators.NumberRange(min=0)]
    }


class VariableModelView(AirflowModelView):
    route_base = '/variable'

    list_template = 'airflow/variable_list.html'
    edit_template = 'airflow/variable_edit.html'

    datamodel = AirflowModelView.CustomSQLAInterface(models.Variable)

    base_permissions = ['can_show', 'can_add', 'can_list', 'can_edit', 'can_delete', 'can_varimport']

    list_columns = ['key', 'val', 'is_encrypted', 'is_curve_template', 'active']
    add_columns = ['key', 'val', 'is_curve_template', 'active']
    edit_columns = ['key', 'val', 'is_curve_template', 'active']
    search_columns = ['key', 'val', 'is_curve_template', 'active']
    label_columns = {
        'key': lazy_gettext('Key'), 'val': lazy_gettext('Val'),
        'is_encrypted': lazy_gettext('Is Encrypted'), 'is_curve_template': lazy_gettext('Is Curve Template'),
        'active': lazy_gettext('Active')
    }
    # add_form = edit_form = VariableForm

    base_order = ('key', 'asc')

    @expose("/list/")
    @has_access
    def list(self):
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['VIEW'], CUSTOM_PAGE_NAME_MAP['TIGHTENING_CURVE_TEMPLATE'],
                                       '曲线模板：查看变量')
        logging.info(msg)
        return super(VariableModelView, self).list()

    @staticmethod
    def generateCurveParamKey(key):
        return "{}@@{}".format(key, uuid.uuid3(uuid.NAMESPACE_DNS, key))

    @staticmethod
    def is_curve_param_key(key: str) -> bool:
        return '@@' in key

    def pre_add(self, item):
        super(VariableModelView, self).pre_add(item)
        if item.is_curve_template:
            item.key = self.generateCurveParamKey(item.key)

    def post_add(self, item):
        super(VariableModelView, self).post_add(item)
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['ADD'], CUSTOM_PAGE_NAME_MAP['TIGHTENING_CURVE_TEMPLATE'],
                                       '曲线模板：增加变量')
        logging.info(msg)

    def pre_update(self, item: models.Variable):
        super(VariableModelView, self).pre_update(item)
        if item.is_curve_template and self.is_curve_param_key(item.key):
            return
        if item.is_curve_template:
            item.key = self.generateCurveParamKey(item.key)
            return
        if self.is_curve_param_key(item.key):
            item.key = item.key.split('@@')[0]
            return

    def post_update(self, item):
        super(VariableModelView, self).post_update(item)
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['UPDATE'],
                                       CUSTOM_PAGE_NAME_MAP['TIGHTENING_CURVE_TEMPLATE'], '曲线模板：修改变量')
        logging.info(msg)

    def hidden_field_formatter(attr):
        if isinstance(attr, str):
            return attr
        key = attr.get('key')
        val = attr.get('val')
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

    def prefill_form(self, form, id):
        if wwwutils.should_hide_value_for_key(form.key.data):
            form.val.data = '*' * 8

    def post_delete(self, item):
        super(VariableModelView, self).post_delete(item)
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['DELETE'],
                                       CUSTOM_PAGE_NAME_MAP['TIGHTENING_CURVE_TEMPLATE'], '曲线模板：删除变量')
        logging.info(msg)

    @action('muldelete', 'Delete', 'Are you sure you want to delete selected records?',
            single=False)
    def action_muldelete(self, items):
        self.datamodel.delete_all(items)
        self.update_redirect()
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['DELETE'],
                                       CUSTOM_PAGE_NAME_MAP['TIGHTENING_CURVE_TEMPLATE'], '曲线模板：删除选中变量')
        logging.info(msg)
        return redirect(self.get_redirect())

    @action('varexport', 'Export', '', single=False)
    def action_varexport(self, items):
        var_dict = {}
        d = json.JSONDecoder()
        for var in items:
            try:
                val = d.decode(var.val)
            except Exception:
                val = var.val
            var_dict[var.key] = val

        response = make_response(json.dumps(var_dict, sort_keys=True, indent=4, ensure_ascii=False))
        response.headers["Content-Disposition"] = "attachment; filename=variables.json"
        response.headers["Content-Type"] = "application/json; charset=utf-8"
        return response

    @expose('/varimport', methods=["POST"])
    @has_access
    @action_logging
    def varimport(self):
        try:
            out = request.files['file'].read()
            if not PY2 and isinstance(out, bytes):
                d = json.loads(out.decode('utf-8'))
            else:
                d = json.loads(out)
        except Exception:
            self.update_redirect()
            flash("Missing file or syntax error.", 'error')
            return redirect(self.get_redirect())
        else:
            suc_count = fail_count = 0
            for k, v in d.items():
                try:
                    is_curve_template = VariableModelView.is_curve_param_key(k)
                    models.Variable.set(k, v, serialize_json=isinstance(v, dict), is_curve_template=is_curve_template)
                except Exception as e:
                    logging.info('Variable import failed: {}'.format(repr(e)))
                    fail_count += 1
                else:
                    suc_count += 1
            flash("{} variable(s) successfully updated.".format(suc_count))
            if fail_count:
                flash("{} variable(s) failed to be updated.".format(fail_count), 'error')
            self.update_redirect()
            return redirect(self.get_redirect())


class JobModelView(AirflowModelView):
    route_base = '/job'

    datamodel = AirflowModelView.CustomSQLAInterface(jobs.BaseJob)

    base_permissions = ['can_list']

    list_columns = ['id', 'dag_id', 'state', 'job_type', 'start_date',
                    'end_date', 'latest_heartbeat',
                    'executor_class', 'hostname', 'unixname']
    label_columns = {
        'id': lazy_gettext('Id'),
        'dag_id': lazy_gettext('Dag Id'),
        'state': lazy_gettext('State'),
        'job_type': lazy_gettext('Job Type'),
        'start_date': lazy_gettext('Start Date'),
        'end_date': lazy_gettext('End Date'),
        'latest_heartbeat': lazy_gettext('Latest Heartbeat'),
        'executor_class': lazy_gettext('Executor Class'),
        'hostname': lazy_gettext('Hostname'),
        'unixname': lazy_gettext('Unixname'),
    }

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
    route_base = '/dagrun'

    datamodel = AirflowModelView.CustomSQLAInterface(models.DagRun)

    base_permissions = ['can_list', 'can_add']

    add_columns = ['state', 'dag_id', 'execution_date', 'run_id', 'external_trigger', 'conf']
    list_columns = ['state', 'dag_id', 'execution_date', 'run_id', 'external_trigger']
    search_columns = ['state', 'dag_id', 'execution_date', 'run_id', 'external_trigger']
    label_columns = {
        'state': lazy_gettext('State'),
        'dag_id': lazy_gettext('Dag Id'),
        'execution_date': lazy_gettext('Execution Date'),
        'run_id': lazy_gettext('Run Id'),
        'external_trigger': lazy_gettext('External Trigger')
    }
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
    def action_muldelete(self, items, session=None):
        self.datamodel.delete_all(items)
        self.update_redirect()
        dirty_ids = []
        for item in items:
            dirty_ids.append(item.dag_id)
        return redirect(self.get_redirect())

    @action('set_running', "Set state to 'running'", '', single=False)
    @provide_session
    def action_set_running(self, drs, session=None):
        try:
            DR = models.DagRun
            count = 0
            dirty_ids = []
            for dr in session.query(DR).filter(
                DR.id.in_([dagrun.id for dagrun in drs])).all():
                dirty_ids.append(dr.dag_id)
                count += 1
                dr.start_date = timezone.utcnow()
                dr.state = State.RUNNING
            session.commit()
            flash("{count} dag runs were set to running".format(count=count))
        except Exception as ex:
            flash(str(ex), 'error')
            flash('Failed to set state', 'error')
        return redirect(self.get_default_url())

    @action('set_failed', "Set state to 'failed'",
            "All running task instances would also be marked as failed, are you sure?",
            single=False)
    @provide_session
    def action_set_failed(self, drs, session=None):
        try:
            DR = models.DagRun
            count = 0
            dirty_ids = []
            altered_tis = []
            for dr in session.query(DR).filter(
                DR.id.in_([dagrun.id for dagrun in drs])).all():
                dirty_ids.append(dr.dag_id)
                count += 1
                altered_tis += \
                    set_dag_run_state_to_failed(dagbag.get_dag(dr.dag_id),
                                                dr.execution_date,
                                                commit=True,
                                                session=session)
            altered_ti_count = len(altered_tis)
            flash(
                "{count} dag runs and {altered_ti_count} task instances "
                "were set to failed".format(count=count, altered_ti_count=altered_ti_count))
        except Exception:
            flash('Failed to set state', 'error')
        return redirect(self.get_default_url())

    @action('set_success', "Set state to 'success'",
            "All task instances would also be marked as success, are you sure?",
            single=False)
    @provide_session
    def action_set_success(self, drs, session=None):
        try:
            DR = models.DagRun
            count = 0
            dirty_ids = []
            altered_tis = []
            for dr in session.query(DR).filter(
                DR.id.in_([dagrun.id for dagrun in drs])).all():
                dirty_ids.append(dr.dag_id)
                count += 1
                altered_tis += \
                    set_dag_run_state_to_success(dagbag.get_dag(dr.dag_id),
                                                 dr.execution_date,
                                                 commit=True,
                                                 session=session)
            altered_ti_count = len(altered_tis)
            flash(
                "{count} dag runs and {altered_ti_count} task instances "
                "were set to success".format(count=count, altered_ti_count=altered_ti_count))
        except Exception:
            flash('Failed to set state', 'error')
        return redirect(self.get_default_url())


class LogModelView(AirflowModelView):
    route_base = '/log'

    datamodel = AirflowModelView.CustomSQLAInterface(Log)

    base_permissions = ['can_list']

    list_columns = ['id', 'dttm', 'dag_id', 'task_id', 'event', 'execution_date',
                    'owner', 'extra']
    label_columns = {
        'id': lazy_gettext('Id'), 'dttm': lazy_gettext('Dttm'),
        'dag_id': lazy_gettext('Dag Id'), 'task_id': lazy_gettext('Task Id'),
        'event': lazy_gettext('Event'), 'execution_date': lazy_gettext('Execution Date'),
        'owners': lazy_gettext('Owner'), 'extra': lazy_gettext('Extra')
    }
    search_columns = ['dag_id', 'task_id', 'execution_date', 'extra']

    base_order = ('dttm', 'desc')

    base_filters = [['dag_id', DagFilter, lambda: []]]

    formatters_columns = {
        'dttm': wwwutils.datetime_f('dttm'),
        'execution_date': wwwutils.datetime_f('execution_date'),
        'dag_id': wwwutils.dag_link,
    }


class TaskInstanceModelView(AirflowModelView):
    route_base = '/taskinstance'

    datamodel = AirflowModelView.CustomSQLAInterface(models.TaskInstance)

    base_permissions = ['can_list']

    page_size = PAGE_SIZE

    list_columns = ['state', 'dag_id', 'task_id', 'line_code', 'entity_id', 'execution_date', 'measure_result',
                    'result', 'error_tag', 'type',
                    'final_state',
                    'start_date', 'end_date', 'duration', 'job_id',
                    'priority_weight', 'try_number',
                    # 'unixname', 'hostname', 'queue', 'queued_dttm', 'operator',
                    'pool', 'log_url']

    search_columns = ['state', 'type', 'dag_id', 'entity_id', 'measure_result', 'result', 'final_state',
                      'task_id', 'error_tag',
                      'execution_date', 'hostname',
                      'queue', 'pool', 'operator', 'start_date', 'end_date']

    label_columns = {
        'state': lazy_gettext('State'),
        'error_tag': lazy_gettext('Error Tags'),
        'dag_id': lazy_gettext('Dag Id'),
        'task_id': lazy_gettext('Task Id'),
        'line_code': lazy_gettext('Line Code'),
        'entity_id': lazy_gettext('Entity Id'),
        'type': lazy_gettext('Task Instance Type'),
        'execution_date': lazy_gettext('Execution Date'),
        'measure_result': lazy_gettext('Measure Result'),
        'result': lazy_gettext('Result'),
        'final_state': lazy_gettext('Final State'),
        'start_date': lazy_gettext('Start Date'),
        'end_date': lazy_gettext('End Date'),
        'duration': lazy_gettext('Duration'),
        'job_id': lazy_gettext('Job Id'),
        'priority_weight': lazy_gettext('Priority Weight'),
        'try_number': lazy_gettext('Try Number'),
        'pool': lazy_gettext('Pool'),
        'log_url': lazy_gettext('Log Url')
    }

    base_order = ('job_id', 'asc')

    base_filters = [['dag_id', DagFilter, lambda: []]]

    def log_url_formatter(attr):
        log_url = attr.get('log_url')
        return Markup(
            '<a href="{log_url}">'
            '    <span class="glyphicon glyphicon-book" aria-hidden="true">'
            '</span></a>').format(log_url=log_url)

    def duration_f(attr):
        end_date = attr.get('end_date')
        duration = attr.get('duration')
        if end_date and duration:
            return timedelta(seconds=duration)

    def error_tag_f(attr):
        ret = []
        try:
            error_tags = json.loads(attr.get('error_tag') or '[]')
            if not error_tags:
                return u'无异常标签'
            error_tag_vals = ErrorTag.get_all_dict() or {}
            for tag in error_tags:
                v = error_tag_vals.get(str(tag), '')
                if not v:
                    continue
                ret.append(v)
        except Exception as e:
            return ','.join(ret)
        return ','.join(ret)

    def type_f(attr):
        ti_type = attr.get('type')
        if ti_type == 'rework':
            return lazy_gettext('Task Instance Rework')
        return lazy_gettext('Task Instance Normal')

    formatters_columns = {
        'log_url': log_url_formatter,
        'task_id': wwwutils.task_instance_link,
        'hostname': wwwutils.nobr_f('hostname'),
        'state': wwwutils.state_f,
        'error_tag': error_tag_f,
        'execution_date': wwwutils.datetime_f('execution_date'),
        'start_date': wwwutils.datetime_f('start_date'),
        'end_date': wwwutils.datetime_f('end_date'),
        'queued_dttm': wwwutils.datetime_f('queued_dttm'),
        'dag_id': wwwutils.dag_link,
        'duration': duration_f,
        'type': type_f,
    }

    @provide_session
    @action('clear', lazy_gettext('Clear'),
            lazy_gettext('Are you sure you want to clear the state of the selected task'
                         ' instance(s) and set their dagruns to the running state?'),
            single=False)
    def action_clear(self, tis, session=None):
        try:
            dag_to_tis = {}

            for ti in tis:
                dag = dagbag.get_dag(ti.dag_id)
                tis = dag_to_tis.setdefault(dag, [])
                tis.append(ti)

            for dag, tis in dag_to_tis.items():
                models.clear_task_instances(tis, session=session, dag=dag)

            session.commit()
            flash("{0} task instances have been cleared".format(len(tis)))
            self.update_redirect()
            return redirect(self.get_redirect())

        except Exception:
            flash('Failed to clear task instances', 'error')

    @provide_session
    def set_task_instance_state(self, tis, target_state, session=None):
        try:
            count = len(tis)
            for ti in tis:
                ti.set_state(target_state, session=session)
            session.commit()
            flash("{count} task instances state were set to '{target_state}'".format(
                count=count, target_state=target_state))
        except Exception:
            flash('Failed to set state', 'error')

    @provide_session
    def set_task_instance_final_state(self, tis, target_state, session=None):
        try:
            count = len(tis)
            for ti in tis:
                ti.set_final_state(target_state, session=session)
            session.commit()
            flash("{count} task instances final state were set to '{target_state}'".format(
                count=count, target_state=target_state))
        except Exception:
            flash('Failed to set final state', 'error')

    @action('set_running', "Set state to 'running'", '', single=False)
    @has_dag_access(can_dag_edit=True)
    def action_set_running(self, tis):
        self.set_task_instance_state(tis, State.RUNNING)
        self.update_redirect()
        return redirect(self.get_redirect())

    @action('set_failed', "Set state to 'failed'", '', single=False)
    @has_dag_access(can_dag_edit=True)
    def action_set_failed(self, tis):
        self.set_task_instance_state(tis, State.FAILED)
        self.update_redirect()
        return redirect(self.get_redirect())

    @action('set_final_state_nok', "Set Final State to 'failed'", '', single=False, multiple=False)
    @has_dag_access(can_dag_edit=True)
    def action_set_final_state_nok(self, tis):
        self.set_task_instance_final_state(tis, State.FINALNOK)
        self.update_redirect()
        return redirect(self.get_redirect())

    @action('set_final_state_ok', "Set Final State to 'success'", '', single=False, multiple=False)
    @has_dag_access(can_dag_edit=True)
    def action_set_final_state_ok(self, tis):
        self.set_task_instance_final_state(tis, State.FINALOK)
        self.update_redirect()
        return redirect(self.get_redirect())

    @action('set_success', "Set state to 'success'", '', single=False)
    @has_dag_access(can_dag_edit=True)
    def action_set_success(self, tis):
        self.set_task_instance_state(tis, State.SUCCESS)
        self.update_redirect()
        return redirect(self.get_redirect())

    @action('set_retry', "Set state to 'up_for_retry'", '', single=False)
    @has_dag_access(can_dag_edit=True)
    def action_set_retry(self, tis):
        self.set_task_instance_state(tis, State.UP_FOR_RETRY)
        self.update_redirect()
        return redirect(self.get_redirect())

    def get_one(self, id):
        """
        As a workaround for AIRFLOW-252, this method overrides Flask-Admin's
        ModelView.get_one().

        TODO: this method should be removed once the below bug is fixed on
        Flask-Admin side. https://github.com/flask-admin/flask-admin/issues/1226
        """
        task_id, dag_id, execution_date = iterdecode(id)  # noqa
        execution_date = pendulum.parse(execution_date)
        return self.session.query(self.model).get((task_id, dag_id, execution_date))


class DagModelView(AirflowModelView):
    route_base = '/dagmodel'

    datamodel = AirflowModelView.CustomSQLAInterface(models.DagModel)

    base_permissions = ['can_list', 'can_show']

    list_columns = ['dag_id', 'is_paused', 'last_scheduler_run',
                    'last_expired', 'scheduler_lock', 'fileloc', 'owners']
    label_columns = {
        'dag_id': lazy_gettext('Dag Id'), 'is_paused': lazy_gettext('Is Paused'),
        'last_scheduler_run': lazy_gettext('Last Scheduler Run'), 'last_expired': lazy_gettext('Last Expired'),
        'scheduler_lock': lazy_gettext('Scheduler Lock'), 'fileloc': lazy_gettext('Fileloc'),
        'owners': lazy_gettext('Owners')
    }
    formatters_columns = {
        'dag_id': wwwutils.dag_link
    }

    base_filters = [['dag_id', DagFilter, lambda: []]]

    def get_query(self):
        """
        Default filters for model
        """
        return (
            super(DagModelView, self).get_query()
                .filter(or_(models.DagModel.is_active,
                            models.DagModel.is_paused))
                .filter(~models.DagModel.is_subdag)
        )

    def get_count_query(self):
        """
        Default filters for model
        """
        return (
            super(DagModelView, self).get_count_query()
                .filter(models.DagModel.is_active)
                .filter(~models.DagModel.is_subdag)
        )

    @has_access
    @permission_name("list")
    @provide_session
    @expose('/autocomplete')
    def autocomplete(self, session=None):
        query = unquote(request.args.get('query', ''))

        if not query:
            wwwutils.json_response([])

        # Provide suggestions of dag_ids and owners
        dag_ids_query = session.query(DagModel.dag_id.label('item')).filter(
            ~DagModel.is_subdag, DagModel.is_active,
            DagModel.dag_id.ilike('%' + query + '%'))

        owners_query = session.query(func.distinct(DagModel.owners).label('item')).filter(
            ~DagModel.is_subdag, DagModel.is_active,
            DagModel.owners.ilike('%' + query + '%'))

        # Hide paused dags
        if request.args.get('showPaused', 'True').lower() == 'false':
            dag_ids_query = dag_ids_query.filter(~DagModel.is_paused)
            owners_query = owners_query.filter(~DagModel.is_paused)

        filter_dag_ids = appbuilder.sm.get_accessible_dag_ids()
        if 'all_dags' not in filter_dag_ids:
            dag_ids_query = dag_ids_query.filter(DagModel.dag_id.in_(filter_dag_ids))
            owners_query = owners_query.filter(DagModel.dag_id.in_(filter_dag_ids))

        payload = [row[0] for row in dag_ids_query.union(owners_query).limit(10).all()]

        return wwwutils.json_response(payload)


class CurveAnalysisListWidget(AirflowModelListWidget):
    template = 'airflow/curve_analysis_list.html'


class CurveAnalysisTrackNoView(TaskInstanceModelView):
    route_base = '/curves_analysis_track'

    datamodel = wwwutils.DistinctSQLAInterface(models.TaskInstance)

    base_permissions = ['can_list', 'can_show']

    list_widget = CurveAnalysisListWidget

    list_title = lazy_gettext("Analysis Via Track No")

    list_columns = ['car_code']

    search_columns = ['car_code']

    label_columns = TaskInstanceModelView.label_columns.update({
        'car_code': lazy_gettext('Car Code')
    })

    base_filters = [['car_code', TrackNoNotNullFilter, lambda: []]]

    base_order = ('car_code', 'asc')


class CurveAnalysisBoltNoView(CurveAnalysisTrackNoView):
    route_base = '/curves_analysis_bolt'

    list_title = lazy_gettext("Analysis Via Bolt No")

    list_columns = ['bolt_number']

    search_columns = ['bolt_number']

    base_filters = [['bolt_number', BoltNoNotNullFilter, lambda: []]]

    base_order = ('bolt_number', 'asc')


class CurveAnalysisControllerView(TighteningControllerView):
    route_base = '/curves_analysis_controller'

    list_title = lazy_gettext("Analysis Via Controller")

    # list_columns = ['controller_name']

    base_permissions = ['can_show']

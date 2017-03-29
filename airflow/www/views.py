# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from past.builtins import basestring, unicode

import ast
import os
import pkg_resources
import socket
from functools import wraps
from datetime import datetime, timedelta
import dateutil.parser
import copy
import json
import bleach

import inspect
from textwrap import dedent
import traceback

import sqlalchemy as sqla
from sqlalchemy import or_, desc, and_, union_all

from flask import (
    redirect, url_for, request, Markup, Response, current_app, render_template, make_response)
from flask_admin import BaseView, expose, AdminIndexView
from flask_admin.contrib.sqla import ModelView
from flask_admin.actions import action
from flask_admin.babel import lazy_gettext
from flask_admin.tools import iterdecode
from flask_login import flash
from flask._compat import PY2

from jinja2.sandbox import ImmutableSandboxedEnvironment
import markdown
import nvd3

from wtforms import (
    Form, SelectField, TextAreaField, PasswordField, StringField, validators)

from pygments import highlight, lexers
from pygments.formatters import HtmlFormatter

import airflow
from airflow import configuration as conf
from airflow import models
from airflow import settings
from airflow.exceptions import AirflowException
from airflow.settings import Session
from airflow.models import XCom, DagRun
from airflow.ti_deps.dep_context import DepContext, QUEUE_DEPS, SCHEDULER_DEPS

from airflow.models import BaseOperator
from airflow.operators.subdag_operator import SubDagOperator

from airflow.utils.logging import LoggingMixin
from airflow.utils.json import json_ser
from airflow.utils.state import State
from airflow.utils.db import provide_session
from airflow.utils.helpers import alchemy_to_dict
from airflow.utils import logging as log_utils
from airflow.utils.dates import infer_time_unit, scale_time_units
from airflow.www import utils as wwwutils
from airflow.www.forms import DateTimeForm, DateTimeWithNumRunsForm
from airflow.configuration import AirflowConfigException

QUERY_LIMIT = 100000
CHART_LIMIT = 200000

dagbag = models.DagBag(settings.DAGS_FOLDER)

login_required = airflow.login.login_required
current_user = airflow.login.current_user
logout_user = airflow.login.logout_user

FILTER_BY_OWNER = False

DEFAULT_SENSITIVE_VARIABLE_FIELDS = (
    'password',
    'secret',
    'passwd',
    'authorization',
    'api_key',
    'apikey',
    'access_token',
)

if conf.getboolean('webserver', 'FILTER_BY_OWNER'):
    # filter_by_owner if authentication is enabled and filter_by_owner is true
    FILTER_BY_OWNER = not current_app.config['LOGIN_DISABLED']


def dag_link(v, c, m, p):
    dag_id = bleach.clean(m.dag_id)
    url = url_for(
        'airflow.graph',
        dag_id=dag_id)
    return Markup(
        '<a href="{}">{}</a>'.format(url, dag_id))


def log_url_formatter(v, c, m, p):
    return Markup(
        '<a href="{m.log_url}">'
        '    <span class="glyphicon glyphicon-book" aria-hidden="true">'
        '</span></a>').format(**locals())


def task_instance_link(v, c, m, p):
    dag_id = bleach.clean(m.dag_id)
    task_id = bleach.clean(m.task_id)
    url = url_for(
        'airflow.task',
        dag_id=dag_id,
        task_id=task_id,
        execution_date=m.execution_date.isoformat())
    url_root = url_for(
        'airflow.graph',
        dag_id=dag_id,
        root=task_id,
        execution_date=m.execution_date.isoformat())
    return Markup(
        """
        <span style="white-space: nowrap;">
        <a href="{url}">{task_id}</a>
        <a href="{url_root}" title="Filter on this task and upstream">
        <span class="glyphicon glyphicon-filter" style="margin-left: 0px;"
            aria-hidden="true"></span>
        </a>
        </span>
        """.format(**locals()))


def state_token(state):
    color = State.color(state)
    return Markup(
        '<span class="label" style="background-color:{color};">'
        '{state}</span>'.format(**locals()))


def state_f(v, c, m, p):
    return state_token(m.state)


def duration_f(v, c, m, p):
    if m.end_date and m.duration:
        return timedelta(seconds=m.duration)


def datetime_f(v, c, m, p):
    attr = getattr(m, p)
    dttm = attr.isoformat() if attr else ''
    if datetime.now().isoformat()[:4] == dttm[:4]:
        dttm = dttm[5:]
    return Markup("<nobr>{}</nobr>".format(dttm))


def nobr_f(v, c, m, p):
    return Markup("<nobr>{}</nobr>".format(getattr(m, p)))


def label_link(v, c, m, p):
    try:
        default_params = ast.literal_eval(m.default_params)
    except:
        default_params = {}
    url = url_for(
        'airflow.chart', chart_id=m.id, iteration_no=m.iteration_no,
        **default_params)
    return Markup("<a href='{url}'>{m.label}</a>".format(**locals()))


def pool_link(v, c, m, p):
    url = '/admin/taskinstance/?flt1_pool_equals=' + m.pool
    return Markup("<a href='{url}'>{m.pool}</a>".format(**locals()))


def pygment_html_render(s, lexer=lexers.TextLexer):
    return highlight(
        s,
        lexer(),
        HtmlFormatter(linenos=True),
    )


def render(obj, lexer):
    out = ""
    if isinstance(obj, basestring):
        out += pygment_html_render(obj, lexer)
    elif isinstance(obj, (tuple, list)):
        for i, s in enumerate(obj):
            out += "<div>List item #{}</div>".format(i)
            out += "<div>" + pygment_html_render(s, lexer) + "</div>"
    elif isinstance(obj, dict):
        for k, v in obj.items():
            out += '<div>Dict item "{}"</div>'.format(k)
            out += "<div>" + pygment_html_render(v, lexer) + "</div>"
    return out


def wrapped_markdown(s):
    return '<div class="rich_doc">' + markdown.markdown(s) + "</div>"


attr_renderer = {
    'bash_command': lambda x: render(x, lexers.BashLexer),
    'hql': lambda x: render(x, lexers.SqlLexer),
    'sql': lambda x: render(x, lexers.SqlLexer),
    'doc': lambda x: render(x, lexers.TextLexer),
    'doc_json': lambda x: render(x, lexers.JsonLexer),
    'doc_rst': lambda x: render(x, lexers.RstLexer),
    'doc_yaml': lambda x: render(x, lexers.YamlLexer),
    'doc_md': wrapped_markdown,
    'python_callable': lambda x: render(
        inspect.getsource(x), lexers.PythonLexer),
}


def data_profiling_required(f):
    '''
    Decorator for views requiring data profiling access
    '''
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if (
            current_app.config['LOGIN_DISABLED'] or
            (not current_user.is_anonymous() and current_user.data_profiling())
        ):
            return f(*args, **kwargs)
        else:
            flash("This page requires data profiling privileges", "error")
            return redirect(url_for('admin.index'))
    return decorated_function


def fused_slots(v, c, m, p):
    url = (
        '/admin/taskinstance/' +
        '?flt1_pool_equals=' + m.pool +
        '&flt2_state_equals=running')
    return Markup("<a href='{0}'>{1}</a>".format(url, m.used_slots()))


def fqueued_slots(v, c, m, p):
    url = (
        '/admin/taskinstance/' +
        '?flt1_pool_equals=' + m.pool +
        '&flt2_state_equals=queued&sort=10&desc=1')
    return Markup("<a href='{0}'>{1}</a>".format(url, m.queued_slots()))


def recurse_tasks(tasks, task_ids, dag_ids, task_id_to_dag):
    if isinstance(tasks, list):
        for task in tasks:
            recurse_tasks(task, task_ids, dag_ids, task_id_to_dag)
        return
    if isinstance(tasks, SubDagOperator):
        subtasks = tasks.subdag.tasks
        dag_ids.append(tasks.subdag.dag_id)
        for subtask in subtasks:
            if subtask.task_id not in task_ids:
                task_ids.append(subtask.task_id)
                task_id_to_dag[subtask.task_id] = tasks.subdag
        recurse_tasks(subtasks, task_ids, dag_ids, task_id_to_dag)
    if isinstance(tasks, BaseOperator):
        task_id_to_dag[tasks.task_id] = tasks.dag


def should_hide_value_for_key(key_name):
    return any(s in key_name for s in DEFAULT_SENSITIVE_VARIABLE_FIELDS) \
           and conf.getboolean('admin', 'hide_sensitive_variable_fields')


class Airflow(BaseView):

    def is_visible(self):
        return False

    @expose('/')
    @login_required
    def index(self):
        return self.render('airflow/dags.html')

    @expose('/chart_data')
    @data_profiling_required
    @wwwutils.gzipped
    # @cache.cached(timeout=3600, key_prefix=wwwutils.make_cache_key)
    def chart_data(self):
        from airflow import macros
        import pandas as pd
        session = settings.Session()
        chart_id = request.args.get('chart_id')
        csv = request.args.get('csv') == "true"
        chart = session.query(models.Chart).filter_by(id=chart_id).first()
        db = session.query(
            models.Connection).filter_by(conn_id=chart.conn_id).first()
        session.expunge_all()
        session.commit()
        session.close()

        payload = {}
        payload['state'] = 'ERROR'
        payload['error'] = ''

        # Processing templated fields
        try:
            args = ast.literal_eval(chart.default_params)
            if type(args) is not type(dict()):
                raise AirflowException('Not a dict')
        except:
            args = {}
            payload['error'] += (
                "Default params is not valid, string has to evaluate as "
                "a Python dictionary. ")

        request_dict = {k: request.args.get(k) for k in request.args}
        args.update(request_dict)
        args['macros'] = macros
        sandbox = ImmutableSandboxedEnvironment()
        sql = sandbox.from_string(chart.sql).render(**args)
        label = sandbox.from_string(chart.label).render(**args)
        payload['sql_html'] = Markup(highlight(
            sql,
            lexers.SqlLexer(),  # Lexer call
            HtmlFormatter(noclasses=True))
        )
        payload['label'] = label

        pd.set_option('display.max_colwidth', 100)
        hook = db.get_hook()
        try:
            df = hook.get_pandas_df(
                wwwutils.limit_sql(sql, CHART_LIMIT, conn_type=db.conn_type))
            df = df.fillna(0)
        except Exception as e:
            payload['error'] += "SQL execution failed. Details: " + str(e)

        if csv:
            return Response(
                response=df.to_csv(index=False),
                status=200,
                mimetype="application/text")

        if not payload['error'] and len(df) == CHART_LIMIT:
            payload['warning'] = (
                "Data has been truncated to {0}"
                " rows. Expect incomplete results.").format(CHART_LIMIT)

        if not payload['error'] and len(df) == 0:
            payload['error'] += "Empty result set. "
        elif (
                not payload['error'] and
                chart.sql_layout == 'series' and
                chart.chart_type != "datatable" and
                len(df.columns) < 3):
            payload['error'] += "SQL needs to return at least 3 columns. "
        elif (
                not payload['error'] and
                chart.sql_layout == 'columns'and
                len(df.columns) < 2):
            payload['error'] += "SQL needs to return at least 2 columns. "
        elif not payload['error']:
            import numpy as np
            chart_type = chart.chart_type

            data = None
            if chart.show_datatable or chart_type == "datatable":
                data = df.to_dict(orient="split")
                data['columns'] = [{'title': c} for c in data['columns']]
                payload['data'] = data

            # Trying to convert time to something Highcharts likes
            x_col = 1 if chart.sql_layout == 'series' else 0
            if chart.x_is_date:
                try:
                    # From string to datetime
                    df[df.columns[x_col]] = pd.to_datetime(
                        df[df.columns[x_col]])
                    df[df.columns[x_col]] = df[df.columns[x_col]].apply(
                        lambda x: int(x.strftime("%s")) * 1000)
                except Exception as e:
                    payload['error'] = "Time conversion failed"

            if chart_type == 'datatable':
                payload['state'] = 'SUCCESS'
                return wwwutils.json_response(payload)
            else:
                if chart.sql_layout == 'series':
                    # User provides columns (series, x, y)
                    xaxis_label = df.columns[1]
                    yaxis_label = df.columns[2]
                    df[df.columns[2]] = df[df.columns[2]].astype(np.float)
                    df = df.pivot_table(
                        index=df.columns[1],
                        columns=df.columns[0],
                        values=df.columns[2], aggfunc=np.sum)
                else:
                    # User provides columns (x, y, metric1, metric2, ...)
                    xaxis_label = df.columns[0]
                    yaxis_label = 'y'
                    df.index = df[df.columns[0]]
                    df = df.sort(df.columns[0])
                    del df[df.columns[0]]
                    for col in df.columns:
                        df[col] = df[col].astype(np.float)

                df = df.fillna(0)
                NVd3ChartClass = chart_mapping.get(chart.chart_type)
                NVd3ChartClass = getattr(nvd3, NVd3ChartClass)
                nvd3_chart = NVd3ChartClass(x_is_date=chart.x_is_date)

                for col in df.columns:
                    nvd3_chart.add_serie(name=col, y=df[col].tolist(), x=df[col].index.tolist())
                try:
                    nvd3_chart.buildcontent()
                    payload['chart_type'] = nvd3_chart.__class__.__name__
                    payload['htmlcontent'] = nvd3_chart.htmlcontent
                except Exception as e:
                    payload['error'] = str(e)

            payload['state'] = 'SUCCESS'
            payload['request_dict'] = request_dict
        return wwwutils.json_response(payload)

    @expose('/chart')
    @data_profiling_required
    def chart(self):
        session = settings.Session()
        chart_id = request.args.get('chart_id')
        embed = request.args.get('embed')
        chart = session.query(models.Chart).filter_by(id=chart_id).first()
        session.expunge_all()
        session.commit()
        session.close()

        NVd3ChartClass = chart_mapping.get(chart.chart_type)
        if not NVd3ChartClass:
            flash(
                "Not supported anymore as the license was incompatible, "
                "sorry",
                "danger")
            redirect('/admin/chart/')

        sql = ""
        if chart.show_sql:
            sql = Markup(highlight(
                chart.sql,
                lexers.SqlLexer(),  # Lexer call
                HtmlFormatter(noclasses=True))
            )
        return self.render(
            'airflow/nvd3.html',
            chart=chart,
            title="Airflow - Chart",
            sql=sql,
            label=chart.label,
            embed=embed)

    @expose('/dag_stats')
    def dag_stats(self):
        ds = models.DagStat
        session = Session()

        qry = (
            session.query(ds.dag_id, ds.state, ds.count)
        )

        data = {}
        for dag_id, state, count in qry:
            if dag_id not in data:
                data[dag_id] = {}
            data[dag_id][state] = count

        payload = {}
        for dag in dagbag.dags.values():
            payload[dag.safe_dag_id] = []
            for state in State.dag_states:
                try:
                    count = data[dag.dag_id][state]
                except Exception:
                    count = 0
                d = {
                    'state': state,
                    'count': count,
                    'dag_id': dag.dag_id,
                    'color': State.color(state)
                }
                payload[dag.safe_dag_id].append(d)
        return wwwutils.json_response(payload)

    @expose('/task_stats')
    def task_stats(self):
        TI = models.TaskInstance
        DagRun = models.DagRun
        Dag = models.DagModel
        session = Session()

        LastDagRun = (
            session.query(DagRun.dag_id, sqla.func.max(DagRun.execution_date).label('execution_date'))
            .join(Dag, Dag.dag_id == DagRun.dag_id)
            .filter(DagRun.state != State.RUNNING)
            .filter(Dag.is_active == True)
            .group_by(DagRun.dag_id)
            .subquery('last_dag_run')
        )
        RunningDagRun = (
            session.query(DagRun.dag_id, DagRun.execution_date)
            .join(Dag, Dag.dag_id == DagRun.dag_id)
            .filter(DagRun.state == State.RUNNING)
            .filter(Dag.is_active == True)
            .subquery('running_dag_run')
        )

        # Select all task_instances from active dag_runs.
        # If no dag_run is active, return task instances from most recent dag_run.
        LastTI = (
            session.query(TI.dag_id.label('dag_id'), TI.state.label('state'))
            .join(LastDagRun, and_(
                LastDagRun.c.dag_id == TI.dag_id,
                LastDagRun.c.execution_date == TI.execution_date))
        )
        RunningTI = (
            session.query(TI.dag_id.label('dag_id'), TI.state.label('state'))
            .join(RunningDagRun, and_(
                RunningDagRun.c.dag_id == TI.dag_id,
                RunningDagRun.c.execution_date == TI.execution_date))
        )

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
        session.commit()
        session.close()

        payload = {}
        for dag in dagbag.dags.values():
            payload[dag.safe_dag_id] = []
            for state in State.task_states:
                try:
                    count = data[dag.dag_id][state]
                except:
                    count = 0
                d = {
                    'state': state,
                    'count': count,
                    'dag_id': dag.dag_id,
                    'color': State.color(state)
                }
                payload[dag.safe_dag_id].append(d)
        return wwwutils.json_response(payload)

    @expose('/code')
    @login_required
    def code(self):
        dag_id = request.args.get('dag_id')
        dag = dagbag.get_dag(dag_id)
        title = dag_id
        try:
            with open(dag.fileloc, 'r') as f:
                code = f.read()
            html_code = highlight(
                code, lexers.PythonLexer(), HtmlFormatter(linenos=True))
        except IOError as e:
            html_code = str(e)

        return self.render(
            'airflow/dag_code.html', html_code=html_code, dag=dag, title=title,
            root=request.args.get('root'),
            demo_mode=conf.getboolean('webserver', 'demo_mode'))

    @expose('/dag_details')
    @login_required
    def dag_details(self):
        dag_id = request.args.get('dag_id')
        dag = dagbag.get_dag(dag_id)
        title = "DAG details"

        session = settings.Session()
        TI = models.TaskInstance
        states = (
            session.query(TI.state, sqla.func.count(TI.dag_id))
            .filter(TI.dag_id == dag_id)
            .group_by(TI.state)
            .all()
        )
        return self.render(
            'airflow/dag_details.html',
            dag=dag, title=title, states=states, State=State)

    @current_app.errorhandler(404)
    def circles(self):
        return render_template(
            'airflow/circles.html', hostname=socket.getfqdn()), 404

    @current_app.errorhandler(500)
    def show_traceback(self):
        from airflow.utils import asciiart as ascii_
        return render_template(
            'airflow/traceback.html',
            hostname=socket.getfqdn(),
            nukular=ascii_.nukular,
            info=traceback.format_exc()), 500

    @expose('/noaccess')
    def noaccess(self):
        return self.render('airflow/noaccess.html')

    @expose('/headers')
    def headers(self):
        d = {
            'headers': {k: v for k, v in request.headers},
        }
        if hasattr(current_user, 'is_superuser'):
            d['is_superuser'] = current_user.is_superuser()
            d['data_profiling'] = current_user.data_profiling()
            d['is_anonymous'] = current_user.is_anonymous()
            d['is_authenticated'] = current_user.is_authenticated()
        if hasattr(current_user, 'username'):
            d['username'] = current_user.username
        return wwwutils.json_response(d)

    @expose('/pickle_info')
    @login_required
    def pickle_info(self):
        d = {}
        dag_id = request.args.get('dag_id')
        dags = [dagbag.dags.get(dag_id)] if dag_id else dagbag.dags.values()
        for dag in dags:
            if not dag.is_subdag:
                d[dag.dag_id] = dag.pickle_info()
        return wwwutils.json_response(d)

    @expose('/login', methods=['GET', 'POST'])
    def login(self):
        return airflow.login.login(self, request)

    @expose('/logout')
    def logout(self):
        logout_user()
        flash('You have been logged out.')
        return redirect(url_for('admin.index'))

    @expose('/rendered')
    @login_required
    @wwwutils.action_logging
    def rendered(self):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        dttm = dateutil.parser.parse(execution_date)
        form = DateTimeForm(data={'execution_date': dttm})
        dag = dagbag.get_dag(dag_id)
        task = copy.copy(dag.get_task(task_id))
        ti = models.TaskInstance(task=task, execution_date=dttm)
        try:
            ti.render_templates()
        except Exception as e:
            flash("Error rendering template: " + str(e), "error")
        title = "Rendered Template"
        html_dict = {}
        for template_field in task.__class__.template_fields:
            content = getattr(task, template_field)
            if template_field in attr_renderer:
                html_dict[template_field] = attr_renderer[template_field](content)
            else:
                html_dict[template_field] = (
                    "<pre><code>" + str(content) + "</pre></code>")

        return self.render(
            'airflow/ti_code.html',
            html_dict=html_dict,
            dag=dag,
            task_id=task_id,
            execution_date=execution_date,
            form=form,
            title=title,)

    @expose('/log')
    @login_required
    @wwwutils.action_logging
    def log(self):
        BASE_LOG_FOLDER = os.path.expanduser(
            conf.get('core', 'BASE_LOG_FOLDER'))
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        dag = dagbag.get_dag(dag_id)
        log_relative = "{dag_id}/{task_id}/{execution_date}".format(
            **locals())
        loc = os.path.join(BASE_LOG_FOLDER, log_relative)
        loc = loc.format(**locals())
        log = ""
        TI = models.TaskInstance
        dttm = dateutil.parser.parse(execution_date)
        form = DateTimeForm(data={'execution_date': dttm})
        session = Session()
        ti = session.query(TI).filter(
            TI.dag_id == dag_id, TI.task_id == task_id,
            TI.execution_date == dttm).first()

        if ti is None:
            log = "*** Task instance did not exist in the DB\n"
        else:
            # load remote logs
            remote_log_base = conf.get('core', 'REMOTE_BASE_LOG_FOLDER')
            remote_log_loaded = False
            if remote_log_base:
                remote_log_path = os.path.join(remote_log_base, log_relative)
                remote_log = ""

                # Only display errors reading the log if the task completed or ran at least
                # once before (otherwise there won't be any remote log stored).
                ti_execution_completed = ti.state in {State.SUCCESS, State.FAILED}
                ti_ran_more_than_once = ti.try_number > 1
                surface_log_retrieval_errors = (
                    ti_execution_completed or ti_ran_more_than_once)

                # S3
                if remote_log_path.startswith('s3:/'):
                    remote_log += log_utils.S3Log().read(
                        remote_log_path, return_error=surface_log_retrieval_errors)
                    remote_log_loaded = True
                # GCS
                elif remote_log_path.startswith('gs:/'):
                    remote_log += log_utils.GCSLog().read(
                        remote_log_path, return_error=surface_log_retrieval_errors)
                    remote_log_loaded = True
                # unsupported
                else:
                    remote_log += '*** Unsupported remote log location.'

                if remote_log:
                    log += ('*** Reading remote log from {}.\n{}\n'.format(
                        remote_log_path, remote_log))

            # We only want to display the
            # local logs while the task is running if a remote log configuration is set up
            # since the logs will be transfered there after the run completes.
            # TODO(aoen): One problem here is that if a task is running on a worker it
            # already ran on, then duplicate logs will be printed for all of the previous
            # runs of the task that already completed since they will have been printed as
            # part of the remote log section above. This can be fixed either by streaming
            # logs to the log servers as tasks are running, or by creating a proper
            # abstraction for multiple task instance runs).
            if not remote_log_loaded or ti.state == State.RUNNING:
                if os.path.exists(loc):
                    try:
                        f = open(loc)
                        log += "*** Reading local log.\n" + "".join(f.readlines())
                        f.close()
                    except:
                        log = "*** Failed to load local log file: {0}.\n".format(loc)
                else:
                    WORKER_LOG_SERVER_PORT = \
                        conf.get('celery', 'WORKER_LOG_SERVER_PORT')
                    url = os.path.join(
                        "http://{ti.hostname}:{WORKER_LOG_SERVER_PORT}/log", log_relative
                    ).format(**locals())
                    log += "*** Log file isn't local.\n"
                    log += "*** Fetching here: {url}\n".format(**locals())
                    try:
                        import requests
                        timeout = None  # No timeout
                        try:
                            timeout = conf.getint('webserver', 'log_fetch_timeout_sec')
                        except (AirflowConfigException, ValueError):
                            pass

                        response = requests.get(url, timeout=timeout)
                        response.raise_for_status()
                        log += '\n' + response.text
                    except:
                        log += "*** Failed to fetch log file from worker.\n".format(
                            **locals())

        if PY2 and not isinstance(log, unicode):
            log = log.decode('utf-8')

        return self.render(
            'airflow/ti_code.html',
            code=log, dag=dag, title="Log", task_id=task_id,
            execution_date=execution_date, form=form)

    @expose('/task')
    @login_required
    @wwwutils.action_logging
    def task(self):
        TI = models.TaskInstance

        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        # Carrying execution_date through, even though it's irrelevant for
        # this context
        execution_date = request.args.get('execution_date')
        dttm = dateutil.parser.parse(execution_date)
        form = DateTimeForm(data={'execution_date': dttm})
        dag = dagbag.get_dag(dag_id)

        if not dag or task_id not in dag.task_ids:
            flash(
                "Task [{}.{}] doesn't seem to exist"
                " at the moment".format(dag_id, task_id),
                "error")
            return redirect('/admin/')
        task = copy.copy(dag.get_task(task_id))
        task.resolve_template_files()
        ti = TI(task=task, execution_date=dttm)
        ti.refresh_from_db()

        ti_attrs = []
        for attr_name in dir(ti):
            if not attr_name.startswith('_'):
                attr = getattr(ti, attr_name)
                if type(attr) != type(self.task):
                    ti_attrs.append((attr_name, str(attr)))

        task_attrs = []
        for attr_name in dir(task):
            if not attr_name.startswith('_'):
                attr = getattr(task, attr_name)
                if type(attr) != type(self.task) and \
                        attr_name not in attr_renderer:
                    task_attrs.append((attr_name, str(attr)))

        # Color coding the special attributes that are code
        special_attrs_rendered = {}
        for attr_name in attr_renderer:
            if hasattr(task, attr_name):
                source = getattr(task, attr_name)
                special_attrs_rendered[attr_name] = attr_renderer[attr_name](source)

        no_failed_deps_result = [(
            "Unknown",
            dedent("""\
            All dependencies are met but the task instance is not running. In most cases this just means that the task will probably be scheduled soon unless:<br/>
            - The scheduler is down or under heavy load<br/>
            {}
            <br/>
            If this task instance does not start soon please contact your Airflow administrator for assistance."""
                   .format(
                       "- This task instance already ran and had it's state changed manually (e.g. cleared in the UI)<br/>"
                       if ti.state == State.NONE else "")))]

        # Use the scheduler's context to figure out which dependencies are not met
        dep_context = DepContext(SCHEDULER_DEPS)
        failed_dep_reasons = [(dep.dep_name, dep.reason) for dep in
                              ti.get_failed_dep_statuses(
                                  dep_context=dep_context)]

        title = "Task Instance Details"
        return self.render(
            'airflow/task.html',
            task_attrs=task_attrs,
            ti_attrs=ti_attrs,
            failed_dep_reasons=failed_dep_reasons or no_failed_deps_result,
            task_id=task_id,
            execution_date=execution_date,
            special_attrs_rendered=special_attrs_rendered,
            form=form,
            dag=dag, title=title)

    @expose('/xcom')
    @login_required
    @wwwutils.action_logging
    def xcom(self):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        # Carrying execution_date through, even though it's irrelevant for
        # this context
        execution_date = request.args.get('execution_date')
        dttm = dateutil.parser.parse(execution_date)
        form = DateTimeForm(data={'execution_date': dttm})
        dag = dagbag.get_dag(dag_id)
        if not dag or task_id not in dag.task_ids:
            flash(
                "Task [{}.{}] doesn't seem to exist"
                " at the moment".format(dag_id, task_id),
                "error")
            return redirect('/admin/')

        session = Session()
        xcomlist = session.query(XCom).filter(
            XCom.dag_id == dag_id, XCom.task_id == task_id,
            XCom.execution_date == dttm).all()

        attributes = []
        for xcom in xcomlist:
            if not xcom.key.startswith('_'):
                attributes.append((xcom.key, xcom.value))

        title = "XCom"
        return self.render(
            'airflow/xcom.html',
            attributes=attributes,
            task_id=task_id,
            execution_date=execution_date,
            form=form,
            dag=dag, title=title)

    @expose('/run')
    @login_required
    @wwwutils.action_logging
    @wwwutils.notify_owner
    def run(self):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        origin = request.args.get('origin')
        dag = dagbag.get_dag(dag_id)
        task = dag.get_task(task_id)

        execution_date = request.args.get('execution_date')
        execution_date = dateutil.parser.parse(execution_date)
        ignore_all_deps = request.args.get('ignore_all_deps') == "true"
        ignore_task_deps = request.args.get('ignore_task_deps') == "true"
        ignore_ti_state = request.args.get('ignore_ti_state') == "true"

        try:
            from airflow.executors import DEFAULT_EXECUTOR as executor
            from airflow.executors import CeleryExecutor
            if not isinstance(executor, CeleryExecutor):
                flash("Only works with the CeleryExecutor, sorry", "error")
                return redirect(origin)
        except ImportError:
            # in case CeleryExecutor cannot be imported it is not active either
            flash("Only works with the CeleryExecutor, sorry", "error")
            return redirect(origin)

        ti = models.TaskInstance(task=task, execution_date=execution_date)
        ti.refresh_from_db()

        # Make sure the task instance can be queued
        dep_context = DepContext(
            deps=QUEUE_DEPS,
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

    @expose('/trigger')
    @login_required
    @wwwutils.action_logging
    @wwwutils.notify_owner
    def trigger(self):
        dag_id = request.args.get('dag_id')
        origin = request.args.get('origin') or "/admin/"
        dag = dagbag.get_dag(dag_id)

        if not dag:
            flash("Cannot find dag {}".format(dag_id))
            return redirect(origin)

        execution_date = datetime.now()
        run_id = "manual__{0}".format(execution_date.isoformat())

        dr = DagRun.find(dag_id=dag_id, run_id=run_id)
        if dr:
            flash("This run_id {} already exists".format(run_id))
            return redirect(origin)

        run_conf = {}

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

    @expose('/clear')
    @login_required
    @wwwutils.action_logging
    @wwwutils.notify_owner
    def clear(self):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        origin = request.args.get('origin')
        dag = dagbag.get_dag(dag_id)

        execution_date = request.args.get('execution_date')
        execution_date = dateutil.parser.parse(execution_date)
        confirmed = request.args.get('confirmed') == "true"
        upstream = request.args.get('upstream') == "true"
        downstream = request.args.get('downstream') == "true"
        future = request.args.get('future') == "true"
        past = request.args.get('past') == "true"
        recursive = request.args.get('recursive') == "true"

        dag = dag.sub_dag(
            task_regex=r"^{0}$".format(task_id),
            include_downstream=downstream,
            include_upstream=upstream)

        end_date = execution_date if not future else None
        start_date = execution_date if not past else None
        if confirmed:
            count = dag.clear(
                start_date=start_date,
                end_date=end_date,
                include_subdags=recursive)

            flash("{0} task instances have been cleared".format(count))
            return redirect(origin)
        else:
            tis = dag.clear(
                start_date=start_date,
                end_date=end_date,
                include_subdags=recursive,
                dry_run=True)
            if not tis:
                flash("No task instances to clear", 'error')
                response = redirect(origin)
            else:
                details = "\n".join([str(t) for t in tis])

                response = self.render(
                    'airflow/confirm.html',
                    message=(
                        "Here's the list of task instances you are about "
                        "to clear:"),
                    details=details,)

            return response

    @expose('/blocked')
    @login_required
    def blocked(self):
        session = settings.Session()
        DR = models.DagRun
        dags = (
            session.query(DR.dag_id, sqla.func.count(DR.id))
            .filter(DR.state == State.RUNNING)
            .group_by(DR.dag_id)
            .all()
        )
        payload = []
        for dag_id, active_dag_runs in dags:
            max_active_runs = 0
            if dag_id in dagbag.dags:
                max_active_runs = dagbag.dags[dag_id].max_active_runs
            payload.append({
                'dag_id': dag_id,
                'active_dag_run': active_dag_runs,
                'max_active_runs': max_active_runs,
            })
        return wwwutils.json_response(payload)

    @expose('/success')
    @login_required
    @wwwutils.action_logging
    @wwwutils.notify_owner
    def success(self):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        origin = request.args.get('origin')
        dag = dagbag.get_dag(dag_id)
        task = dag.get_task(task_id)
        task.dag = dag

        execution_date = request.args.get('execution_date')
        execution_date = dateutil.parser.parse(execution_date)
        confirmed = request.args.get('confirmed') == "true"
        upstream = request.args.get('upstream') == "true"
        downstream = request.args.get('downstream') == "true"
        future = request.args.get('future') == "true"
        past = request.args.get('past') == "true"

        if not dag:
            flash("Cannot find DAG: {}".format(dag_id))
            return redirect(origin)

        if not task:
            flash("Cannot find task {} in DAG {}".format(task_id, dag.dag_id))
            return redirect(origin)

        from airflow.api.common.experimental.mark_tasks import set_state

        if confirmed:
            altered = set_state(task=task, execution_date=execution_date,
                                upstream=upstream, downstream=downstream,
                                future=future, past=past, state=State.SUCCESS,
                                commit=True)

            flash("Marked success on {} task instances".format(len(altered)))
            return redirect(origin)

        to_be_altered = set_state(task=task, execution_date=execution_date,
                                  upstream=upstream, downstream=downstream,
                                  future=future, past=past, state=State.SUCCESS,
                                  commit=False)

        details = "\n".join([str(t) for t in to_be_altered])

        response = self.render("airflow/confirm.html",
                               message=("Here's the list of task instances you are "
                                        "about to mark as successful:"),
                               details=details)

        return response

    @expose('/tree')
    @login_required
    @wwwutils.gzipped
    @wwwutils.action_logging
    def tree(self):
        dag_id = request.args.get('dag_id')
        blur = conf.getboolean('webserver', 'demo_mode')
        dag = dagbag.get_dag(dag_id)
        root = request.args.get('root')
        if root:
            dag = dag.sub_dag(
                task_regex=root,
                include_downstream=False,
                include_upstream=True)

        session = settings.Session()

        base_date = request.args.get('base_date')
        num_runs = request.args.get('num_runs')
        num_runs = int(num_runs) if num_runs else 25

        if base_date:
            base_date = dateutil.parser.parse(base_date)
        else:
            base_date = dag.latest_execution_date or datetime.now()

        dates = dag.date_range(base_date, num=-abs(num_runs))
        min_date = dates[0] if dates else datetime(2000, 1, 1)

        DR = models.DagRun
        dag_runs = (
            session.query(DR)
            .filter(
                DR.dag_id==dag.dag_id,
                DR.execution_date<=base_date,
                DR.execution_date>=min_date)
            .all()
        )
        dag_runs = {
            dr.execution_date: alchemy_to_dict(dr) for dr in dag_runs}

        dates = sorted(list(dag_runs.keys()))
        max_date = max(dates) if dates else None

        tis = dag.get_task_instances(
            session, start_date=min_date, end_date=base_date)
        task_instances = {}
        for ti in tis:
            tid = alchemy_to_dict(ti)
            dr = dag_runs.get(ti.execution_date)
            tid['external_trigger'] = dr['external_trigger'] if dr else False
            task_instances[(ti.task_id, ti.execution_date)] = tid

        expanded = []
        # The default recursion traces every path so that tree view has full
        # expand/collapse functionality. After 5,000 nodes we stop and fall
        # back on a quick DFS search for performance. See PR #320.
        node_count = [0]
        node_limit = 5000 / max(1, len(dag.roots))

        def recurse_nodes(task, visited):
            visited.add(task)
            node_count[0] += 1

            children = [
                recurse_nodes(t, visited) for t in task.upstream_list
                if node_count[0] < node_limit or t not in visited]

            # D3 tree uses children vs _children to define what is
            # expanded or not. The following block makes it such that
            # repeated nodes are collapsed by default.
            children_key = 'children'
            if task.task_id not in expanded:
                expanded.append(task.task_id)
            elif children:
                children_key = "_children"

            def set_duration(tid):
                if (isinstance(tid, dict) and tid.get("state") == State.RUNNING and
                        tid["start_date"] is not None):
                    d = datetime.now() - dateutil.parser.parse(tid["start_date"])
                    tid["duration"] = d.total_seconds()
                return tid

            return {
                'name': task.task_id,
                'instances': [
                    set_duration(task_instances.get((task.task_id, d))) or {
                        'execution_date': d.isoformat(),
                        'task_id': task.task_id
                    }
                    for d in dates],
                children_key: children,
                'num_dep': len(task.upstream_list),
                'operator': task.task_type,
                'retries': task.retries,
                'owner': task.owner,
                'start_date': task.start_date,
                'end_date': task.end_date,
                'depends_on_past': task.depends_on_past,
                'ui_color': task.ui_color,
            }
        data = {
            'name': '[DAG]',
            'children': [recurse_nodes(t, set()) for t in dag.roots],
            'instances': [
                dag_runs.get(d) or {'execution_date': d.isoformat()}
                for d in dates],
        }

        data = json.dumps(data, indent=4, default=json_ser)
        session.commit()
        session.close()

        form = DateTimeWithNumRunsForm(data={'base_date': max_date,
                                             'num_runs': num_runs})
        return self.render(
            'airflow/tree.html',
            operators=sorted(
                list(set([op.__class__ for op in dag.tasks])),
                key=lambda x: x.__name__
            ),
            root=root,
            form=form,
            dag=dag, data=data, blur=blur)

    @expose('/graph')
    @login_required
    @wwwutils.gzipped
    @wwwutils.action_logging
    def graph(self):
        session = settings.Session()
        dag_id = request.args.get('dag_id')
        blur = conf.getboolean('webserver', 'demo_mode')
        dag = dagbag.get_dag(dag_id)
        if dag_id not in dagbag.dags:
            flash('DAG "{0}" seems to be missing.'.format(dag_id), "error")
            return redirect('/admin/')

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
                }
            })

        def get_upstream(task):
            for t in task.upstream_list:
                edge = {
                    'u': t.task_id,
                    'v': task.task_id,
                }
                if edge not in edges:
                    edges.append(edge)
                    get_upstream(t)

        for t in dag.roots:
            get_upstream(t)

        dttm = request.args.get('execution_date')
        if dttm:
            dttm = dateutil.parser.parse(dttm)
        else:
            dttm = dag.latest_execution_date or datetime.now().date()

        DR = models.DagRun
        drs = (
            session.query(DR)
            .filter_by(dag_id=dag_id)
            .order_by(desc(DR.execution_date)).all()
        )
        dr_choices = []
        dr_state = None
        for dr in drs:
            dr_choices.append((dr.execution_date.isoformat(), dr.run_id))
            if dttm == dr.execution_date:
                dr_state = dr.state

        class GraphForm(Form):
            execution_date = SelectField("DAG run", choices=dr_choices)
            arrange = SelectField("Layout", choices=(
                ('LR', "Left->Right"),
                ('RL', "Right->Left"),
                ('TB', "Top->Bottom"),
                ('BT', "Bottom->Top"),
            ))
        form = GraphForm(
            data={'execution_date': dttm.isoformat(), 'arrange': arrange})

        task_instances = {
            ti.task_id: alchemy_to_dict(ti)
            for ti in dag.get_task_instances(session, dttm, dttm)}
        tasks = {
            t.task_id: {
                'dag_id': t.dag_id,
                'task_type': t.task_type,
            }
            for t in dag.tasks}
        if not tasks:
            flash("No tasks found", "error")
        session.commit()
        session.close()
        doc_md = markdown.markdown(dag.doc_md) if hasattr(dag, 'doc_md') and dag.doc_md else ''

        return self.render(
            'airflow/graph.html',
            dag=dag,
            form=form,
            width=request.args.get('width', "100%"),
            height=request.args.get('height', "800"),
            execution_date=dttm.isoformat(),
            state_token=state_token(dr_state),
            doc_md=doc_md,
            arrange=arrange,
            operators=sorted(
                list(set([op.__class__ for op in dag.tasks])),
                key=lambda x: x.__name__
            ),
            blur=blur,
            root=root or '',
            task_instances=json.dumps(task_instances, indent=2),
            tasks=json.dumps(tasks, indent=2),
            nodes=json.dumps(nodes, indent=2),
            edges=json.dumps(edges, indent=2),)

    @expose('/duration')
    @login_required
    @wwwutils.action_logging
    def duration(self):
        session = settings.Session()
        dag_id = request.args.get('dag_id')
        dag = dagbag.get_dag(dag_id)
        base_date = request.args.get('base_date')
        num_runs = request.args.get('num_runs')
        num_runs = int(num_runs) if num_runs else 25

        if base_date:
            base_date = dateutil.parser.parse(base_date)
        else:
            base_date = dag.latest_execution_date or datetime.now()

        dates = dag.date_range(base_date, num=-abs(num_runs))
        min_date = dates[0] if dates else datetime(2000, 1, 1)

        root = request.args.get('root')
        if root:
            dag = dag.sub_dag(
                task_regex=root,
                include_upstream=True,
                include_downstream=False)

        chart = nvd3.lineChart(
            name="lineChart", x_is_date=True, height=600, width="1200")
        cum_chart = nvd3.lineChart(
            name="cumLineChart", x_is_date=True, height=600, width="1200")

        y = {}
        x = {}
        cum_y = {}
        for task in dag.tasks:
            y[task.task_id] = []
            x[task.task_id] = []
            cum_y[task.task_id] = []
            for ti in task.get_task_instances(session, start_date=min_date,
                                              end_date=base_date):
                if ti.duration:
                    dttm = wwwutils.epoch(ti.execution_date)
                    x[ti.task_id].append(dttm)
                    y[ti.task_id].append(float(ti.duration))
                    fails = session.query(models.TaskFail).filter_by(
                        task_id=ti.task_id,
                        dag_id=ti.dag_id,
                        execution_date=ti.execution_date).all()
                    fails_total = sum([f.duration for f in fails])
                    cum_y[ti.task_id].append(float(ti.duration + fails_total))
        # determine the most relevant time unit for the set of task instance
        # durations for the DAG
        y_unit = infer_time_unit([d for t in y.values() for d in t])
        cum_y_unit = infer_time_unit([d for t in cum_y.values() for d in t])
        # update the y Axis on both charts to have the correct time units
        chart.create_y_axis('yAxis', format='.02f', custom_format=False,
                            label='Duration ({})'.format(y_unit))
        cum_chart.create_y_axis('yAxis', format='.02f', custom_format=False,
                                label='Duration ({})'.format(cum_y_unit))
        for task in dag.tasks:
            if x[task.task_id]:
                chart.add_serie(name=task.task_id, x=x[task.task_id],
                                y=scale_time_units(y[task.task_id], y_unit))
                cum_chart.add_serie(name=task.task_id, x=x[task.task_id],
                                    y=scale_time_units(cum_y[task.task_id],
                                    cum_y_unit))

        tis = dag.get_task_instances(
            session, start_date=min_date, end_date=base_date)
        dates = sorted(list({ti.execution_date for ti in tis}))
        max_date = max([ti.execution_date for ti in tis]) if dates else None

        session.commit()
        session.close()

        form = DateTimeWithNumRunsForm(data={'base_date': max_date,
                                             'num_runs': num_runs})
        chart.buildcontent()
        cum_chart.buildcontent()
        s_index = cum_chart.htmlcontent.rfind('});')
        cum_chart.htmlcontent = (cum_chart.htmlcontent[:s_index] +
                                 "$( document ).trigger('chartload')" +
                                 cum_chart.htmlcontent[s_index:])

        return self.render(
            'airflow/duration_chart.html',
            dag=dag,
            demo_mode=conf.getboolean('webserver', 'demo_mode'),
            root=root,
            form=form,
            chart=chart.htmlcontent,
            cum_chart=cum_chart.htmlcontent
        )

    @expose('/tries')
    @login_required
    @wwwutils.action_logging
    def tries(self):
        session = settings.Session()
        dag_id = request.args.get('dag_id')
        dag = dagbag.get_dag(dag_id)
        base_date = request.args.get('base_date')
        num_runs = request.args.get('num_runs')
        num_runs = int(num_runs) if num_runs else 25

        if base_date:
            base_date = dateutil.parser.parse(base_date)
        else:
            base_date = dag.latest_execution_date or datetime.now()

        dates = dag.date_range(base_date, num=-abs(num_runs))
        min_date = dates[0] if dates else datetime(2000, 1, 1)

        root = request.args.get('root')
        if root:
            dag = dag.sub_dag(
                task_regex=root,
                include_upstream=True,
                include_downstream=False)

        chart = nvd3.lineChart(
            name="lineChart", x_is_date=True, y_axis_format='d', height=600, width="1200")

        for task in dag.tasks:
            y = []
            x = []
            for ti in task.get_task_instances(session, start_date=min_date,
                                              end_date=base_date):
                dttm = wwwutils.epoch(ti.execution_date)
                x.append(dttm)
                y.append(ti.try_number)
            if x:
                chart.add_serie(name=task.task_id, x=x, y=y)

        tis = dag.get_task_instances(
            session, start_date=min_date, end_date=base_date)
        tries = sorted(list({ti.try_number for ti in tis}))
        max_date = max([ti.execution_date for ti in tis]) if tries else None

        session.commit()
        session.close()

        form = DateTimeWithNumRunsForm(data={'base_date': max_date,
                                             'num_runs': num_runs})

        chart.buildcontent()

        return self.render(
            'airflow/chart.html',
            dag=dag,
            demo_mode=conf.getboolean('webserver', 'demo_mode'),
            root=root,
            form=form,
            chart=chart.htmlcontent
        )

    @expose('/landing_times')
    @login_required
    @wwwutils.action_logging
    def landing_times(self):
        session = settings.Session()
        dag_id = request.args.get('dag_id')
        dag = dagbag.get_dag(dag_id)
        base_date = request.args.get('base_date')
        num_runs = request.args.get('num_runs')
        num_runs = int(num_runs) if num_runs else 25

        if base_date:
            base_date = dateutil.parser.parse(base_date)
        else:
            base_date = dag.latest_execution_date or datetime.now()

        dates = dag.date_range(base_date, num=-abs(num_runs))
        min_date = dates[0] if dates else datetime(2000, 1, 1)

        root = request.args.get('root')
        if root:
            dag = dag.sub_dag(
                task_regex=root,
                include_upstream=True,
                include_downstream=False)

        chart = nvd3.lineChart(
            name="lineChart", x_is_date=True, height=600, width="1200")
        y = {}
        x = {}
        for task in dag.tasks:
            y[task.task_id] = []
            x[task.task_id] = []
            for ti in task.get_task_instances(session, start_date=min_date,
                                              end_date=base_date):
                ts = ti.execution_date
                if dag.schedule_interval:
                    ts = dag.following_schedule(ts)
                if ti.end_date:
                    dttm = wwwutils.epoch(ti.execution_date)
                    secs = (ti.end_date - ts).total_seconds()
                    x[ti.task_id].append(dttm)
                    y[ti.task_id].append(secs)

        # determine the most relevant time unit for the set of landing times
        # for the DAG
        y_unit = infer_time_unit([d for t in y.values() for d in t])
        # update the y Axis to have the correct time units
        chart.create_y_axis('yAxis', format='.02f', custom_format=False,
                            label='Landing Time ({})'.format(y_unit))
        for task in dag.tasks:
            if x[task.task_id]:
                chart.add_serie(name=task.task_id, x=x[task.task_id],
                                y=scale_time_units(y[task.task_id], y_unit))

        tis = dag.get_task_instances(
            session, start_date=min_date, end_date=base_date)
        dates = sorted(list({ti.execution_date for ti in tis}))
        max_date = max([ti.execution_date for ti in tis]) if dates else None

        session.commit()
        session.close()

        form = DateTimeWithNumRunsForm(data={'base_date': max_date,
                                             'num_runs': num_runs})
        chart.buildcontent()
        return self.render(
            'airflow/chart.html',
            dag=dag,
            chart=chart.htmlcontent,
            height="700px",
            demo_mode=conf.getboolean('webserver', 'demo_mode'),
            root=root,
            form=form,
        )

    @expose('/paused', methods=['POST'])
    @login_required
    @wwwutils.action_logging
    def paused(self):
        DagModel = models.DagModel
        dag_id = request.args.get('dag_id')
        session = settings.Session()
        orm_dag = session.query(
            DagModel).filter(DagModel.dag_id == dag_id).first()
        if request.args.get('is_paused') == 'false':
            orm_dag.is_paused = True
        else:
            orm_dag.is_paused = False
        session.merge(orm_dag)
        session.commit()
        session.close()

        dagbag.get_dag(dag_id)
        return "OK"

    @expose('/refresh')
    @login_required
    @wwwutils.action_logging
    def refresh(self):
        DagModel = models.DagModel
        dag_id = request.args.get('dag_id')
        session = settings.Session()
        orm_dag = session.query(
            DagModel).filter(DagModel.dag_id == dag_id).first()

        if orm_dag:
            orm_dag.last_expired = datetime.now()
            session.merge(orm_dag)
        session.commit()
        session.close()

        dagbag.get_dag(dag_id)
        flash("DAG [{}] is now fresh as a daisy".format(dag_id))
        return redirect(request.referrer)

    @expose('/refresh_all')
    @login_required
    @wwwutils.action_logging
    def refresh_all(self):
        dagbag.collect_dags(only_if_updated=False)
        flash("All DAGs are now up to date")
        return redirect('/')

    @expose('/gantt')
    @login_required
    @wwwutils.action_logging
    def gantt(self):
        session = settings.Session()
        dag_id = request.args.get('dag_id')
        dag = dagbag.get_dag(dag_id)
        demo_mode = conf.getboolean('webserver', 'demo_mode')

        root = request.args.get('root')
        if root:
            dag = dag.sub_dag(
                task_regex=root,
                include_upstream=True,
                include_downstream=False)

        dttm = request.args.get('execution_date')
        if dttm:
            dttm = dateutil.parser.parse(dttm)
        else:
            dttm = dag.latest_execution_date or datetime.now().date()

        form = DateTimeForm(data={'execution_date': dttm})

        tis = [
            ti for ti in dag.get_task_instances(session, dttm, dttm)
            if ti.start_date]
        tis = sorted(tis, key=lambda ti: ti.start_date)

        tasks = []
        for ti in tis:
            end_date = ti.end_date if ti.end_date else datetime.now()
            tasks.append({
                'startDate': wwwutils.epoch(ti.start_date),
                'endDate': wwwutils.epoch(end_date),
                'isoStart': ti.start_date.isoformat()[:-4],
                'isoEnd': end_date.isoformat()[:-4],
                'taskName': ti.task_id,
                'duration': "{}".format(end_date - ti.start_date)[:-4],
                'status': ti.state,
                'executionDate': ti.execution_date.isoformat(),
            })
        states = {ti.state: ti.state for ti in tis}
        data = {
            'taskNames': [ti.task_id for ti in tis],
            'tasks': tasks,
            'taskStatus': states,
            'height': len(tis) * 25,
        }

        session.commit()
        session.close()

        return self.render(
            'airflow/gantt.html',
            dag=dag,
            execution_date=dttm.isoformat(),
            form=form,
            data=json.dumps(data, indent=2),
            base_date='',
            demo_mode=demo_mode,
            root=root,
        )

    @expose('/object/task_instances')
    @login_required
    @wwwutils.action_logging
    def task_instances(self):
        session = settings.Session()
        dag_id = request.args.get('dag_id')
        dag = dagbag.get_dag(dag_id)

        dttm = request.args.get('execution_date')
        if dttm:
            dttm = dateutil.parser.parse(dttm)
        else:
            return ("Error: Invalid execution_date")

        task_instances = {
            ti.task_id: alchemy_to_dict(ti)
            for ti in dag.get_task_instances(session, dttm, dttm)}

        return json.dumps(task_instances)

    @expose('/variables/<form>', methods=["GET", "POST"])
    @login_required
    @wwwutils.action_logging
    def variables(self, form):
        try:
            if request.method == 'POST':
                data = request.json
                if data:
                    session = settings.Session()
                    var = models.Variable(key=form, val=json.dumps(data))
                    session.add(var)
                    session.commit()
                return ""
            else:
                return self.render(
                    'airflow/variables/{}.html'.format(form)
                )
        except:
            return ("Error: form airflow/variables/{}.html "
                    "not found.").format(form), 404

    @expose('/varimport', methods=["GET", "POST"])
    @login_required
    @wwwutils.action_logging
    def varimport(self):
        try:
            out = str(request.files['file'].read())
            d = json.loads(out)
        except Exception:
            flash("Missing file or syntax error.")
        else:
            for k, v in d.items():
                models.Variable.set(k, v, serialize_json=isinstance(v, dict))
            flash("{} variable(s) successfully updated.".format(len(d)))
        return redirect('/admin/variable')


class HomeView(AdminIndexView):
    @expose("/")
    @login_required
    def index(self):
        session = Session()
        DM = models.DagModel
        qry = None

        # restrict the dags shown if filter_by_owner and current user is not superuser
        do_filter = FILTER_BY_OWNER and (not current_user.is_superuser())
        owner_mode = conf.get('webserver', 'OWNER_MODE').strip().lower()

        hide_paused_dags_by_default = conf.getboolean('webserver',
                                                      'hide_paused_dags_by_default')
        show_paused_arg = request.args.get('showPaused', 'None')
        if show_paused_arg.strip().lower() == 'false':
            hide_paused = True

        elif show_paused_arg.strip().lower() == 'true':
            hide_paused = False

        else:
            hide_paused = hide_paused_dags_by_default

        # read orm_dags from the db
        qry = session.query(DM)
        qry_fltr = []

        if do_filter and owner_mode == 'ldapgroup':
            qry_fltr = qry.filter(
                ~DM.is_subdag, DM.is_active,
                DM.owners.in_(current_user.ldap_groups)
            ).all()
        elif do_filter and owner_mode == 'user':
            qry_fltr = qry.filter(
                ~DM.is_subdag, DM.is_active,
                DM.owners == current_user.user.username
            ).all()
        else:
            qry_fltr = qry.filter(
                ~DM.is_subdag, DM.is_active
            ).all()

        # optionally filter out "paused" dags
        if hide_paused:
            orm_dags = {dag.dag_id: dag for dag in qry_fltr if not dag.is_paused}

        else:
            orm_dags = {dag.dag_id: dag for dag in qry_fltr}

        import_errors = session.query(models.ImportError).all()
        for ie in import_errors:
            flash(
                "Broken DAG: [{ie.filename}] {ie.stacktrace}".format(ie=ie),
                "error")
        session.expunge_all()
        session.commit()
        session.close()

        # get a list of all non-subdag dags visible to everyone
        # optionally filter out "paused" dags
        if hide_paused:
            unfiltered_webserver_dags = [dag for dag in dagbag.dags.values() if
                                         not dag.parent_dag and not dag.is_paused]

        else:
            unfiltered_webserver_dags = [dag for dag in dagbag.dags.values() if
                                         not dag.parent_dag]

        # optionally filter to get only dags that the user should see
        if do_filter and owner_mode == 'ldapgroup':
            # only show dags owned by someone in @current_user.ldap_groups
            webserver_dags = {
                dag.dag_id: dag
                for dag in unfiltered_webserver_dags
                if dag.owner in current_user.ldap_groups
            }
        elif do_filter and owner_mode == 'user':
            # only show dags owned by @current_user.user.username
            webserver_dags = {
                dag.dag_id: dag
                for dag in unfiltered_webserver_dags
                if dag.owner == current_user.user.username
            }
        else:
            webserver_dags = {
                dag.dag_id: dag
                for dag in unfiltered_webserver_dags
            }

        all_dag_ids = sorted(set(orm_dags.keys()) | set(webserver_dags.keys()))
        return self.render(
            'airflow/dags.html',
            webserver_dags=webserver_dags,
            orm_dags=orm_dags,
            hide_paused=hide_paused,
            all_dag_ids=all_dag_ids)


class QueryView(wwwutils.DataProfilingMixin, BaseView):
    @expose('/', methods=['POST', 'GET'])
    @wwwutils.gzipped
    def query(self):
        session = settings.Session()
        dbs = session.query(models.Connection).order_by(
            models.Connection.conn_id).all()
        session.expunge_all()
        db_choices = list(
            ((db.conn_id, db.conn_id) for db in dbs if db.get_hook()))
        conn_id_str = request.form.get('conn_id')
        csv = request.form.get('csv') == "true"
        sql = request.form.get('sql')

        class QueryForm(Form):
            conn_id = SelectField("Layout", choices=db_choices)
            sql = TextAreaField("SQL", widget=wwwutils.AceEditorWidget())
        data = {
            'conn_id': conn_id_str,
            'sql': sql,
        }
        results = None
        has_data = False
        error = False
        if conn_id_str:
            db = [db for db in dbs if db.conn_id == conn_id_str][0]
            hook = db.get_hook()
            try:
                df = hook.get_pandas_df(wwwutils.limit_sql(sql, QUERY_LIMIT, conn_type=db.conn_type))
                # df = hook.get_pandas_df(sql)
                has_data = len(df) > 0
                df = df.fillna('')
                results = df.to_html(
                    classes=[
                        'table', 'table-bordered', 'table-striped', 'no-wrap'],
                    index=False,
                    na_rep='',
                ) if has_data else ''
            except Exception as e:
                flash(str(e), 'error')
                error = True

        if has_data and len(df) == QUERY_LIMIT:
            flash(
                "Query output truncated at " + str(QUERY_LIMIT) +
                " rows", 'info')

        if not has_data and error:
            flash('No data', 'error')

        if csv:
            return Response(
                response=df.to_csv(index=False),
                status=200,
                mimetype="application/text")

        form = QueryForm(request.form, data=data)
        session.commit()
        session.close()
        return self.render(
            'airflow/query.html', form=form,
            title="Ad Hoc Query",
            results=results or '',
            has_data=has_data)


class AirflowModelView(ModelView):
    list_template = 'airflow/model_list.html'
    edit_template = 'airflow/model_edit.html'
    create_template = 'airflow/model_create.html'
    column_display_actions = True
    page_size = 500


class ModelViewOnly(wwwutils.LoginMixin, AirflowModelView):
    """
    Modifying the base ModelView class for non edit, browse only operations
    """
    named_filter_urls = True
    can_create = False
    can_edit = False
    can_delete = False
    column_display_pk = True


class PoolModelView(wwwutils.SuperUserMixin, AirflowModelView):
    column_list = ('pool', 'slots', 'used_slots', 'queued_slots')
    column_formatters = dict(
        pool=pool_link, used_slots=fused_slots, queued_slots=fqueued_slots)
    named_filter_urls = True


class SlaMissModelView(wwwutils.SuperUserMixin, ModelViewOnly):
    verbose_name_plural = "SLA misses"
    verbose_name = "SLA miss"
    column_list = (
        'dag_id', 'task_id', 'execution_date', 'email_sent', 'timestamp')
    column_formatters = dict(
        task_id=task_instance_link,
        execution_date=datetime_f,
        timestamp=datetime_f,
        dag_id=dag_link)
    named_filter_urls = True
    column_searchable_list = ('dag_id', 'task_id',)
    column_filters = (
        'dag_id', 'task_id', 'email_sent', 'timestamp', 'execution_date')
    form_widget_args = {
        'email_sent': {'disabled': True},
        'timestamp': {'disabled': True},
    }


class ChartModelView(wwwutils.DataProfilingMixin, AirflowModelView):
    verbose_name = "chart"
    verbose_name_plural = "charts"
    form_columns = (
        'label',
        'owner',
        'conn_id',
        'chart_type',
        'show_datatable',
        'x_is_date',
        'y_log_scale',
        'show_sql',
        'height',
        'sql_layout',
        'sql',
        'default_params',)
    column_list = (
        'label', 'conn_id', 'chart_type', 'owner', 'last_modified',)
    column_formatters = dict(label=label_link, last_modified=datetime_f)
    column_default_sort = ('last_modified', True)
    create_template = 'airflow/chart/create.html'
    edit_template = 'airflow/chart/edit.html'
    column_filters = ('label', 'owner.username', 'conn_id')
    column_searchable_list = ('owner.username', 'label', 'sql')
    column_descriptions = {
        'label': "Can include {{ templated_fields }} and {{ macros }}",
        'chart_type': "The type of chart to be displayed",
        'sql': "Can include {{ templated_fields }} and {{ macros }}.",
        'height': "Height of the chart, in pixels.",
        'conn_id': "Source database to run the query against",
        'x_is_date': (
            "Whether the X axis should be casted as a date field. Expect most "
            "intelligible date formats to get casted properly."
        ),
        'owner': (
            "The chart's owner, mostly used for reference and filtering in "
            "the list view."
        ),
        'show_datatable':
            "Whether to display an interactive data table under the chart.",
        'default_params': (
            'A dictionary of {"key": "values",} that define what the '
            'templated fields (parameters) values should be by default. '
            'To be valid, it needs to "eval" as a Python dict. '
            'The key values will show up in the url\'s querystring '
            'and can be altered there.'
        ),
        'show_sql': "Whether to display the SQL statement as a collapsible "
                    "section in the chart page.",
        'y_log_scale': "Whether to use a log scale for the Y axis.",
        'sql_layout': (
            "Defines the layout of the SQL that the application should "
            "expect. Depending on the tables you are sourcing from, it may "
            "make more sense to pivot / unpivot the metrics."
        ),
    }
    column_labels = {
        'sql': "SQL",
        'height': "Chart Height",
        'sql_layout': "SQL Layout",
        'show_sql': "Display the SQL Statement",
        'default_params': "Default Parameters",
    }
    form_choices = {
        'chart_type': [
            ('line', 'Line Chart'),
            ('spline', 'Spline Chart'),
            ('bar', 'Bar Chart'),
            ('column', 'Column Chart'),
            ('area', 'Overlapping Area Chart'),
            ('stacked_area', 'Stacked Area Chart'),
            ('percent_area', 'Percent Area Chart'),
            ('datatable', 'No chart, data table only'),
        ],
        'sql_layout': [
            ('series', 'SELECT series, x, y FROM ...'),
            ('columns', 'SELECT x, y (series 1), y (series 2), ... FROM ...'),
        ],
        'conn_id': [
            (c.conn_id, c.conn_id)
            for c in (
                Session().query(models.Connection.conn_id)
                .group_by(models.Connection.conn_id)
            )
        ]
    }

    def on_model_change(self, form, model, is_created=True):
        if model.iteration_no is None:
            model.iteration_no = 0
        else:
            model.iteration_no += 1
        if not model.user_id and current_user and hasattr(current_user, 'id'):
            model.user_id = current_user.id
        model.last_modified = datetime.now()

chart_mapping = (
    ('line', 'lineChart'),
    ('spline', 'lineChart'),
    ('bar', 'multiBarChart'),
    ('column', 'multiBarChart'),
    ('area', 'stackedAreaChart'),
    ('stacked_area', 'stackedAreaChart'),
    ('percent_area', 'stackedAreaChart'),
    ('datatable', 'datatable'),
)
chart_mapping = dict(chart_mapping)


class KnowEventView(wwwutils.DataProfilingMixin, AirflowModelView):
    verbose_name = "known event"
    verbose_name_plural = "known events"
    form_columns = (
        'label',
        'event_type',
        'start_date',
        'end_date',
        'reported_by',
        'description')
    column_list = (
        'label', 'event_type', 'start_date', 'end_date', 'reported_by')
    column_default_sort = ("start_date", True)


class KnowEventTypeView(wwwutils.DataProfilingMixin, AirflowModelView):
    pass


# NOTE: For debugging / troubleshooting
# mv = KnowEventTypeView(
#     models.KnownEventType,
#     Session, name="Known Event Types", category="Manage")
# admin.add_view(mv)
# class DagPickleView(SuperUserMixin, ModelView):
#     pass
# mv = DagPickleView(
#     models.DagPickle,
#     Session, name="Pickles", category="Manage")
# admin.add_view(mv)


class VariableView(wwwutils.DataProfilingMixin, AirflowModelView):
    verbose_name = "Variable"
    verbose_name_plural = "Variables"
    list_template = 'airflow/variable_list.html'

    def hidden_field_formatter(view, context, model, name):
        if should_hide_value_for_key(model.key):
            return Markup('*' * 8)
        return getattr(model, name)

    form_columns = (
        'key',
        'val',
    )
    column_list = ('key', 'val', 'is_encrypted',)
    column_filters = ('key', 'val')
    column_searchable_list = ('key', 'val')
    form_widget_args = {
        'is_encrypted': {'disabled': True},
        'val': {
            'rows': 20,
        }
    }
    column_sortable_list = (
        'key',
        'val',
        'is_encrypted',
    )
    column_formatters = {
        'val': hidden_field_formatter
    }

    # Default flask-admin export functionality doesn't handle serialized json
    @action('varexport', 'Export', None)
    def action_varexport(self, ids):
        V = models.Variable
        session = settings.Session()
        qry = session.query(V).filter(V.id.in_(ids)).all()
        session.close()

        var_dict = {}
        d = json.JSONDecoder()
        for var in qry:
            val = None
            try:
                val = d.decode(var.val)
            except:
                val = var.val
            var_dict[var.key] = val

        response = make_response(json.dumps(var_dict, sort_keys=True, indent=4))
        response.headers["Content-Disposition"] = "attachment; filename=variables.json"
        return response

    def on_form_prefill(self, form, id):
        if should_hide_value_for_key(form.key.data):
            form.val.data = '*' * 8


class XComView(wwwutils.SuperUserMixin, AirflowModelView):
    verbose_name = "XCom"
    verbose_name_plural = "XComs"
    page_size = 20

    form_columns = (
        'key',
        'value',
        'execution_date',
        'task_id',
        'dag_id',
    )

    form_extra_fields = {
        'value': StringField('Value'),
    }

    column_filters = ('key', 'timestamp', 'execution_date', 'task_id', 'dag_id')
    column_searchable_list = ('key', 'timestamp', 'execution_date', 'task_id', 'dag_id')


class JobModelView(ModelViewOnly):
    verbose_name_plural = "jobs"
    verbose_name = "job"
    column_default_sort = ('start_date', True)
    column_filters = (
        'job_type', 'dag_id', 'state',
        'unixname', 'hostname', 'start_date', 'end_date', 'latest_heartbeat')
    column_formatters = dict(
        start_date=datetime_f,
        end_date=datetime_f,
        hostname=nobr_f,
        state=state_f,
        latest_heartbeat=datetime_f)


class DagRunModelView(ModelViewOnly):
    verbose_name_plural = "DAG Runs"
    can_edit = True
    can_create = True
    column_editable_list = ('state',)
    verbose_name = "dag run"
    column_default_sort = ('execution_date', True)
    form_choices = {
        'state': [
            ('success', 'success'),
            ('running', 'running'),
            ('failed', 'failed'),
        ],
    }
    form_args = dict(
        dag_id=dict(validators=[validators.DataRequired()])
    )
    column_list = (
        'state', 'dag_id', 'execution_date', 'run_id', 'external_trigger')
    column_filters = column_list
    column_searchable_list = ('dag_id', 'state', 'run_id')
    column_formatters = dict(
        execution_date=datetime_f,
        state=state_f,
        start_date=datetime_f,
        dag_id=dag_link)

    @action('new_delete', "Delete", "Are you sure you want to delete selected records?")
    def action_new_delete(self, ids):
        session = settings.Session()
        deleted = set(session.query(models.DagRun)
                      .filter(models.DagRun.id.in_(ids))
                      .all())
        session.query(models.DagRun)\
            .filter(models.DagRun.id.in_(ids))\
            .delete(synchronize_session='fetch')
        session.commit()
        dirty_ids = []
        for row in deleted:
            models.DagStat.set_dirty(row.dag_id, session=session)
            dirty_ids.append(row.dag_id)
        models.DagStat.clean_dirty(dirty_ids, session=session)
        session.close()

    @action('set_running', "Set state to 'running'", None)
    def action_set_running(self, ids):
        self.set_dagrun_state(ids, State.RUNNING)

    @action('set_failed', "Set state to 'failed'", None)
    def action_set_failed(self, ids):
        self.set_dagrun_state(ids, State.FAILED)

    @action('set_success', "Set state to 'success'", None)
    def action_set_success(self, ids):
        self.set_dagrun_state(ids, State.SUCCESS)

    @provide_session
    def set_dagrun_state(self, ids, target_state, session=None):
        try:
            DR = models.DagRun
            count = 0
            dirty_ids = []
            for dr in session.query(DR).filter(DR.id.in_(ids)).all():
                dirty_ids.append(dr.dag_id)
                count += 1
                dr.state = target_state
                if target_state == State.RUNNING:
                    dr.start_date = datetime.now()
                else:
                    dr.end_date = datetime.now()
            session.commit()
            models.DagStat.clean_dirty(dirty_ids, session=session)
            flash(
                "{count} dag runs were set to '{target_state}'".format(**locals()))
        except Exception as ex:
            if not self.handle_view_exception(ex):
                raise Exception("Ooops")
            flash('Failed to set state', 'error')


class LogModelView(ModelViewOnly):
    verbose_name_plural = "logs"
    verbose_name = "log"
    column_default_sort = ('dttm', True)
    column_filters = ('dag_id', 'task_id', 'execution_date')
    column_formatters = dict(
        dttm=datetime_f, execution_date=datetime_f, dag_id=dag_link)


class TaskInstanceModelView(ModelViewOnly):
    verbose_name_plural = "task instances"
    verbose_name = "task instance"
    column_filters = (
        'state', 'dag_id', 'task_id', 'execution_date', 'hostname',
        'queue', 'pool', 'operator', 'start_date', 'end_date')
    named_filter_urls = True
    column_formatters = dict(
        log_url=log_url_formatter,
        task_id=task_instance_link,
        hostname=nobr_f,
        state=state_f,
        execution_date=datetime_f,
        start_date=datetime_f,
        end_date=datetime_f,
        queued_dttm=datetime_f,
        dag_id=dag_link, duration=duration_f)
    column_searchable_list = ('dag_id', 'task_id', 'state')
    column_default_sort = ('start_date', True)
    form_choices = {
        'state': [
            ('success', 'success'),
            ('running', 'running'),
            ('failed', 'failed'),
        ],
    }
    column_list = (
        'state', 'dag_id', 'task_id', 'execution_date', 'operator',
        'start_date', 'end_date', 'duration', 'job_id', 'hostname',
        'unixname', 'priority_weight', 'queue', 'queued_dttm', 'try_number',
        'pool', 'log_url')
    can_delete = True
    page_size = 500

    @action('set_running', "Set state to 'running'", None)
    def action_set_running(self, ids):
        self.set_task_instance_state(ids, State.RUNNING)

    @action('set_failed', "Set state to 'failed'", None)
    def action_set_failed(self, ids):
        self.set_task_instance_state(ids, State.FAILED)

    @action('set_success', "Set state to 'success'", None)
    def action_set_success(self, ids):
        self.set_task_instance_state(ids, State.SUCCESS)

    @action('set_retry', "Set state to 'up_for_retry'", None)
    def action_set_retry(self, ids):
        self.set_task_instance_state(ids, State.UP_FOR_RETRY)

    @action('delete',
            lazy_gettext('Delete'),
            lazy_gettext('Are you sure you want to delete selected records?'))
    def action_delete(self, ids):
        """
        As a workaround for AIRFLOW-277, this method overrides Flask-Admin's ModelView.action_delete().

        TODO: this method should be removed once the below bug is fixed on Flask-Admin side.
        https://github.com/flask-admin/flask-admin/issues/1226
        """
        if 'sqlite' in conf.get('core', 'sql_alchemy_conn'):
            self.delete_task_instances(ids)
        else:
            super(TaskInstanceModelView, self).action_delete(ids)

    @provide_session
    def set_task_instance_state(self, ids, target_state, session=None):
        try:
            TI = models.TaskInstance
            count = len(ids)
            for id in ids:
                task_id, dag_id, execution_date = id.split(',')
                execution_date = datetime.strptime(execution_date, '%Y-%m-%d %H:%M:%S')
                ti = session.query(TI).filter(TI.task_id == task_id,
                                              TI.dag_id == dag_id,
                                              TI.execution_date == execution_date).one()
                ti.state = target_state
            session.commit()
            flash(
                "{count} task instances were set to '{target_state}'".format(**locals()))
        except Exception as ex:
            if not self.handle_view_exception(ex):
                raise Exception("Ooops")
            flash('Failed to set state', 'error')

    @provide_session
    def delete_task_instances(self, ids, session=None):
        try:
            TI = models.TaskInstance
            count = 0
            for id in ids:
                task_id, dag_id, execution_date = id.split(',')
                execution_date = datetime.strptime(execution_date, '%Y-%m-%d %H:%M:%S')
                count += session.query(TI).filter(TI.task_id == task_id,
                                                  TI.dag_id == dag_id,
                                                  TI.execution_date == execution_date).delete()
            session.commit()
            flash("{count} task instances were deleted".format(**locals()))
        except Exception as ex:
            if not self.handle_view_exception(ex):
                raise Exception("Ooops")
            flash('Failed to delete', 'error')

    def get_one(self, id):
        """
        As a workaround for AIRFLOW-252, this method overrides Flask-Admin's ModelView.get_one().

        TODO: this method should be removed once the below bug is fixed on Flask-Admin side.
        https://github.com/flask-admin/flask-admin/issues/1226
        """
        task_id, dag_id, execution_date = iterdecode(id)
        execution_date = dateutil.parser.parse(execution_date)
        return self.session.query(self.model).get((task_id, dag_id, execution_date))


class ConnectionModelView(wwwutils.SuperUserMixin, AirflowModelView):
    create_template = 'airflow/conn_create.html'
    edit_template = 'airflow/conn_edit.html'
    list_template = 'airflow/conn_list.html'
    form_columns = (
        'conn_id',
        'conn_type',
        'host',
        'schema',
        'login',
        'password',
        'port',
        'extra',
        'extra__jdbc__drv_path',
        'extra__jdbc__drv_clsname',
        'extra__google_cloud_platform__project',
        'extra__google_cloud_platform__key_path',
        'extra__google_cloud_platform__scope',
    )
    verbose_name = "Connection"
    verbose_name_plural = "Connections"
    column_default_sort = ('conn_id', False)
    column_list = ('conn_id', 'conn_type', 'host', 'port', 'is_encrypted', 'is_extra_encrypted',)
    form_overrides = dict(_password=PasswordField)
    form_widget_args = {
        'is_extra_encrypted': {'disabled': True},
        'is_encrypted': {'disabled': True},
    }
    # Used to customized the form, the forms elements get rendered
    # and results are stored in the extra field as json. All of these
    # need to be prefixed with extra__ and then the conn_type ___ as in
    # extra__{conn_type}__name. You can also hide form elements and rename
    # others from the connection_form.js file
    form_extra_fields = {
        'extra__jdbc__drv_path': StringField('Driver Path'),
        'extra__jdbc__drv_clsname': StringField('Driver Class'),
        'extra__google_cloud_platform__project': StringField('Project Id'),
        'extra__google_cloud_platform__key_path': StringField('Keyfile Path'),
        'extra__google_cloud_platform__scope': StringField('Scopes (comma seperated)'),

    }
    form_choices = {
        'conn_type': models.Connection._types
    }

    def on_model_change(self, form, model, is_created):
        formdata = form.data
        if formdata['conn_type'] in ['jdbc', 'google_cloud_platform']:
            extra = {
                key: formdata[key]
                for key in self.form_extra_fields.keys() if key in formdata}
            model.extra = json.dumps(extra)

    @classmethod
    def alert_fernet_key(cls):
        fk = None
        try:
            fk = conf.get('core', 'fernet_key')
        except:
            pass
        return fk is None

    @classmethod
    def is_secure(cls):
        """
        Used to display a message in the Connection list view making it clear
        that the passwords and `extra` field can't be encrypted.
        """
        is_secure = False
        try:
            import cryptography
            conf.get('core', 'fernet_key')
            is_secure = True
        except:
            pass
        return is_secure

    def on_form_prefill(self, form, id):
        try:
            d = json.loads(form.data.get('extra', '{}'))
        except Exception:
            d = {}

        for field in list(self.form_extra_fields.keys()):
            value = d.get(field, '')
            if value:
                field = getattr(form, field)
                field.data = value


class UserModelView(wwwutils.SuperUserMixin, AirflowModelView):
    verbose_name = "User"
    verbose_name_plural = "Users"
    column_default_sort = 'username'


class VersionView(wwwutils.SuperUserMixin, LoggingMixin, BaseView):
    @expose('/')
    def version(self):
        # Look at the version from setup.py
        try:
            airflow_version = pkg_resources.require("airflow")[0].version
        except Exception as e:
            airflow_version = None
            self.logger.error(e)

        # Get the Git repo and git hash
        git_version = None
        try:
            with open(os.path.join(*[settings.AIRFLOW_HOME, 'airflow', 'git_version'])) as f:
                git_version = f.readline()
        except Exception as e:
            self.logger.error(e)

        # Render information
        title = "Version Info"
        return self.render('airflow/version.html',
                           title=title,
                           airflow_version=airflow_version,
                           git_version=git_version)


class ConfigurationView(wwwutils.SuperUserMixin, BaseView):
    @expose('/')
    def conf(self):
        raw = request.args.get('raw') == "true"
        title = "Airflow Configuration"
        subtitle = conf.AIRFLOW_CONFIG
        if conf.getboolean("webserver", "expose_config"):
            with open(conf.AIRFLOW_CONFIG, 'r') as f:
                config = f.read()
            table = [(section, key, value, source)
                     for section, parameters in conf.as_dict(True, True).items()
                     for key, (value, source) in parameters.items()]

        else:
            config = (
                "# You Airflow administrator chose not to expose the "
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
            return self.render(
                'airflow/config.html',
                pre_subtitle=settings.HEADER + "  v" + airflow.__version__,
                code_html=code_html, title=title, subtitle=subtitle,
                table=table)


class DagModelView(wwwutils.SuperUserMixin, ModelView):
    column_list = ('dag_id', 'owners')
    column_editable_list = ('is_paused',)
    form_excluded_columns = ('is_subdag', 'is_active')
    column_searchable_list = ('dag_id',)
    column_filters = (
        'dag_id', 'owners', 'is_paused', 'is_active', 'is_subdag',
        'last_scheduler_run', 'last_expired')
    form_widget_args = {
        'last_scheduler_run': {'disabled': True},
        'fileloc': {'disabled': True},
        'is_paused': {'disabled': True},
        'last_pickled': {'disabled': True},
        'pickle_id': {'disabled': True},
        'last_loaded': {'disabled': True},
        'last_expired': {'disabled': True},
        'pickle_size': {'disabled': True},
        'scheduler_lock': {'disabled': True},
        'owners': {'disabled': True},
    }
    column_formatters = dict(
        dag_id=dag_link,
    )
    can_delete = False
    can_create = False
    page_size = 50
    list_template = 'airflow/list_dags.html'
    named_filter_urls = True

    def get_query(self):
        """
        Default filters for model
        """
        return (
            super(DagModelView, self)
            .get_query()
            .filter(or_(models.DagModel.is_active, models.DagModel.is_paused))
            .filter(~models.DagModel.is_subdag)
        )

    def get_count_query(self):
        """
        Default filters for model
        """
        return (
            super(DagModelView, self)
            .get_count_query()
            .filter(models.DagModel.is_active)
            .filter(~models.DagModel.is_subdag)
        )

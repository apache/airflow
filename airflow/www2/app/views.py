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
from collections import defaultdict

import inspect
from textwrap import dedent
import traceback

import sqlalchemy as sqla
from sqlalchemy import or_, desc, and_, union_all

from flask import (
    g, redirect, url_for, request, Markup, Response, current_app, render_template, make_response, abort)

from flask_login import flash

from flask_appbuilder import BaseView, ModelView, expose, has_access, AppBuilder
from flask_appbuilder.models.sqla.interface import SQLAInterface
from flask_appbuilder.actions import action

from jinja2.sandbox import ImmutableSandboxedEnvironment
import markdown
import nvd3

from wtforms import (
    Form, SelectField, TextAreaField, PasswordField, StringField, validators)

from pygments import highlight, lexers
from pygments.formatters import HtmlFormatter

from app import appbuilder, db
from app import models as fab_models

import airflow
from airflow import configuration as conf
from airflow import models
from airflow import settings
from airflow.api.common.experimental.mark_tasks import set_dag_run_state
from airflow.exceptions import AirflowException
from airflow.settings import Session
from airflow.models import XCom, DagRun
from airflow.ti_deps.dep_context import DepContext, QUEUE_DEPS, SCHEDULER_DEPS

from airflow.models import BaseOperator
from airflow.operators.subdag_operator import SubDagOperator

from airflow.utils.logging import LoggingMixin, get_log_filename
from airflow.utils.json import json_ser
from airflow.utils.state import State
from airflow.utils.db import provide_session
from airflow.utils.helpers import alchemy_to_dict
from airflow.utils import logging as log_utils
from airflow.utils.dates import infer_time_unit, scale_time_units
from airflow.www import utils as wwwutils
from airflow.www.forms import DateTimeForm, DateTimeWithNumRunsForm
from airflow.www.validators import GreaterEqualThan
from airflow.configuration import AirflowConfigException

QUERY_LIMIT = 100000
CHART_LIMIT = 200000

dagbag = models.DagBag(settings.DAGS_FOLDER)

def dag_link(dag_id):
    dag_id = bleach.clean(dag_id)
    url = url_for(
        'Airflow.graph',
        dag_id=dag_id)
    return Markup(
        '<a href="{}">{}</a>'.format(url, dag_id))


def log_url_formatter(log_url):
    return Markup(
        '<a href="{log_url}">'
        '    <span class="glyphicon glyphicon-book" aria-hidden="true">'
        '</span></a>').format(**locals())


def task_instance_link(v, c, m, p):
    dag_id = bleach.clean(m.dag_id)
    task_id = bleach.clean(m.task_id)
    url = url_for(
        'Airflow.task',
        dag_id=dag_id,
        task_id=task_id,
        execution_date=m.execution_date.isoformat())
    url_root = url_for(
        'Airflow.graph',
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


def state_f(state):
    return state_token(state)


def duration_f(v, c, m, p):
    if m.end_date and m.duration:
        return timedelta(seconds=m.duration)


def datetime_f(attr):
    dttm = attr.isoformat() if attr else ''
    if datetime.now().isoformat()[:4] == dttm[:4]:
        dttm = dttm[5:]
    return Markup("<nobr>{}</nobr>".format(dttm))


def nobr_f(hostname):
    return Markup("<nobr>{}</nobr>".format(hostname))


def label_link(v, c, m, p):
    try:
        default_params = ast.literal_eval(m.default_params)
    except:
        default_params = {}
    url = url_for(
        'Airflow.chart', chart_id=m.id, iteration_no=m.iteration_no,
        **default_params)
    return Markup("<a href='{url}'>{m.label}</a>".format(**locals()))


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


def get_chart_height(dag):
    """
    TODO(aoen): See [AIRFLOW-1263] We use the number of tasks in the DAG as a heuristic to
    approximate the size of generated chart (otherwise the charts are tiny and unreadable
    when DAGs have a large number of tasks). Ideally nvd3 should allow for dynamic-height
    charts, that is charts that take up space based on the size of the components within.
    """
    return 600 + len(dag.tasks) * 10

def get_roles():
    if g.user.is_anonymous():
        return [appbuilder.sm.find_role('public')]
    return g.user.roles

def is_admin():
    roles = get_roles()
    admin_role = appbuilder.sm.find_role('Admin')
    return admin_role in roles


'''
######################################################################################
                                    BaseViews
######################################################################################
'''
class Airflow(BaseView):
    route_base='/admin'

    @expose("/")
    @has_access
    def index(self):
        session = Session()
        DM = models.DagModel
        qry = None

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
        webserver_dags = {
            dag.dag_id: dag
            for dag in unfiltered_webserver_dags
        }

        all_dag_ids = sorted(set(orm_dags.keys()) | set(webserver_dags.keys()))

        if is_admin():
            return render_template(
                'airflow/dags.html',
                webserver_dags=webserver_dags,
                orm_dags=orm_dags,
                hide_paused=hide_paused,
                all_dag_ids=all_dag_ids,
                read_only=False,
                base_template=appbuilder.base_template,
                appbuilder=appbuilder)
        else:
            return render_template(
                'airflow/dags.html',
                webserver_dags=webserver_dags,
                orm_dags=orm_dags,
                hide_paused=hide_paused,
                all_dag_ids=all_dag_ids,
                read_only=True,
                base_template=appbuilder.base_template,
                appbuilder=appbuilder)

    @expose('/chart_data')
    @has_access
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
    @has_access
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
        return render_template(
            'airflow/nvd3.html',
            chart=chart,
            title="Airflow - Chart",
            sql=sql,
            label=chart.label,
            embed=embed,
            base_template=appbuilder.base_template,
            appbuilder=appbuilder)

    @expose('/dag_stats')
    @has_access
    def dag_stats(self):
        ds = models.DagStat
        session = Session()

        ds.update()

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
    @has_access
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
    @has_access
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

        return render_template(
            'airflow/dag_code.html', html_code=html_code, dag=dag, title=title,
            root=request.args.get('root'),
            demo_mode=conf.getboolean('webserver', 'demo_mode'),
            base_template=appbuilder.base_template,
            appbuilder=appbuilder)

    @expose('/dag_details')
    @has_access
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
        return render_template(
            'airflow/dag_details.html',
            dag=dag, title=title, states=states, State=State,
            base_template=appbuilder.base_template,
            appbuilder=appbuilder)

    @appbuilder.app.errorhandler(404)
    def circles(self):
        return render_template(
            'airflow/circles.html', hostname=socket.getfqdn()), 404

    @appbuilder.app.errorhandler(500)
    def show_traceback(self):
        from airflow.utils import asciiart as ascii_
        return render_template(
            'airflow/traceback.html',
            hostname=socket.getfqdn(),
            nukular=ascii_.nukular,
            info=traceback.format_exc(),), 500

    @expose('/noaccess')
    @has_access
    def noaccess(self):
        return render_template('airflow/noaccess.html',
            base_template=appbuilder.base_template,
            appbuilder=appbuilder)

    @expose('/headers')
    @has_access
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
    @has_access
    def pickle_info(self):
        d = {}
        dag_id = request.args.get('dag_id')
        dags = [dagbag.dags.get(dag_id)] if dag_id else dagbag.dags.values()
        for dag in dags:
            if not dag.is_subdag:
                d[dag.dag_id] = dag.pickle_info()
        return wwwutils.json_response(d)

    @expose('/login', methods=['GET', 'POST'])
    @has_access
    def login(self):
        return "foo"

    @expose('/logout')
    @has_access
    def login(self):
        return "foo"

    @expose('/rendered')
    @has_access
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

        return render_template(
            'airflow/ti_code.html',
            html_dict=html_dict,
            dag=dag,
            task_id=task_id,
            execution_date=execution_date,
            form=form,
            title=title,
            base_template=appbuilder.base_template,
            appbuilder=appbuilder)


    @expose('/log')
    @has_access
    def log(self):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        dttm = dateutil.parser.parse(execution_date)
        form = DateTimeForm(data={'execution_date': dttm})
        dag = dagbag.get_dag(dag_id)
        TI = models.TaskInstance
        session = Session()
        ti = session.query(TI).filter(
            TI.dag_id == dag_id,
            TI.task_id == task_id,
            TI.execution_date == dttm).first()
        logs = []
        if ti is None:
            logs = ["*** Task instance did not exist in the DB\n"]
        else:
            logs = [''] * ti.try_number
            for try_number in range(ti.try_number):
                log_filename = get_log_filename(
                    dag_id, task_id, execution_date, try_number)
                logs[try_number] += self._get_log(ti, log_filename)

        return render_template(
            'airflow/ti_log.html',
            logs=logs, dag=dag, title="Log by attempts", task_id=task_id,
            execution_date=execution_date, form=form,
            base_template=appbuilder.base_template,
            appbuilder=appbuilder)


    @expose('/task')
    @has_access
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
        return render_template(
            'airflow/task.html',
            task_attrs=task_attrs,
            ti_attrs=ti_attrs,
            failed_dep_reasons=failed_dep_reasons or no_failed_deps_result,
            task_id=task_id,
            execution_date=execution_date,
            special_attrs_rendered=special_attrs_rendered,
            form=form,
            dag=dag,
            title=title,
            base_template=appbuilder.base_template,
            appbuilder=appbuilder)


    @expose('/xcom')
    @has_access
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
        return render_template(
            'airflow/xcom.html',
            attributes=attributes,
            task_id=task_id,
            execution_date=execution_date,
            form=form,
            dag=dag, title=title,
            base_template=appbuilder.base_template,
            appbuilder=appbuilder)

    @expose('/run')
    @has_access
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
            from airflow.executors import GetDefaultExecutor
            from airflow.executors import CeleryExecutor
            executor = GetDefaultExecutor()
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
    @has_access
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

    def _clear_dag_tis(self, dag, start_date, end_date, origin,
                       recursive=False, confirmed=False):
        if confirmed:
            count = dag.clear(
                start_date=start_date,
                end_date=end_date,
                include_subdags=recursive)

            flash("{0} task instances have been cleared".format(count))
            return redirect(origin)

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

            response = render_template(
                'airflow/confirm.html',
                message=("Here's the list of task instances you are about "
                         "to clear:"),
                details=details,
                base_template=appbuilder.base_template,
                appbuilder=appbuilder)

        return response

    @expose('/clear')
    @has_access
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

        return self._clear_dag_tis(dag, start_date, end_date, origin,
                                   recursive=recursive, confirmed=confirmed)

    @expose('/dagrun_clear')
    @has_access
    def dagrun_clear(self):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        origin = request.args.get('origin')
        execution_date = request.args.get('execution_date')
        confirmed = request.args.get('confirmed') == "true"

        dag = dagbag.get_dag(dag_id)
        execution_date = dateutil.parser.parse(execution_date)
        start_date = execution_date
        end_date = execution_date

        return self._clear_dag_tis(dag, start_date, end_date, origin,
                                   recursive=True, confirmed=confirmed)

    @expose('/blocked')
    @has_access
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

    @expose('/dagrun_success')
    @has_access
    def dagrun_success(self):
        dag_id = request.args.get('dag_id')
        execution_date = request.args.get('execution_date')
        confirmed = request.args.get('confirmed') == 'true'
        origin = request.args.get('origin')

        if not execution_date:
            flash('Invalid execution date', 'error')
            return redirect(origin)

        execution_date = dateutil.parser.parse(execution_date)
        dag = dagbag.get_dag(dag_id)

        if not dag:
            flash('Cannot find DAG: {}'.format(dag_id), 'error')
            return redirect(origin)

        new_dag_state = set_dag_run_state(dag, execution_date, state=State.SUCCESS,
                                          commit=confirmed)

        if confirmed:
            flash('Marked success on {} task instances'.format(len(new_dag_state)))
            return redirect(origin)

        else:
            details = '\n'.join([str(t) for t in new_dag_state])

            response = render_template('airflow/confirm.html',
                                   message=("Here's the list of task instances you are "
                                            "about to mark as successful:"),
                                   details=details,
                                   base_template=appbuilder.base_template,
                                   appbuilder=appbuilder)

            return response

    @expose('/success')
    @has_access
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

        response = render_template("airflow/confirm.html",
                                   message=("Here's the list of task instances you are "
                                            "about to mark as successful:"),
                                   details=details,
                                   base_template=appbuilder.base_template,
                                   appbuilder=appbuilder)

        return response

    @expose('/tree')
    @has_access
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
        return render_template(
            'airflow/tree.html',
            operators=sorted(
                list(set([op.__class__ for op in dag.tasks])),
                key=lambda x: x.__name__
            ),
            root=root,
            form=form,
            dag=dag,
            data=data,
            blur=blur,
            base_template=appbuilder.base_template,
            appbuilder=appbuilder)


    @expose('/graph')
    @has_access
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

        if is_admin():
            return render_template(
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
                edges=json.dumps(edges, indent=2),
                read_only=False,
                base_template=appbuilder.base_template,
                appbuilder=appbuilder)
        else:
            return render_template(
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
                edges=json.dumps(edges, indent=2),
                read_only=False,
                base_template=appbuilder.base_template,
                appbuilder=appbuilder)

    @expose('/duration')
    @has_access
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


        chart_height = get_chart_height(dag)
        chart = nvd3.lineChart(
            name="lineChart", x_is_date=True, height=chart_height, width="1200")
        cum_chart = nvd3.lineChart(
            name="cumLineChart", x_is_date=True, height=chart_height, width="1200")

        y = defaultdict(list)
        x = defaultdict(list)
        cum_y = defaultdict(list)

        tis = dag.get_task_instances(
            session, start_date=min_date, end_date=base_date)
        TF = models.TaskFail
        ti_fails = (
            session
                .query(TF)
                .filter(
                    TF.dag_id == dag.dag_id,
                    TF.execution_date >= min_date,
                    TF.execution_date <= base_date,
                    TF.task_id.in_([t.task_id for t in dag.tasks]))
                .all()
        )

        fails_totals = defaultdict(int)
        for tf in ti_fails:
            dict_key = (tf.dag_id, tf.task_id, tf.execution_date)
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
        cum_chart.create_y_axis('yAxis', format='.02f', custom_format=False,
                                label='Duration ({})'.format(cum_y_unit))
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
        session.close()

        form = DateTimeWithNumRunsForm(data={'base_date': max_date,
                                             'num_runs': num_runs})
        chart.buildcontent()
        cum_chart.buildcontent()
        s_index = cum_chart.htmlcontent.rfind('});')
        cum_chart.htmlcontent = (cum_chart.htmlcontent[:s_index] +
                                 "$( document ).trigger('chartload')" +
                                 cum_chart.htmlcontent[s_index:])

        return render_template(
            'airflow/duration_chart.html',
            dag=dag,
            demo_mode=conf.getboolean('webserver', 'demo_mode'),
            root=root,
            form=form,
            chart=chart.htmlcontent,
            cum_chart=cum_chart.htmlcontent,
            base_template=appbuilder.base_template,
            appbuilder=appbuilder)


    @expose('/tries')
    @has_access
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

        chart_height = get_chart_height(dag)
        chart = nvd3.lineChart(
            name="lineChart", x_is_date=True, y_axis_format='d', height=chart_height,
            width="1200")

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

        return render_template(
            'airflow/chart.html',
            dag=dag,
            demo_mode=conf.getboolean('webserver', 'demo_mode'),
            root=root,
            form=form,
            chart=chart.htmlcontent,
            base_template=appbuilder.base_template,
            appbuilder=appbuilder)


    @expose('/landing_times')
    @has_access
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

        chart_height = get_chart_height(dag)
        chart = nvd3.lineChart(
            name="lineChart", x_is_date=True, height=chart_height, width="1200")
        y = {}
        x = {}
        for task in dag.tasks:
            y[task.task_id] = []
            x[task.task_id] = []
            for ti in task.get_task_instances(session, start_date=min_date,
                                              end_date=base_date):
                ts = ti.execution_date
                if dag.schedule_interval and dag.following_schedule(ts):
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
        return render_template(
            'airflow/chart.html',
            dag=dag,
            chart=chart.htmlcontent,
            height=str(chart_height + 100) + "px",
            demo_mode=conf.getboolean('webserver', 'demo_mode'),
            root=root,
            form=form,
            base_template=appbuilder.base_template,
            appbuilder=appbuilder)


    @expose('/paused', methods=['POST'])
    @has_access
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
    @has_access
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
    @has_access
    def refresh_all(self):
        dagbag.collect_dags(only_if_updated=False)
        flash("All DAGs are now up to date")
        return redirect('/')

    @expose('/gantt')
    @has_access
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

        return render_template(
            'airflow/gantt.html',
            dag=dag,
            execution_date=dttm.isoformat(),
            form=form,
            data=json.dumps(data, indent=2),
            base_date='',
            demo_mode=demo_mode,
            root=root,
            base_template=appbuilder.base_template,
            appbuilder=appbuilder)


    @expose('/object/task_instances')
    @has_access
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
    @has_access
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
                return render_template(
                    'airflow/variables/{}.html'.format(form),
                    base_template=appbuilder.base_template,
                    appbuilder=appbuilder)
        except:
            return ("Error: form airflow/variables/{}.html "
                    "not found.").format(form), 404

    @expose('/varimport', methods=["GET", "POST"])
    @has_access
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

class ConfigurationView(BaseView):
    route_base = '/admin'

    @expose('/configurationview')
    @has_access
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
            return render_template(
                'airflow/config.html',
                pre_subtitle=settings.HEADER + "  v" + airflow.__version__,
                code_html=code_html,
                title=title,
                subtitle=subtitle,
                table=table,
                base_template=appbuilder.base_template,
                appbuilder=appbuilder)


class VersionView(BaseView):
    route_base='/admin'

    @expose('/versionview')
    @has_access
    def version(self):
        # Look at the version from setup.py
        try:
            airflow_version = pkg_resources.require("apache-airflow")[0].version
        except Exception as e:
            airflow_version = None
            # self.logger.error(e) # todo: add logger
            pass

        # Get the Git repo and git hash
        git_version = None
        try:
            with open(os.path.join(*[settings.AIRFLOW_HOME, 'airflow', 'git_version'])) as f:
                git_version = f.readline()
        except Exception as e:
            # self.logger.error(e)  # todo: add logger
            pass

        # Render information
        title = "Version Info"
        return render_template(
            'airflow/version.html',
            title=title,
            airflow_version=airflow_version,
            git_version=git_version,
            base_template=appbuilder.base_template,
            appbuilder=appbuilder)


class QueryView(BaseView):
    route_base='/admin'

    @expose('/queryview', methods=['POST', 'GET'])
    @has_access
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
        return render_template(
            'airflow/query.html', form=form,
            title="Ad Hoc Query",
            results=results or '',
            has_data=has_data,
            base_template=appbuilder.base_template,
            appbuilder=appbuilder)

'''
######################################################################################
                                    ModelViews
######################################################################################
'''

class DagModelModelView(ModelView):
    route_base='/admin/dagmodel'

    datamodel = SQLAInterface(fab_models.DagModel)

    list_columns = ['dag_id', 'is_paused', 'last_scheduler_run',
                    'last_expired', 'scheduler_lock', 'fileloc', 'owners']
    show_columns = ['dag_id', 'is_paused', 'last_scheduler_run',
                    'last_expired', 'scheduler_lock', 'fileloc', 'owners']
    search_columns = ['dag_id', 'is_paused', 'last_scheduler_run',
                    'last_expired', 'scheduler_lock', 'fileloc', 'owners']
    formatters_columns = { 'dag_id':dag_link }



class XComModelView(ModelView):
    route_base='/admin/xcom'

    datamodel = SQLAInterface(fab_models.XCom)

    base_order = ('execution_date', 'desc')
    list_columns = ['key','timestamp', 'execution_date','task_id', 'dag_id']
    add_columns = ['key','execution_date','task_id', 'dag_id']
    edit_columns = ['key','execution_date','task_id', 'dag_id']
    search_columns = ['key', 'timestamp', 'execution_date', 'task_id', 'dag_id']


class DagRunModelView(ModelView):
    route_base='/admin/dagrun'

    datamodel = SQLAInterface(fab_models.DagRun)

    base_order = ('execution_date', 'desc')
    list_columns = ['state', 'dag_id', 'execution_date', 'run_id', 'external_trigger']
    add_columns = ['state', 'dag_id', 'execution_date', 'run_id', 'external_trigger']
    edit_columns = ['state', 'dag_id', 'execution_date', 'run_id', 'external_trigger']
    search_columns = ['state', 'dag_id', 'execution_date', 'run_id', 'external_trigger']
    formatters_columns = {
        'execution_date': datetime_f,
        'state': state_f,
        'start_date': datetime_f,
        'dag_id':dag_link,
    }

    @action('delete', "Delete", "Are you sure you want to delete selected records?", single=False)
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
            dirty_ids.append(row.dag_id)
        models.DagStat.update(dirty_ids, dirty_only=False, session=session)
        session.close()

    @action('set_running', "Set state to 'running'", '', single=False)
    def action_set_running(self, drs):
        return self.set_dagrun_state(drs, State.RUNNING)

    @action('set_failed', "Set state to 'failed'", '', single=False)
    def action_set_failed(self, drs):
        return self.set_dagrun_state(drs, State.FAILED)

    @action('set_success', "Set state to 'success'", '', single=False)
    def action_set_success(self, drs):
        return self.set_dagrun_state(drs, State.SUCCESS)

    @provide_session
    def set_dagrun_state(self, drs, target_state, session=None):
        try:
            DR = models.DagRun
            count = 0
            dirty_ids = []
            for dr in session.query(DR).filter(
                    DR.id.in_([dagrun.id for dagrun in drs])).all():
                dirty_ids.append(dr.dag_id)
                count += 1
                dr.state = target_state
                if target_state == State.RUNNING:
                    dr.start_date = datetime.now()
                else:
                    dr.end_date = datetime.now()
            session.commit()
            models.DagStat.update(dirty_ids, session=session)
            flash(
                "{count} dag runs were set to '{target_state}'".format(**locals()))
        except Exception as ex:
            raise Exception("Ooops")
            flash('Failed to set state', 'error')
        return redirect(self.route_base + '/list')


class ConnectionModelView(ModelView):
    route_base='/admin/connection'

    datamodel = SQLAInterface(fab_models.Connection)

    base_order = ('conn_id', 'asc')
    list_columns = ['conn_id', 'conn_type', 'host', 'port', 'is_encrypted', 'is_extra_encrypted']
    add_columns = ['conn_id', 'conn_type', 'host', 'port', 'is_encrypted', 'is_extra_encrypted']
    edit_columns = ['conn_id', 'conn_type', 'host', 'port', 'is_encrypted', 'is_extra_encrypted']
    column_filters = ['conn_id', 'conn_type', 'host', 'port', 'is_encrypted', 'is_extra_encrypted']
    column_searchable_list = ['conn_id', 'conn_type', 'host', 'port', 'is_encrypted', 'is_extra_encrypted']


class PoolModelView(ModelView):
    route_base='/admin/pool'

    datamodel = SQLAInterface(fab_models.Pool)

    base_order = ('pool', 'asc')
    list_columns = ['pool', 'slots']
    add_columns = ['pool', 'slots']
    edit_columns = ['pool', 'slots']
    search_columns = ['pool', 'slots']

    def pool_link(pool):
        url = '/admin/taskinstance/list/?_flt_3_pool=' + pool
        return Markup("<a href='{url}'>{pool}</a>".format(**locals()))

    formatters_columns = {
        'pool': pool_link,
        # todo: multiple args for formatter not supported by FAB
        # 'used_slots': fused_slots,
        # 'queued_slots': fqueued_slots,
    }


class VariableModelView(ModelView):
    route_base='/admin/variable'

    datamodel = SQLAInterface(fab_models.Variable)

    list_columns = ['val', 'is_encrypted']
    add_columns = ['val', 'is_encrypted']
    edit_columns = ['val', 'is_encrypted']
    search_columns = ['val']

    def hidden_field_formatter(val, key):
        if wwwutils.should_hide_value_for_key(key):
            return Markup('*' * 8)
        return val

    formatters_columns = {
        # todo: multiple args for formatter not supported by FAB
        # 'val': hidden_field_formatter,
    }

    # Default flask-admin export functionality doesn't handle serialized json
    @action('varexport', 'Export', '', single=False)
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


class SlaMissModelView(ModelView):
    route_base='/admin/slamiss'

    datamodel = SQLAInterface(fab_models.SlaMiss)

    base_order = ('execution_date', 'desc')
    list_columns = ['dag_id', 'task_id', 'execution_date', 'email_sent', 'timestamp']
    add_columns = ['dag_id', 'task_id', 'execution_date', 'email_sent', 'timestamp']
    edit_columns = ['dag_id', 'task_id', 'execution_date', 'email_sent', 'timestamp']
    search_columns = ['dag_id', 'task_id', 'email_sent', 'timestamp', 'execution_date']
    formatters_columns = {
        # todo: multiple args for formatter not supported by FAB
        # 'task_id': task_instance_link,
        'execution_date': datetime_f,
        'timestamp': datetime_f,
        'dag_id': dag_link,
    }


class TaskInstanceModelView(ModelView):
    route_base='/admin/taskinstance'

    datamodel = SQLAInterface(fab_models.TaskInstance)
    page_size = 500

    base_order = ('job_id', 'asc')
    list_columns = ['state', 'dag_id', 'task_id', 'execution_date', 'operator',
        'start_date', 'end_date', 'duration', 'job_id', 'hostname',
        'unixname', 'priority_weight', 'queue', 'queued_dttm', 'try_number',
        'pool']
    add_columns = ['state', 'dag_id', 'task_id', 'execution_date', 'operator',
        'start_date', 'end_date', 'duration', 'job_id', 'hostname',
        'unixname', 'priority_weight', 'queue', 'queued_dttm', 'try_number',
        'pool']
    edit_columns = ['state', 'dag_id', 'task_id', 'execution_date', 'operator',
        'start_date', 'end_date', 'duration', 'job_id', 'hostname',
        'unixname', 'priority_weight', 'queue', 'queued_dttm', 'try_number',
        'pool']
    search_columns = ['state', 'dag_id', 'task_id', 'execution_date', 'hostname',
        'queue', 'pool', 'operator', 'start_date', 'end_date']

    formatters_columns = {
        'log_url': log_url_formatter,
        # todo: multiple args for formatter not supported by FAB
        # 'task_id': task_instance_link,
        'hostname': nobr_f,
        'state': state_f,
        'execution_date': datetime_f,
        'start_date': datetime_f,
        'end_date': datetime_f,
        'queued_dttm': datetime_f,
        'dag_id': dag_link,
        # todo: multiple args for formatter not supported by FAB
        # 'duration': duration_f,
    }


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
            flash('Failed to set state', 'error')
        return redirect(self.route_base + '/list')

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
            flash('Failed to delete', 'error')
        return redirect(self.route_base + '/list')

    @action('set_running', "Set state to 'running'", '', single=False)
    def action_set_running(self, ids):
        return self.set_task_instance_state(ids, State.RUNNING)

    @action('set_failed', "Set state to 'failed'", '', single=False)
    def action_set_failed(self, ids):
        return self.set_task_instance_state(ids, State.FAILED)

    @action('set_success', "Set state to 'success'", '', single=False)
    def action_set_success(self, ids):
        return self.set_task_instance_state(ids, State.SUCCESS)

    @action('set_retry', "Set state to 'up_for_retry'", '', single=False)
    def action_set_retry(self, ids):
        return self.set_task_instance_state(ids, State.UP_FOR_RETRY)

    @action('delete', 'Delete', 
            confirmation='Are you sure you want to delete selected records?', single=False)
    def action_delete(self, ids):
        return self.delete_task_instances(ids)


class KnownEventModelView(ModelView):
    route_base='/admin/knownevent'

    datamodel = SQLAInterface(fab_models.KnownEvent)

    base_order = ('start_date', 'desc')
    list_columns = ['label', 'event_type', 'start_date', 'end_date', 'reported_by',]
    add_columns = ['label', 'event_type', 'start_date', 'end_date', 'reported_by',]
    edit_columns = ['label', 'event_type', 'start_date', 'end_date', 'reported_by',]
    search_columns = ['label', 'event_type', 'start_date', 'end_date', 'reported_by',]


class ChartModelView(ModelView):
    route_base='/admin/chart'

    datamodel = SQLAInterface(fab_models.Chart)
    list_columns = ['label', 'conn_id', 'chart_type', 'owner', 'last_modified']
    add_columns =  ['label', 'conn_id', 'chart_type', 'owner', 'last_modified']
    edit_columns = ['label', 'conn_id', 'chart_type', 'owner', 'last_modified']
    search_columns = ['label', 'owner', 'conn_id']
    formatters_columns = {
        # todo: multiple args for formatter not supported by FAB
        # 'label': label_link,
        'last_modified': datetime_f,
    }


class JobModelView(ModelView):
    route_base='/admin/job'

    datamodel = SQLAInterface(fab_models.BaseJob)

    base_order = ('start_date', 'desc')
    list_columns = ['job_type', 'dag_id', 'state', 'unixname', 'hostname',
                    'start_date', 'end_date', 'latest_heartbeat']
    search_columns = ['job_type', 'dag_id', 'state', 'unixname', 'hostname',
                    'start_date', 'end_date', 'latest_heartbeat']
    formatters_columns = {
        'start_date': datetime_f,
        'end_date': datetime_f,
        'hostname': nobr_f,
        'state': state_f,
        'latest_heartbeat': datetime_f
    }


class LogModelView(ModelView):
    route_base='/admin/log'

    datamodel = SQLAInterface(fab_models.Log)

    base_order = ('dttm', 'desc')
    list_columns = ['id', 'dttm', 'dag_id', 'task_id', 'event', 'execution_date', 'owner', 'extra']
    search_columns = ['dag_id', 'task_id', 'execution_date']
    formatters_columns = {
        'dttm': datetime_f,
        'execution_date':datetime_f,
        'dag_id':dag_link,
    }


db.create_all()

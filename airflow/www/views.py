import sys

import os
import socket

from functools import wraps
from datetime import datetime, timedelta
import dateutil.parser
import copy
from itertools import chain, product

from past.utils import old_div
from past.builtins import basestring

import inspect
import traceback

import sqlalchemy as sqla
from sqlalchemy import or_


from flask import redirect, url_for, request, Markup, Response, current_app, render_template
from flask.ext.admin import BaseView, expose, AdminIndexView
from flask.ext.admin.contrib.sqla import ModelView
from flask.ext.admin.actions import action
from flask.ext.login import current_user, flash, logout_user, login_required
from flask._compat import PY2

import jinja2
import markdown
import json

from wtforms import (
    widgets,
    Form, SelectField, TextAreaField, PasswordField, StringField)

from pygments import highlight, lexers
from pygments.formatters import HtmlFormatter

import airflow
from airflow import models
from airflow.settings import Session
from airflow import configuration
from airflow import login
from airflow import utils
from airflow.utils import AirflowException
from airflow.www import utils as wwwutils
from airflow import settings
from airflow.models import State

from airflow.www.forms import DateTimeForm, TreeForm

QUERY_LIMIT = 100000
CHART_LIMIT = 200000

dagbag = models.DagBag(os.path.expanduser(configuration.get('core', 'DAGS_FOLDER')))

login_required = airflow.login.login_required
current_user = airflow.login.current_user
logout_user = airflow.login.logout_user

FILTER_BY_OWNER = False
if configuration.getboolean('webserver', 'FILTER_BY_OWNER'):
    # filter_by_owner if authentication is enabled and filter_by_owner is true
    FILTER_BY_OWNER = not current_app.config['LOGIN_DISABLED']


def dag_link(v, c, m, p):
    url = url_for(
        'airflow.graph',
        dag_id=m.dag_id)
    return Markup(
        '<a href="{url}">{m.dag_id}</a>'.format(**locals()))


def log_link(v, c, m, p):
    url = url_for(
        'airflow.log',
        dag_id=m.dag_id,
        task_id=m.task_id,
        execution_date=m.execution_date.isoformat())
    return Markup(
        '<a href="{url}">'
        '    <span class="glyphicon glyphicon-book" aria-hidden="true">'
        '</span></a>').format(**locals())


def task_instance_link(v, c, m, p):
    url = url_for(
        'airflow.task',
        dag_id=m.dag_id,
        task_id=m.task_id,
        execution_date=m.execution_date.isoformat())
    url_root = url_for(
        'airflow.graph',
        dag_id=m.dag_id,
        root=m.task_id,
        execution_date=m.execution_date.isoformat())
    return Markup(
        """
        <span style="white-space: nowrap;">
        <a href="{url}">{m.task_id}</a>
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
        default_params = eval(m.default_params)
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
            args = eval(chart.default_params)
            if type(args) is not type(dict()):
                raise AirflowException('Not a dict')
        except:
            args = {}
            payload['error'] += (
                "Default params is not valid, string has to evaluate as "
                "a Python dictionary. ")

        request_dict = {k: request.args.get(k) for k in request.args}
        from airflow import macros
        args.update(request_dict)
        args['macros'] = macros
        sql = jinja2.Template(chart.sql).render(**args)
        label = jinja2.Template(chart.label).render(**args)
        payload['sql_html'] = Markup(highlight(
            sql,
            lexers.SqlLexer(),  # Lexer call
            HtmlFormatter(noclasses=True))
        )
        payload['label'] = label

        import pandas as pd
        pd.set_option('display.max_colwidth', 100)
        hook = db.get_hook()
        try:
            df = hook.get_pandas_df(wwwutils.limit_sql(sql, CHART_LIMIT, conn_type=db.conn_type))
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
            if chart_type == "datatable":
                chart.show_datatable = True
            if chart.show_datatable:
                data = df.to_dict(orient="split")
                data['columns'] = [{'title': c} for c in data['columns']]

            # Trying to convert time to something Highcharts likes
            x_col = 1 if chart.sql_layout == 'series' else 0
            if chart.x_is_date:
                try:
                    # From string to datetime
                    df[df.columns[x_col]] = pd.to_datetime(
                        df[df.columns[x_col]])
                except Exception as e:
                    raise AirflowException(str(e))
                df[df.columns[x_col]] = df[df.columns[x_col]].apply(
                    lambda x: int(x.strftime("%s")) * 1000)

            series = []
            colorAxis = None
            if chart_type == 'datatable':
                payload['data'] = data
                payload['state'] = 'SUCCESS'
                return wwwutils.json_response(payload)

            elif chart_type == 'para':
                df.rename(columns={
                    df.columns[0]: 'name',
                    df.columns[1]: 'group',
                }, inplace=True)
                return Response(
                    response=df.to_csv(index=False),
                    status=200,
                    mimetype="application/text")

            elif chart_type == 'heatmap':
                color_perc_lbound = float(
                    request.args.get('color_perc_lbound', 0))
                color_perc_rbound = float(
                    request.args.get('color_perc_rbound', 1))
                color_scheme = request.args.get('color_scheme', 'blue_red')

                if color_scheme == 'blue_red':
                    stops = [
                        [color_perc_lbound, '#00D1C1'],
                        [
                            color_perc_lbound +
                            ((color_perc_rbound - color_perc_lbound)/2),
                            '#FFFFCC'
                        ],
                        [color_perc_rbound, '#FF5A5F']
                    ]
                elif color_scheme == 'blue_scale':
                    stops = [
                        [color_perc_lbound, '#FFFFFF'],
                        [color_perc_rbound, '#2222FF']
                    ]
                elif color_scheme == 'fire':
                    diff = float(color_perc_rbound - color_perc_lbound)
                    stops = [
                        [color_perc_lbound, '#FFFFFF'],
                        [color_perc_lbound + 0.33*diff, '#FFFF00'],
                        [color_perc_lbound + 0.66*diff, '#FF0000'],
                        [color_perc_rbound, '#000000']
                    ]
                else:
                    stops = [
                        [color_perc_lbound, '#FFFFFF'],
                        [
                            color_perc_lbound +
                            ((color_perc_rbound - color_perc_lbound)/2),
                            '#888888'
                        ],
                        [color_perc_rbound, '#000000'],
                    ]

                xaxis_label = df.columns[1]
                yaxis_label = df.columns[2]
                data = []
                for row in df.itertuples():
                    data.append({
                        'x': row[2],
                        'y': row[3],
                        'value': row[4],
                    })
                x_format = '{point.x:%Y-%m-%d}' \
                    if chart.x_is_date else '{point.x}'
                series.append({
                    'data': data,
                    'borderWidth': 0,
                    'colsize': 24 * 36e5,
                    'turboThreshold': sys.float_info.max,
                    'tooltip': {
                        'headerFormat': '',
                        'pointFormat': (
                            df.columns[1] + ': ' + x_format + '<br/>' +
                            df.columns[2] + ': {point.y}<br/>' +
                            df.columns[3] + ': <b>{point.value}</b>'
                        ),
                    },
                })
                colorAxis = {
                    'stops': stops,
                    'minColor': '#FFFFFF',
                    'maxColor': '#000000',
                    'min': 50,
                    'max': 2200,
                }
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

                for col in df.columns:
                    series.append({
                        'name': col,
                        'data': [
                            (k, df[col][k])
                            for k in df[col].keys()
                            if not np.isnan(df[col][k])]
                    })
                series = [serie for serie in sorted(
                    series, key=lambda s: s['data'][0][1], reverse=True)]

            if chart_type == "stacked_area":
                stacking = "normal"
                chart_type = 'area'
            elif chart_type == "percent_area":
                stacking = "percent"
                chart_type = 'area'
            else:
                stacking = None
            hc = {
                'chart': {
                    'type': chart_type
                },
                'plotOptions': {
                    'series': {
                        'marker': {
                            'enabled': False
                        }
                    },
                    'area': {'stacking': stacking},
                },
                'title': {'text': ''},
                'xAxis': {
                    'title': {'text': xaxis_label},
                    'type': 'datetime' if chart.x_is_date else None,
                },
                'yAxis': {
                    'title': {'text': yaxis_label},
                },
                'colorAxis': colorAxis,
                'tooltip': {
                    'useHTML': True,
                    'backgroundColor': None,
                    'borderWidth': 0,
                },
                'series': series,
            }

            if chart.y_log_scale:
                hc['yAxis']['type'] = 'logarithmic'
                hc['yAxis']['minorTickInterval'] = 0.1
                if 'min' in hc['yAxis']:
                    del hc['yAxis']['min']

            payload['state'] = 'SUCCESS'
            payload['hc'] = hc
            payload['data'] = data
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
        if chart.chart_type == 'para':
            return self.render('airflow/para/para.html', chart=chart)

        sql = ""
        if chart.show_sql:
            sql = Markup(highlight(
                chart.sql,
                lexers.SqlLexer(),  # Lexer call
                HtmlFormatter(noclasses=True))
            )
        return self.render(
            'airflow/highchart.html',
            chart=chart,
            title="Airflow - Chart",
            sql=sql,
            label=chart.label,
            embed=embed)

    @expose('/dag_stats')
    #@login_required
    def dag_stats(self):
        states = [
            State.SUCCESS,
            State.RUNNING,
            State.FAILED,
            State.UPSTREAM_FAILED,
            State.UP_FOR_RETRY,
            State.QUEUED,
        ]
        task_ids = []
        dag_ids = []
        for dag in dagbag.dags.values():
            task_ids += dag.task_ids
            if not dag.is_subdag:
                dag_ids.append(dag.dag_id)
        TI = models.TaskInstance
        session = Session()
        qry = (
            session.query(TI.dag_id, TI.state, sqla.func.count(TI.task_id))
                .filter(TI.task_id.in_(task_ids))
                .filter(TI.dag_id.in_(dag_ids))
                .group_by(TI.dag_id, TI.state)
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
            for state in states:
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
        code = "".join(open(dag.full_filepath, 'r').readlines())
        title = dag.filepath
        html_code = highlight(
            code, lexers.PythonLexer(), HtmlFormatter(linenos=True))
        return self.render(
            'airflow/dag_code.html', html_code=html_code, dag=dag, title=title,
            root=request.args.get('root'),
            demo_mode=configuration.getboolean('webserver', 'demo_mode'))

    @current_app.errorhandler(404)
    def circles(self):
        return render_template(
            'airflow/circles.html', hostname=socket.gethostname()), 404

    @current_app.errorhandler(500)
    def show_traceback(self):
        from airflow import ascii as ascii_
        return render_template(
            'airflow/traceback.html',
            hostname=socket.gethostname(),
            nukular=ascii_.nukular,
            info=traceback.format_exc()), 500

    @expose('/sandbox')
    @login_required
    def sandbox(self):
        from airflow import configuration
        title = "Sandbox Suggested Configuration"
        cfg_loc = configuration.AIRFLOW_CONFIG + '.sandbox'
        f = open(cfg_loc, 'r')
        config = f.read()
        f.close()
        code_html = Markup(highlight(
            config,
            lexers.IniLexer(),  # Lexer call
            HtmlFormatter(noclasses=True))
        )
        return self.render(
            'airflow/code.html',
            code_html=code_html, title=title, subtitle=cfg_loc)

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

    @expose('/login', methods=['GET', 'POST'])
    def login(self):
        return airflow.login.login(self, request)

    @expose('/logout')
    def logout(self):
        logout_user()
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
            configuration.get('core', 'BASE_LOG_FOLDER'))
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
        session = Session()
        dttm = dateutil.parser.parse(execution_date)
        ti = session.query(TI).filter(
            TI.dag_id == dag_id, TI.task_id == task_id,
            TI.execution_date == dttm).first()
        dttm = dateutil.parser.parse(execution_date)
        form = DateTimeForm(data={'execution_date': dttm})

        if ti:
            host = ti.hostname
            log_loaded = False

            if socket.gethostname() == host:
                try:
                    f = open(loc)
                    log += "".join(f.readlines())
                    f.close()
                    log_loaded = True
                except:
                    log = "*** Log file isn't where expected.\n".format(loc)
            else:
                WORKER_LOG_SERVER_PORT = \
                    configuration.get('celery', 'WORKER_LOG_SERVER_PORT')
                url = os.path.join(
                    "http://{host}:{WORKER_LOG_SERVER_PORT}/log", log_relative
                    ).format(**locals())
                log += "*** Log file isn't local.\n"
                log += "*** Fetching here: {url}\n".format(**locals())
                try:
                    import requests
                    log += '\n' + requests.get(url).text
                    log_loaded = True
                except:
                    log += "*** Failed to fetch log file from worker.\n".format(
                        **locals())

            # try to load log backup from S3
            s3_log_folder = configuration.get('core', 'S3_LOG_FOLDER')
            if not log_loaded and s3_log_folder.startswith('s3:'):
                import boto
                s3 = boto.connect_s3()
                s3_log_loc = os.path.join(
                    configuration.get('core', 'S3_LOG_FOLDER'), log_relative)
                log += '*** Fetching log from S3: {}\n'.format(s3_log_loc)
                log += ('*** Note: S3 logs are only available once '
                        'tasks have completed.\n')
                bucket, key = s3_log_loc.lstrip('s3:/').split('/', 1)
                s3_key = boto.s3.key.Key(s3.get_bucket(bucket), key)
                if s3_key.exists():
                    log += '\n' + s3_key.get_contents_as_string().decode()
                else:
                    log += '*** No log found on S3.\n'

            session.commit()
            session.close()
        log = log.decode('utf-8') if PY2 else log

        title = "Log"

        return self.render(
            'airflow/ti_code.html',
            code=log, dag=dag, title=title, task_id=task_id,
            execution_date=execution_date, form=form)

    @expose('/task')
    @login_required
    @wwwutils.action_logging
    def task(self):
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
        task = dag.get_task(task_id)
        task = copy.copy(task)
        task.resolve_template_files()

        attributes = []
        for attr_name in dir(task):
            if not attr_name.startswith('_'):
                attr = getattr(task, attr_name)
                if type(attr) != type(self.task) and \
                                attr_name not in attr_renderer:
                    attributes.append((attr_name, str(attr)))

        title = "Task Details"
        # Color coding the special attributes that are code
        special_attrs_rendered = {}
        for attr_name in attr_renderer:
            if hasattr(task, attr_name):
                source = getattr(task, attr_name)
                special_attrs_rendered[attr_name] = attr_renderer[attr_name](source)

        return self.render(
            'airflow/task.html',
            attributes=attributes,
            task_id=task_id,
            execution_date=execution_date,
            special_attrs_rendered=special_attrs_rendered,
            form=form,
            dag=dag, title=title)

    @expose('/run')
    @login_required
    @wwwutils.action_logging
    def run(self):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        origin = request.args.get('origin')
        dag = dagbag.get_dag(dag_id)
        task = dag.get_task(task_id)

        execution_date = request.args.get('execution_date')
        execution_date = dateutil.parser.parse(execution_date)
        force = request.args.get('force') == "true"
        deps = request.args.get('deps') == "true"

        from airflow.executors import DEFAULT_EXECUTOR as executor
        from airflow.executors import CeleryExecutor
        if not isinstance(executor, CeleryExecutor):
            flash("Only works with the CeleryExecutor, sorry", "error")
            return redirect(origin)
        ti = models.TaskInstance(task=task, execution_date=execution_date)
        executor.start()
        executor.queue_task_instance(
            ti, force=force, ignore_dependencies=deps)
        executor.heartbeat()
        flash(
            "Sent {} to the message queue, "
            "it should start any moment now.".format(ti))
        return redirect(origin)

    @expose('/clear')
    @login_required
    @wwwutils.action_logging
    def clear(self):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        origin = request.args.get('origin')
        dag = dagbag.get_dag(dag_id)
        task = dag.get_task(task_id)

        execution_date = request.args.get('execution_date')
        execution_date = dateutil.parser.parse(execution_date)
        confirmed = request.args.get('confirmed') == "true"
        upstream = request.args.get('upstream') == "true"
        downstream = request.args.get('downstream') == "true"
        future = request.args.get('future') == "true"
        past = request.args.get('past') == "true"

        dag = dag.sub_dag(
            task_regex=r"^{0}$".format(task_id),
            include_downstream=downstream,
            include_upstream=upstream)

        end_date = execution_date if not future else None
        start_date = execution_date if not past else None
        if confirmed:
            count = dag.clear(
                start_date=start_date,
                end_date=end_date)

            flash("{0} task instances have been cleared".format(count))
            return redirect(origin)
        else:
            tis = dag.clear(
                start_date=start_date,
                end_date=end_date,
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
    def success(self):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        origin = request.args.get('origin')
        dag = dagbag.get_dag(dag_id)
        task = dag.get_task(task_id)

        execution_date = request.args.get('execution_date')
        execution_date = dateutil.parser.parse(execution_date)
        confirmed = request.args.get('confirmed') == "true"
        upstream = request.args.get('upstream') == "true"
        downstream = request.args.get('downstream') == "true"
        future = request.args.get('future') == "true"
        past = request.args.get('past') == "true"
        MAX_PERIODS = 1000

        # Flagging tasks as successful
        session = settings.Session()
        task_ids = [task_id]
        end_date = ((dag.latest_execution_date or datetime.now())
                    if future else execution_date)

        if 'start_date' in dag.default_args:
            start_date = dag.default_args['start_date']
        elif dag.start_date:
            start_date = dag.start_date
        else:
            start_date = execution_date

        start_date = execution_date if not past else start_date

        if downstream:
            task_ids += [
                t.task_id
                for t in task.get_flat_relatives(upstream=False)]
        if upstream:
            task_ids += [
                t.task_id
                for t in task.get_flat_relatives(upstream=True)]
        TI = models.TaskInstance

        if dag.schedule_interval == '@once':
            dates = [start_date]
        else:
            dates = dag.date_range(start_date, end_date=end_date)

        tis = session.query(TI).filter(
            TI.dag_id == dag_id,
            TI.execution_date.in_(dates),
            TI.task_id.in_(task_ids)).all()
        tis_to_change = session.query(TI).filter(
            TI.dag_id == dag_id,
            TI.execution_date.in_(dates),
            TI.task_id.in_(task_ids),
            TI.state != State.SUCCESS).all()
        tasks = list(product(task_ids, dates))
        tis_to_create = list(
            set(tasks) -
            set([(ti.task_id, ti.execution_date) for ti in tis]))

        tis_all_altered = list(chain(
            [(ti.task_id, ti.execution_date) for ti in tis_to_change],
            tis_to_create))

        if len(tis_all_altered) > MAX_PERIODS:
            flash("Too many tasks at once (>{0})".format(
                MAX_PERIODS), 'error')
            return redirect(origin)

        if confirmed:
            for ti in tis_to_change:
                ti.state = State.SUCCESS
            session.commit()

            for task_id, task_execution_date in tis_to_create:
                ti = TI(
                    task=dag.get_task(task_id),
                    execution_date=task_execution_date,
                    state=State.SUCCESS)
                session.add(ti)
                session.commit()

            session.commit()
            session.close()
            flash("Marked success on {} task instances".format(
                len(tis_all_altered)))

            return redirect(origin)
        else:
            if not tis_all_altered:
                flash("No task instances to mark as successful", 'error')
                response = redirect(origin)
            else:
                tis = []
                for task_id, task_execution_date in tis_all_altered:
                    tis.append(TI(
                        task=dag.get_task(task_id),
                        execution_date=task_execution_date,
                        state=State.SUCCESS))
                details = "\n".join([str(t) for t in tis])

                response = self.render(
                    'airflow/confirm.html',
                    message=(
                        "Here's the list of task instances you are about "
                        "to mark as successful:"),
                    details=details,)
            return response

    @expose('/tree')
    @login_required
    @wwwutils.gzipped
    @wwwutils.action_logging
    def tree(self):
        dag_id = request.args.get('dag_id')
        blur = configuration.getboolean('webserver', 'demo_mode')
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
            dr.execution_date: utils.alchemy_to_dict(dr) for dr in dag_runs}

        tis = dag.get_task_instances(
                session, start_date=min_date, end_date=base_date)
        dates = sorted(list({ti.execution_date for ti in tis}))
        max_date = max([ti.execution_date for ti in tis]) if dates else None
        task_instances = {}
        for ti in tis:
            tid = utils.alchemy_to_dict(ti)
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

            return {
                'name': task.task_id,
                'instances': [
                        task_instances.get((task.task_id, d)) or {
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

        data = json.dumps(data, indent=4, default=utils.json_ser)
        session.commit()
        session.close()

        form = TreeForm(data={'base_date': max_date, 'num_runs': num_runs})
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
        blur = configuration.getboolean('webserver', 'demo_mode')
        arrange = request.args.get('arrange', "LR")
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
        drs = session.query(DR).filter_by(dag_id=dag_id).all()
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
            ti.task_id: utils.alchemy_to_dict(ti)
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
        doc_md = markdown.markdown(dag.doc_md) if hasattr(dag, 'doc_md') else ''

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
        days = int(request.args.get('days', 30))
        dag = dagbag.get_dag(dag_id)
        from_date = (datetime.today()-timedelta(days)).date()
        from_date = datetime.combine(from_date, datetime.min.time())

        root = request.args.get('root')
        if root:
            dag = dag.sub_dag(
                task_regex=root,
                include_upstream=True,
                include_downstream=False)

        all_data = []
        for task in dag.tasks:
            data = []
            for ti in task.get_task_instances(session, from_date):
                if ti.duration:
                    data.append([
                        ti.execution_date.isoformat(),
                        float(ti.duration) / (60*60)
                    ])
            if data:
                all_data.append({'data': data, 'name': task.task_id})

        session.commit()
        session.close()

        return self.render(
            'airflow/chart.html',
            dag=dag,
            data=json.dumps(all_data),
            chart_options={'yAxis': {'title': {'text': 'hours'}}},
            height="700px",
            demo_mode=configuration.getboolean('webserver', 'demo_mode'),
            root=root,
        )

    @expose('/landing_times')
    @login_required
    @wwwutils.action_logging
    def landing_times(self):
        session = settings.Session()
        dag_id = request.args.get('dag_id')
        days = int(request.args.get('days', 30))
        dag = dagbag.get_dag(dag_id)
        from_date = (datetime.today()-timedelta(days)).date()
        from_date = datetime.combine(from_date, datetime.min.time())

        root = request.args.get('root')
        if root:
            dag = dag.sub_dag(
                task_regex=root,
                include_upstream=True,
                include_downstream=False)

        all_data = []
        for task in dag.tasks:
            data = []
            for ti in task.get_task_instances(session, from_date):
                if ti.end_date:
                    ts = ti.execution_date
                    if dag.schedule_interval:
                        ts = dag.following_schedule(ts)
                    secs = old_div((ti.end_date - ts).total_seconds(), 60*60)
                    data.append([ti.execution_date.isoformat(), secs])
            all_data.append({'data': data, 'name': task.task_id})

        session.commit()
        session.close()

        return self.render(
            'airflow/chart.html',
            dag=dag,
            data=json.dumps(all_data),
            height="700px",
            chart_options={'yAxis': {'title': {'text': 'hours after 00:00'}}},
            demo_mode=configuration.getboolean('webserver', 'demo_mode'),
            root=root,
        )

    @expose('/paused')
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
        return redirect('/')

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
        demo_mode = configuration.getboolean('webserver', 'demo_mode')

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
            ti
            for ti in dag.get_task_instances(session, dttm, dttm)
            if ti.start_date]
        tis = sorted(tis, key=lambda ti: ti.start_date)
        tasks = []
        data = []
        for i, ti in enumerate(tis):
            end_date = ti.end_date or datetime.now()
            tasks += [ti.task_id]
            color = State.color(ti.state)
            data.append({
                'x': i,
                'low': int(ti.start_date.strftime('%s')) * 1000,
                'high': int(end_date.strftime('%s')) * 1000,
                'color': color,
            })
        height = (len(tis) * 25) + 50
        session.commit()
        session.close()

        hc = {
            'chart': {
                'type': 'columnrange',
                'inverted': True,
                'height': height,
            },
            'xAxis': {'categories': tasks},
            'yAxis': {'type': 'datetime'},
            'title': {
                'text': None
            },
            'plotOptions': {
                'series': {
                    'cursor': 'pointer',
                    'minPointLength': 4,
                },
            },
            'legend': {
                'enabled': False
            },
            'series': [{
                'data': data
            }]
        }
        return self.render(
            'airflow/gantt.html',
            dag=dag,
            execution_date=dttm.isoformat(),
            form=form,
            hc=json.dumps(hc, indent=4),
            height=height,
            demo_mode=demo_mode,
            root=root,
        )

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


class HomeView(AdminIndexView):
    @expose("/")
    @login_required
    def index(self):
        session = Session()
        DM = models.DagModel
        qry = None
        # filter the dags if filter_by_owner and current user is not superuser
        do_filter = FILTER_BY_OWNER and (not current_user.is_superuser())
        if do_filter:
            qry = (
                session.query(DM)
                    .filter(
                    ~DM.is_subdag, DM.is_active,
                    DM.owners == current_user.username)
                    .all()
            )
        else:
            qry = session.query(DM).filter(~DM.is_subdag, DM.is_active).all()
        orm_dags = {dag.dag_id: dag for dag in qry}
        import_errors = session.query(models.ImportError).all()
        for ie in import_errors:
            flash(
                "Broken DAG: [{ie.filename}] {ie.stacktrace}".format(ie=ie),
                "error")
        session.expunge_all()
        session.commit()
        session.close()
        dags = dagbag.dags.values()
        if do_filter:
            dags = {
                dag.dag_id: dag
                for dag in dags
                if (
                    dag.owner == current_user.username and (not dag.parent_dag)
                )
                }
        else:
            dags = {dag.dag_id: dag for dag in dags if not dag.parent_dag}
        all_dag_ids = sorted(set(orm_dags.keys()) | set(dags.keys()))
        return self.render(
            'airflow/dags.html',
            dags=dags,
            orm_dags=orm_dags,
            all_dag_ids=all_dag_ids)


class QueryView(wwwutils.DataProfilingMixin, BaseView):
    @expose('/')
    @wwwutils.gzipped
    def query(self):
        session = settings.Session()
        dbs = session.query(models.Connection).order_by(
            models.Connection.conn_id).all()
        session.expunge_all()
        db_choices = list(
            ((db.conn_id, db.conn_id) for db in dbs if db.get_hook()))
        conn_id_str = request.args.get('conn_id')
        csv = request.args.get('csv') == "true"
        sql = request.args.get('sql')

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
            ('para', 'Parallel Coordinates'),
            ('column', 'Column Chart'),
            ('area', 'Overlapping Area Chart'),
            ('stacked_area', 'Stacked Area Chart'),
            ('percent_area', 'Percent Area Chart'),
            ('heatmap', 'Heatmap'),
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

'''
# For debugging / troubleshooting
mv = KnowEventTypeView(
    models.KnownEventType,
    Session, name="Known Event Types", category="Manage")
admin.add_view(mv)
class DagPickleView(SuperUserMixin, ModelView):
    pass
mv = DagPickleView(
    models.DagPickle,
    Session, name="Pickles", category="Manage")
admin.add_view(mv)
'''


class VariableView(wwwutils.LoginMixin, AirflowModelView):
    verbose_name = "Variable"
    verbose_name_plural = "Variables"
    column_list = ('key',)
    column_filters = ('key', 'val')
    column_searchable_list = ('key', 'val')
    form_widget_args = {
        'val': {
            'rows': 20,
        }
    }


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
    can_delete = True
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
    column_list = (
        'state', 'dag_id', 'execution_date', 'run_id', 'external_trigger')
    column_filters = column_list
    column_searchable_list = ('dag_id', 'state', 'run_id')
    column_formatters = dict(
        execution_date=datetime_f,
        state=state_f,
        start_date=datetime_f,
        dag_id=dag_link)
    @action(
        'set_running', "Set state to 'running'", None)
    @utils.provide_session
    def action_set_running(self, ids, session=None):
        try:
            DR = models.DagRun
            count = 0
            for dr in session.query(DR).filter(DR.id.in_(ids)).all():
                count += 1
            flash("{} dag runs were set to 'running'".format(ids))
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
        log=log_link, task_id=task_instance_link,
        hostname=nobr_f,
        state=state_f,
        execution_date=datetime_f,
        start_date=datetime_f,
        end_date=datetime_f,
        queued_dttm=datetime_f,
        dag_id=dag_link, duration=duration_f)
    column_searchable_list = ('dag_id', 'task_id', 'state')
    column_default_sort = ('start_date', True)
    column_list = (
        'state', 'dag_id', 'task_id', 'execution_date', 'operator',
        'start_date', 'end_date', 'duration', 'job_id', 'hostname',
        'unixname', 'priority_weight', 'queue', 'queued_dttm', 'pool', 'log')
    can_delete = True
    page_size = 500


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
    )
    verbose_name = "Connection"
    verbose_name_plural = "Connections"
    column_default_sort = ('conn_id', False)
    column_list = ('conn_id', 'conn_type', 'host', 'port', 'is_encrypted',)
    form_overrides = dict(_password=PasswordField)
    form_widget_args = {
        'is_encrypted': {'disabled': True},
    }
    # Used to customized the form, the forms elements get rendered
    # and results are stored in the extra field as json. All of these
    # need to be prefixed with extra__ and then the conn_type ___ as in
    # extra__{conn_type}__name. You can also hide form elements and rename
    # others from the connection_form.js file
    form_extra_fields = {
        'extra__jdbc__drv_path' : StringField('Driver Path'),
        'extra__jdbc__drv_clsname': StringField('Driver Class'),
    }
    form_choices = {
        'conn_type': [
            ('ftp', 'FTP',),
            ('hdfs', 'HDFS',),
            ('http', 'HTTP',),
            ('hive_cli', 'Hive Client Wrapper',),
            ('hive_metastore', 'Hive Metastore Thrift',),
            ('hiveserver2', 'Hive Server 2 Thrift',),
            ('jdbc', 'Jdbc Connection',),
            ('mysql', 'MySQL',),
            ('postgres', 'Postgres',),
            ('oracle', 'Oracle',),
            ('vertica', 'Vertica',),
            ('presto', 'Presto',),
            ('s3', 'S3',),
            ('samba', 'Samba',),
            ('sqlite', 'Sqlite',),
            ('mssql', 'Microsoft SQL Server'),
            ('mesos_framework-id', 'Mesos Framework ID'),
        ]
    }

    def on_model_change(self, form, model, is_created):
        formdata = form.data
        if formdata['conn_type'] in ['jdbc']:
            extra = {
                key:formdata[key]
                for key in self.form_extra_fields.keys() if key in formdata}
            model.extra = json.dumps(extra)

    @classmethod
    def alert_fernet_key(cls):
        return not configuration.has_option('core', 'fernet_key')

    @classmethod
    def is_secure(self):
        """
        Used to display a message in the Connection list view making it clear
        that the passwords can't be encrypted.
        """
        is_secure = False
        try:
            import cryptography
            configuration.get('core', 'fernet_key')
            is_secure = True
        except:
            pass
        return is_secure

    def on_form_prefill(self, form, id):
        try:
            d = json.loads(form.data.get('extra', '{}'))
        except Exception as e:
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


class ConfigurationView(wwwutils.SuperUserMixin, BaseView):
    @expose('/')
    def conf(self):
        from airflow import configuration
        raw = request.args.get('raw') == "true"
        title = "Airflow Configuration"
        subtitle = configuration.AIRFLOW_CONFIG
        if configuration.getboolean("webserver", "expose_config"):
            with open(configuration.AIRFLOW_CONFIG, 'r') as f:
                config = f.read()
        else:
            config = (
                "# You Airflow administrator chose not to expose the "
                "configuration, most likely for security reasons.")
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
                'airflow/code.html',
                pre_subtitle=settings.HEADER + "  v" + airflow.__version__,
                code_html=code_html, title=title, subtitle=subtitle)


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

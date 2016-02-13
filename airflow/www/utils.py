from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import object
from cgi import escape
from io import BytesIO as IO
import functools
import gzip
import dateutil.parser as dateparser
import json
import os
from flask import after_this_request, request, Response
from flask.ext.login import current_user
from jinja2 import Template
import wtforms
from wtforms.compat import text_type

from airflow import configuration, models, settings, utils
AUTHENTICATE = configuration.getboolean('webserver', 'AUTHENTICATE')


class LoginMixin(object):
    def is_accessible(self):
        return (
            not AUTHENTICATE or (
                not current_user.is_anonymous() and
                current_user.is_authenticated()
            )
        )


class SuperUserMixin(object):
    def is_accessible(self):
        return (
            not AUTHENTICATE or
            (not current_user.is_anonymous() and current_user.is_superuser())
        )


class DataProfilingMixin(object):
    def is_accessible(self):
        return (
            not AUTHENTICATE or
            (not current_user.is_anonymous() and current_user.data_profiling())
        )


def limit_sql(sql, limit, conn_type):
    sql = sql.strip()
    sql = sql.rstrip(';')
    if sql.lower().startswith("select"):
        if conn_type in ['mssql']:
            sql = """\
            SELECT TOP {limit} * FROM (
            {sql}
            ) qry
            """.format(**locals())
        elif conn_type in ['oracle']:
            sql = """\
            SELECT * FROM (
            {sql}
            ) qry
            WHERE ROWNUM <= {limit}
            """.format(**locals())
        else:
            sql = """\
            SELECT * FROM (
            {sql}
            ) qry
            LIMIT {limit}
            """.format(**locals())
    return sql


def action_logging(f):
    '''
    Decorator to log user actions
    '''
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        session = settings.Session()

        if current_user and hasattr(current_user, 'username'):
            user = current_user.username
        else:
            user = 'anonymous'

        log = models.Log(
            event=f.__name__,
            task_instance=None,
            owner=user,
            extra=str(request.args.items()),
            task_id=request.args.get('task_id'),
            dag_id=request.args.get('dag_id'))

        if 'execution_date' in request.args:
            log.execution_date = dateparser.parse(
                request.args.get('execution_date'))

        session.add(log)
        session.commit()

        return f(*args, **kwargs)

    return wrapper


def notify_owner(f):
    '''
    Decorator to notify owner of actions taken on their DAGs by others
    '''
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        """
        if request.args.get('confirmed') == "true":
            dag_id = request.args.get('dag_id')
            task_id = request.args.get('task_id')
            dagbag = models.DagBag(
                os.path.expanduser(configuration.get('core', 'DAGS_FOLDER')))

            dag = dagbag.get_dag(dag_id)
            task = dag.get_task(task_id)

            if current_user and hasattr(current_user, 'username'):
                user = current_user.username
            else:
                user = 'anonymous'

            if task.owner != user:
                subject = (
                    'Actions taken on DAG {0} by {1}'.format(
                        dag_id, user))
                items = request.args.items()
                content = Template('''
                    action: <i>{{ f.__name__ }}</i><br>
                    <br>
                    <b>Parameters</b>:<br>
                    <table>
                    {% for k, v in items %}
                        {% if k != 'origin' %}
                            <tr>
                                <td>{{ k }}</td>
                                <td>{{ v }}</td>
                            </tr>
                        {% endif %}
                    {% endfor %}
                    </table>
                    ''').render(**locals())
                if task.email:
                    utils.send_email(task.email, subject, content)
        """
        return f(*args, **kwargs)
    return wrapper


def json_response(obj):
    """
    returns a json response from a json serializable python object
    """
    return Response(
        response=json.dumps(
            obj, indent=4, cls=utils.AirflowJsonEncoder),
        status=200,
        mimetype="application/json")

def gzipped(f):
    '''
    Decorator to make a view compressed
    '''
    @functools.wraps(f)
    def view_func(*args, **kwargs):
        @after_this_request
        def zipper(response):
            accept_encoding = request.headers.get('Accept-Encoding', '')

            if 'gzip' not in accept_encoding.lower():
                return response

            response.direct_passthrough = False

            if (response.status_code < 200 or
                response.status_code >= 300 or
                'Content-Encoding' in response.headers):
                return response
            gzip_buffer = IO()
            gzip_file = gzip.GzipFile(mode='wb',
                                      fileobj=gzip_buffer)
            gzip_file.write(response.data)
            gzip_file.close()

            response.data = gzip_buffer.getvalue()
            response.headers['Content-Encoding'] = 'gzip'
            response.headers['Vary'] = 'Accept-Encoding'
            response.headers['Content-Length'] = len(response.data)

            return response

        return f(*args, **kwargs)

    return view_func


def make_cache_key(*args, **kwargs):
    '''
    Used by cache to get a unique key per URL
    '''
    path = request.path
    args = str(hash(frozenset(request.args.items())))
    return (path + args).encode('ascii', 'ignore')


class AceEditorWidget(wtforms.widgets.TextArea):
    """
    Renders an ACE code editor.
    """
    def __call__(self, field, **kwargs):
        kwargs.setdefault('id', field.id)
        html = '''
        <div id="{el_id}" style="height:100px;">{contents}</div>
        <textarea
            id="{el_id}_ace" name="{form_name}"
            style="display:none;visibility:hidden;">
        </textarea>
        '''.format(
            el_id=kwargs.get('id', field.id),
            contents=escape(text_type(field._value())),
            form_name=field.id,
        )
        return wtforms.widgets.core.HTMLString(html)

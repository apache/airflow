from cgi import escape
from cStringIO import StringIO as IO
import gzip
import functools

from flask import after_this_request, request
from flask_login import current_user
import wtforms
from wtforms.compat import text_type

from airflow.configuration import conf
AUTHENTICATE = conf.getboolean('webserver', 'AUTHENTICATE')


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
        else:
            sql = """\
            SELECT * FROM (
            {sql}
            ) qry
            LIMIT {limit}
            """.format(**locals())
    return sql

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

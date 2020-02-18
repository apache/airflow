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

import functools
import gzip
from io import BytesIO as IO

import pendulum
from flask import after_this_request, flash, g, redirect, request, url_for

from airflow.models import Log
from airflow.utils.session import create_session


def action_logging(f):
    """
    Decorator to log user actions
    """
    @functools.wraps(f)
    def wrapper(*args, **kwargs):

        with create_session() as session:
            if g.user.is_anonymous:
                user = 'anonymous'
            else:
                user = g.user.username

            log = Log(
                event=f.__name__,
                task_instance=None,
                owner=user,
                extra=str(list(request.values.items())),
                task_id=request.values.get('task_id'),
                dag_id=request.values.get('dag_id'))

            if 'execution_date' in request.values:
                log.execution_date = pendulum.parse(
                    request.values.get('execution_date'))

            session.add(log)

        return f(*args, **kwargs)

    return wrapper


def gzipped(f):
    """
    Decorator to make a view compressed
    """
    @functools.wraps(f)
    def view_func(*args, **kwargs):
        @after_this_request
        def zipper(response):  # pylint: disable=unused-variable
            accept_encoding = request.headers.get('Accept-Encoding', '')

            if 'gzip' not in accept_encoding.lower():
                return response

            response.direct_passthrough = False

            if (response.status_code < 200 or response.status_code >= 300 or
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


def has_dag_access(**dag_kwargs):
    """
    Decorator to check whether the user has read / write permission on the dag.
    """
    def decorator(f):
        @functools.wraps(f)
        def wrapper(self, *args, **kwargs):
            has_access = self.appbuilder.sm.has_access
            dag_id = request.values.get('dag_id')
            # if it is false, we need to check whether user has write access on the dag
            can_dag_edit = dag_kwargs.get('can_dag_edit', False)

            # 1. check whether the user has can_dag_edit permissions on all_dags
            # 2. if 1 false, check whether the user
            #    has can_dag_edit permissions on the dag
            # 3. if 2 false, check whether it is can_dag_read view,
            #    and whether user has the permissions
            if (
                has_access('can_dag_edit', 'all_dags') or
                has_access('can_dag_edit', dag_id) or (not can_dag_edit and
                                                       (has_access('can_dag_read',
                                                                   'all_dags') or
                                                        has_access('can_dag_read',
                                                                   dag_id)))):
                return f(self, *args, **kwargs)
            else:
                flash("Access is Denied", "danger")
                return redirect(url_for(self.appbuilder.sm.auth_view.
                                        __class__.__name__ + ".login"))
        return wrapper
    return decorator

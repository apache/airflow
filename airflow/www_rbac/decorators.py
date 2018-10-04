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

import gzip
import functools
import pendulum
from io import BytesIO as IO
from flask import after_this_request, request, g
from airflow import models, settings


def action_logging(f):
    """
    Decorator to log user actions
    """
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        session = settings.Session()
        if g.user.is_anonymous:
            user = 'anonymous'
        else:
            user = g.user.username

        log = models.Log(
            event=f.__name__,
            task_instance=None,
            owner=user,
            extra=str(list(request.args.items())),
            task_id=request.args.get('task_id'),
            dag_id=request.args.get('dag_id'))

        if 'execution_date' in request.args:
            log.execution_date = pendulum.parse(
                request.args.get('execution_date'))

        session.add(log)
        session.commit()

        return f(*args, **kwargs)

    return wrapper


def gzipped(f):
    """
    Decorator to make a view compressed
    """
    @functools.wraps(f)
    def view_func(*args, **kwargs):
        @after_this_request
        def zipper(response):
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

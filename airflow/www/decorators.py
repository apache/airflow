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
import warnings
from typing import Callable, TypeVar, cast

import pendulum
from flask import after_this_request, current_app, g, request

from airflow.models import Log
from airflow.utils.session import create_session

T = TypeVar("T", bound=Callable)  # pylint: disable=invalid-name


def action_logging(f: T) -> T:
    """Decorator to log user actions"""

    @functools.wraps(f)
    def wrapper(*args, **kwargs):

        with create_session() as session:
            if g.user.is_anonymous:
                user = 'anonymous'
            else:
                user = g.user.username

            fields_skip_logging = {'csrf_token', '_csrf_token'}
            log = Log(
                event=f.__name__,
                task_instance=None,
                owner=user,
                extra=str([(k, v) for k, v in request.values.items() if k not in fields_skip_logging]),
                task_id=request.values.get('task_id'),
                dag_id=request.values.get('dag_id'),
            )

            if 'execution_date' in request.values:
                log.execution_date = pendulum.parse(request.values.get('execution_date'), strict=False)

            session.add(log)

        return f(*args, **kwargs)

    return cast(T, wrapper)


def compressed(f: T) -> T:
    """Decorator to make a view compressed"""

    @functools.wraps(f)
    def view_func(*args, **kwargs):
        def compressor(response):
            compress = current_app.extensions['compress']
            return compress.after_request(response)

        after_this_request(compressor)

        return f(*args, **kwargs)

    return view_func


def gzipped(*args, **kwargs) -> T:
    """This function is deprecated. Please use `airflow.www.decorators.compressed`."""
    warnings.warn(
        "This function is deprecated. Please use `airflow.www.decorators.compressed`.",
        DeprecationWarning,
        stacklevel=2,
    )

    return compressed(*args, **kwargs)

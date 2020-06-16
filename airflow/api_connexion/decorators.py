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

from flask import g, request

from airflow.models import Log
from airflow.utils import timezone
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
                log.execution_date = timezone.parse(
                    request.values.get('execution_date'))

            session.add(log)

        return f(*args, **kwargs)

    return wrapper

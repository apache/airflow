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

from airflow.exceptions import AirflowBadRequest
from airflow.utils import timezone


def validate_datetime(field, date_value):
    try:
        if isinstance(date_value, str):
            return timezone.parse(date_value)
    except ValueError:
        error_message = (
            "Given '{}', '{}', could not be identified "
            "as a date. Example date format: 2015-11-16T14:34:15+00:00"
        ).format(field, date_value)
        raise AirflowBadRequest(error_message)


def datetime(field):
    def handler(func):
        def callback(*args, **kwargs):
            if field in kwargs:
                kwargs[field] = validate_datetime(field, kwargs[field])
            return func(*args, **kwargs)

        return callback

    return handler


def body_var(field):
    def handler(func):
        def callback(*args, **kwargs):
            if 'body' in kwargs:
                kwargs[field] = kwargs['body']
                del kwargs['body']
            return func(*args, **kwargs)

        return callback

    return handler


def body_to_vars():
    def handler(func):
        def callback(*args, **kwargs):
            if 'body' in kwargs:
                kwargs = kwargs['body']
            return func(*args, **kwargs)

        return callback

    return handler

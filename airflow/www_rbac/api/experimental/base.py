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
from flask import jsonify
from flask.views import MethodView

import airflow.api
from airflow import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin

_log = LoggingMixin().log
requires_authentication = airflow.api.api_auth.requires_authentication


def json_response(func):
    """
    Converts Response to JSON, using flask's jsonify()
    :param func:
    :return:
    """

    def decorator(*args, **kwargs):
        try:
            response = jsonify(func(*args, **kwargs))
        except AirflowException as err:
            _log.error(err)
            response = jsonify(error="{}".format(err))
            response.status_code = err.status_code

        return response

    return decorator


class BaseEndpointView(MethodView):
    decorators = [json_response, requires_authentication]

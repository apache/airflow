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


from builtins import str

from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class HttpSensor(BaseSensorOperator):
    """
    Executes a HTTP GET statement and returns False on failure caused by
    404 Not Found or `response_check` returning False.

    HTTP Error codes other than 404 (like 403) or Connection Refused Error
    would fail the sensor itself directly (no more poking).

    :param http_conn_id: The connection to run the sensor against
    :type http_conn_id: string
    :param method: The HTTP request method to use
    :type method: string
    :param endpoint: The relative part of the full url
    :type endpoint: string
    :param request_params: The parameters to be added to the GET url
    :type request_params: a dictionary of string key/value pairs
    :param headers: The HTTP headers to be added to the GET request
    :type headers: a dictionary of string key/value pairs
    :param response_check: A check against the 'requests' response object.
        Returns True for 'pass' and False otherwise.
    :type response_check: A lambda or defined function.
    :param extra_options: Extra options for the 'requests' library, see the
        'requests' documentation (options to modify timeout, ssl, etc.)
    :type extra_options: A dictionary of options, where key is string and value
        depends on the option that's being modified.
    """

    template_fields = ('endpoint', 'request_params')

    @apply_defaults
    def __init__(self,
                 endpoint,
                 http_conn_id='http_default',
                 method='GET',
                 request_params=None,
                 headers=None,
                 response_check=None,
                 extra_options=None, *args, **kwargs):
        super(HttpSensor, self).__init__(*args, **kwargs)
        self.endpoint = endpoint
        self.http_conn_id = http_conn_id
        self.request_params = request_params or {}
        self.headers = headers or {}
        self.extra_options = extra_options or {}
        self.response_check = response_check

        self.hook = HttpHook(
            method=method,
            http_conn_id=http_conn_id)

    def poke(self, context):
        self.log.info('Poking: %s', self.endpoint)
        try:
            response = self.hook.run(self.endpoint,
                                     data=self.request_params,
                                     headers=self.headers,
                                     extra_options=self.extra_options)
            if self.response_check:
                # run content check on response
                return self.response_check(response)
        except AirflowException as ae:
            if str(ae).startswith("404"):
                return False

            raise ae

        return True

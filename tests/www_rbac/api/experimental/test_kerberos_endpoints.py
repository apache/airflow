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

import json
import mock
import os
import socket
import unittest

from datetime import datetime

from airflow.configuration import conf
from airflow.api.auth.backend.kerberos_auth import CLIENT_AUTH
from airflow.www_rbac import app as application
from tests.test_utils.config import conf_vars


@unittest.skipIf('KRB5_KTNAME' not in os.environ,
                 'Skipping Kerberos API tests due to missing KRB5_KTNAME')
@conf_vars({('api', 'auth_backend'): 'airflow.contrib.auth.backends.kerberos_auth'})
class ApiKerberosTests(unittest.TestCase):
    def setUp(self):
        conf.set("kerberos",
                 "keytab",
                 os.environ['KRB5_KTNAME'])

        self.app, _ = application.create_app(testing=True)

    def test_trigger_dag(self):
        with self.app.test_client() as c:
            url_template = '/api/experimental/dags/{}/dag_runs'
            response = c.post(
                url_template.format('example_bash_operator'),
                data=json.dumps(dict(run_id='my_run' + datetime.now().isoformat())),
                content_type="application/json"
            )
            self.assertEqual(401, response.status_code)

            response.url = 'http://{}'.format(socket.getfqdn())

            class Request(object):
                headers = {}

            response.request = Request()
            response.content = ''
            response.raw = mock.MagicMock()
            response.connection = mock.MagicMock()
            response.connection.send = mock.MagicMock()

            # disable mutual authentication for testing
            CLIENT_AUTH.mutual_authentication = 3

            # case can influence the results
            CLIENT_AUTH.hostname_override = socket.getfqdn()

            CLIENT_AUTH.handle_response(response)
            self.assertIn('Authorization', response.request.headers)

            response2 = c.post(
                url_template.format('example_bash_operator'),
                data=json.dumps(dict(run_id='my_run' + datetime.now().isoformat())),
                content_type="application/json",
                headers=response.request.headers
            )
            self.assertEqual(200, response2.status_code)

    def test_unauthorized(self):
        with self.app.test_client() as c:
            url_template = '/api/experimental/dags/{}/dag_runs'
            response = c.post(
                url_template.format('example_bash_operator'),
                data=json.dumps(dict(run_id='my_run' + datetime.now().isoformat())),
                content_type="application/json"
            )

            self.assertEqual(401, response.status_code)

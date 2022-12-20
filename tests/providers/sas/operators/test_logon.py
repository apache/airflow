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

from airflow.providers.sas._utils.logon import create_session_for_connection
from unittest.mock import patch, ANY, Mock
import requests


class TestSasLogon:

    @patch("requests.Session")
    @patch("requests.post")
    @patch("airflow.hooks.base.BaseHook.get_connection")
    def test_execute_sas_logon(self, bh_mock, req_mock, sess_mock):
        bh_ret = Mock()
        bh_mock.return_value = bh_ret
        bh_ret.extra_dejson = {"token": "", "client_id": "", "client_secret": ""}
        bh_ret.login = "user"
        bh_ret.password = "pass"
        bh_ret.host = "host"
        req_ret = Mock()
        req_mock.return_value = req_ret
        req_ret.json.return_value = {'access_token': 'tok'}
        req_ret.status_code = 200

        r = create_session_for_connection("SAS")

        req_mock.assert_called_with('host/SASLogon/oauth/token',
                                    data={'grant_type': 'password', 'username': 'user',
                                          'password': 'pass'}, verify=False,
                                    headers={'Authorization': 'Basic c2FzLmNsaTo='})

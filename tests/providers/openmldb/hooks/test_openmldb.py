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
import unittest

import requests_mock

from airflow.models import Connection
from airflow.providers.openmldb.hooks.openmldb import OpenMLDBHook
from airflow.utils import db


class TestOpenMLDBHook(unittest.TestCase):
    openmldb_conn_id = 'openmldb_conn_id_test'
    test_db_endpoint = 'http://127.0.0.1:9080/dbs/test_db'

    _mock_job_status_success_response_body = {'code': 0, 'msg': 'ok'}

    def setUp(self):
        db.merge_conn(
            Connection(
                conn_id='openmldb_conn_id_test', conn_type='openmldb', host='http://127.0.0.1', port=9080
            )
        )
        self.hook = OpenMLDBHook(openmldb_conn_id=self.openmldb_conn_id)

    @requests_mock.mock()
    def test_submit_offsync_job(self, m):
        m.post(self.test_db_endpoint, status_code=200, json=self._mock_job_status_success_response_body)
        resp = self.hook.submit_job('test_db', 'offsync', 'select * from t1')
        assert resp.status_code == 200
        assert resp.json() == self._mock_job_status_success_response_body


if __name__ == '__main__':
    unittest.main()

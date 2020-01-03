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

import unittest

from airflow.knative_worker import knative_task_runner


class TestTaskRunnerWorker(unittest.TestCase):
    def setUp(self) -> None:
        app = knative_task_runner.create_app()
        self.app = app.test_client()

    def test_hello(self):
        resp = self.app.get("health", query_string={'name': 'daniel'})
        self.assertEqual(resp.status_code, 200)

    def test_passing_task(self):
        dag_id = 'example_python_operator'
        task_id = 'print_the_context'
        import datetime
        start_date = datetime.datetime(year=2019, day=1, month=1)
        exec_date = int(datetime.datetime.timestamp(start_date))

        resp = self.app.get("run", query_string={"task_id": task_id,
                                                 "dag_id": dag_id,
                                                 "execution_date": exec_date}
                            )
        self.assertEqual(200, resp.status_code, resp.data)

    def test_failing_task(self):
        dag_id = 'example_pythxxxxon_operator'
        task_id = 'print_the_context'
        import datetime
        start_date = datetime.datetime(year=2019, day=1, month=1)
        exec_date = int(datetime.datetime.timestamp(start_date))

        resp = self.app.get("run", query_string={"task_id": task_id,
                                                 "dag_id": dag_id,
                                                 "execution_date": exec_date}
                            )
        self.assertEqual(500, resp.status_code)

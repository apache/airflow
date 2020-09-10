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
import unittest

from parameterized import parameterized_class

from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.models import DagBag, DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.settings import Session
from airflow.utils.state import State
from airflow.www_rbac import app as application
from tests.test_utils.config import conf_vars


@parameterized_class([
    {"dag_serialzation": "False"},
    {"dag_serialzation": "True"},
])
class TestDagRunClearEndpoint(unittest.TestCase):
    dag_serialzation = "False"

    @classmethod
    def setUpClass(cls):
        super(TestDagRunClearEndpoint, cls).setUpClass()
        session = Session()
        session.query(DagRun).delete()
        session.commit()
        session.close()
        dagbag = DagBag(include_examples=True)
        for dag in dagbag.dags.values():
            dag.sync_to_db()
            SerializedDagModel.write_dag(dag)

    def setUp(self):
        super(TestDagRunClearEndpoint, self).setUp()
        app, _ = application.create_app(session=Session, testing=True)
        self.app = app.test_client()

    def tearDown(self):
        session = Session()
        session.query(DagRun).delete()
        session.commit()
        session.close()
        super(TestDagRunClearEndpoint, self).tearDown()

    def test_clear_dag_run_success(self):
        with conf_vars(
            {("core", "store_serialized_dags"): self.dag_serialzation}
        ):
            url_template = '/api/experimental/dags/{}/clear_dag_run'
            dag_id = 'example_bash_operator'
            # Create DagRun
            run_id = 'test_get_dag_runs_success'
            dag_run = trigger_dag(
                dag_id=dag_id, run_id=run_id)

            session = Session()
            for ti in dag_run.get_task_instances():
                ti.state = State.SCHEDULED
                session.add(session.merge(ti))
            session.commit()
            session.close()

            response = self.app.post(
                url_template.format(dag_id),
                data=json.dumps({'run_id': run_id}),
                content_type="application/json")
            self.assertEqual(200, response.status_code)
            data = json.loads(response.data.decode('utf-8'))

            for ti in dag_run.get_task_instances():
                self.assertEqual(None, ti.state)
            dag_run.refresh_from_db()
            self.assertEqual(dag_run.state, State.RUNNING)


if __name__ == '__main__':
    unittest.main()

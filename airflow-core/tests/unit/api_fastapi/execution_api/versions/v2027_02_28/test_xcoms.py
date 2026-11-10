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
from __future__ import annotations

import pytest

from airflow.models.xcom import XCOM_RETURN_KEY, XComModelV2
from airflow.providers.standard.operators.python import PythonOperator

pytestmark = pytest.mark.db_test


class TestOmittedMapIndexVersioning:
    """An omitted ``map_index`` on Get XCom read -1 before the 2027-02-28 API version."""

    @pytest.fixture(autouse=True)
    def dag_run(self, dag_maker, session):
        with dag_maker(dag_id="dag", serialized=True):
            PythonOperator.partial(task_id="mapped", python_callable=str).expand(op_args=[[1], [2]])
        dag_run = dag_maker.create_dagrun(run_id="runid")
        for ti in dag_run.get_task_instances(session=session):
            session.add(XComModelV2(key=XCOM_RETURN_KEY, value=ti.region_index, task_instance_id=ti.id))
        session.commit()
        return dag_run

    def test_old_version_reads_minus_one(self, client):
        client.headers["Airflow-API-Version"] = "2026-10-30"

        response = client.get(f"/execution/xcoms/dag/runid/mapped/{XCOM_RETURN_KEY}")

        assert response.status_code == 404
        assert response.json()["detail"]["message"].startswith("XCom with key='return_value' map_index=-1")

    def test_head_version_reads_every_instance(self, client):
        response = client.get(f"/execution/xcoms/dag/runid/mapped/{XCOM_RETURN_KEY}")

        assert response.status_code == 200
        assert response.json() == {"key": XCOM_RETURN_KEY, "value": [0, 1]}

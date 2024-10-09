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
from __future__ import annotations

from unittest import mock

import pytest

from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.providers.trino.operators.trino import TrinoOperator


@pytest.mark.integration("trino")
class TestTrinoHookIntegration:
    @mock.patch.dict("os.environ", AIRFLOW_CONN_TRINO_DEFAULT="trino://airflow@trino:8080/")
    def test_should_record_records(self):
        hook = TrinoHook()
        sql = "SELECT name FROM tpch.sf1.customer ORDER BY custkey ASC LIMIT 3"
        records = hook.get_records(sql)
        assert [["Customer#000000001"], ["Customer#000000002"], ["Customer#000000003"]] == records

    @pytest.mark.integration("kerberos")
    def test_should_record_records_with_kerberos_auth(self):
        conn_url = (
            "trino://airflow@trino.example.com:7778/?"
            "auth=kerberos&kerberos__service_name=HTTP&"
            "verify=False&"
            "protocol=https"
        )
        with mock.patch.dict("os.environ", AIRFLOW_CONN_TRINO_DEFAULT=conn_url):
            hook = TrinoHook()
            sql = "SELECT name FROM tpch.sf1.customer ORDER BY custkey ASC LIMIT 3"
            records = hook.get_records(sql)
            assert [["Customer#000000001"], ["Customer#000000002"], ["Customer#000000003"]] == records

    @mock.patch.dict("os.environ", AIRFLOW_CONN_TRINO_DEFAULT="trino://airflow@trino:8080/")
    def test_openlineage_methods(self):
        op = TrinoOperator(task_id="trino_test", sql="SELECT name FROM tpch.sf1.customer LIMIT 3")
        op.execute({})
        lineage = op.get_openlineage_facets_on_start()
        assert lineage.inputs[0].namespace == "trino://trino:8080"
        assert lineage.inputs[0].name == "tpch.sf1.customer"
        assert "schema" in lineage.inputs[0].facets
        assert lineage.job_facets["sql"].query == "SELECT name FROM tpch.sf1.customer LIMIT 3"

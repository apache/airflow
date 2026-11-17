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

from datetime import datetime

from apache_polaris.sdk.management import Principal

from airflow.providers.apache.polaris.operators.principal import (
    PolarisCreatePrincipalOperator,
    PolarisDeletePrincipalOperator,
)
from airflow.providers.common.compat.sdk import DAG

from tests_common.test_utils.compat import DagSerialization


class TestPrincipalOperators:
    def test_create_returns_no_credentials(self, management_hook):
        management_hook.create_principal.return_value = Principal(name="etl", client_id="cid")
        result = PolarisCreatePrincipalOperator(
            task_id="t", principal_name="etl", properties={"team": "data"}, credential_rotation_required=True
        ).execute({})
        assert result == {"name": "etl", "clientId": "cid"}
        management_hook.create_principal.assert_called_once_with(
            "etl", properties={"team": "data"}, credential_rotation_required=True, if_exists="skip"
        )

    def test_delete(self, management_hook):
        management_hook.delete_principal.return_value = True
        assert PolarisDeletePrincipalOperator(task_id="t", principal_name="etl", if_missing="fail").execute(
            {}
        )
        management_hook.delete_principal.assert_called_once_with("etl", if_missing="fail")


def test_operators_serialize():
    with DAG("polaris_principal_operators", schedule=None, start_date=datetime(2026, 1, 1)) as dag:
        PolarisCreatePrincipalOperator(task_id="create", principal_name="{{ params.name }}")
        PolarisDeletePrincipalOperator(task_id="delete", principal_name="p")
    restored = DagSerialization.from_dict(DagSerialization.to_dict(dag))
    assert sorted(restored.task_ids) == sorted(dag.task_ids)
    assert restored.get_task("create").principal_name == "{{ params.name }}"

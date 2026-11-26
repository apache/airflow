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
from unittest import mock

import pytest
from apache_polaris.sdk.management import CatalogRole, PrincipalRole

from airflow.providers.apache.polaris.operators.rbac import (
    PolarisAssignCatalogRoleOperator,
    PolarisAssignPrincipalRoleOperator,
    PolarisCreateCatalogRoleOperator,
    PolarisCreatePrincipalRoleOperator,
    PolarisDeleteCatalogRoleOperator,
    PolarisDeletePrincipalRoleOperator,
    PolarisGrantPrivilegeOperator,
    PolarisRevokeCatalogRoleOperator,
    PolarisRevokePrincipalRoleOperator,
    PolarisRevokePrivilegeOperator,
)
from airflow.providers.common.compat.sdk import DAG, AirflowFailException

from tests_common.test_utils.compat import DagSerialization

GRANT = {"type": "catalog", "privilege": "TABLE_READ_DATA"}


class TestRbacOperators:
    def test_roles(self, management_hook):
        management_hook.create_principal_role.return_value = PrincipalRole(name="r")
        management_hook.create_catalog_role.return_value = CatalogRole(name="w")
        assert PolarisCreatePrincipalRoleOperator(task_id="a", role_name="r").execute({})["name"] == "r"
        assert (
            PolarisCreateCatalogRoleOperator(task_id="b", catalog_name="c", role_name="w").execute({})["name"]
            == "w"
        )
        PolarisDeletePrincipalRoleOperator(task_id="c", role_name="r").execute({})
        PolarisDeleteCatalogRoleOperator(task_id="d", catalog_name="c", role_name="w").execute({})
        management_hook.create_principal_role.assert_called_once_with("r", properties=None, if_exists="skip")
        management_hook.create_catalog_role.assert_called_once_with(
            "c", "w", properties=None, if_exists="skip"
        )
        management_hook.delete_principal_role.assert_called_once_with("r", if_missing="skip")
        management_hook.delete_catalog_role.assert_called_once_with("c", "w", if_missing="skip")

    def test_assignments(self, management_hook):
        PolarisAssignPrincipalRoleOperator(task_id="a", principal_name="p", principal_role_name="r").execute(
            {}
        )
        PolarisRevokePrincipalRoleOperator(task_id="b", principal_name="p", principal_role_name="r").execute(
            {}
        )
        PolarisAssignCatalogRoleOperator(
            task_id="c", principal_role_name="r", catalog_name="cat", catalog_role_name="w"
        ).execute({})
        PolarisRevokeCatalogRoleOperator(
            task_id="d", principal_role_name="r", catalog_name="cat", catalog_role_name="w"
        ).execute({})
        management_hook.assign_principal_role.assert_called_once_with("p", "r")
        management_hook.revoke_principal_role.assert_called_once_with("p", "r", if_missing="skip")
        management_hook.assign_catalog_role.assert_called_once_with("r", "cat", "w")
        management_hook.revoke_catalog_role.assert_called_once_with("r", "cat", "w", if_missing="skip")

    def test_grants_rendered_as_text_fail(self, management_hook):
        op = PolarisGrantPrivilegeOperator(
            task_id="t", catalog_name="c", catalog_role_name="w", grants="[{'type': 'catalog'}]"
        )
        with pytest.raises(AirflowFailException, match="render_template_as_native_obj"):
            op.execute({})
        management_hook.add_grant.assert_not_called()

    def test_grant_accepts_one_or_many(self, management_hook):
        one = {"type": "catalog", "privilege": "TABLE_READ_DATA"}
        two = {"type": "namespace", "namespace": ["sales"], "privilege": "TABLE_WRITE_DATA"}
        PolarisGrantPrivilegeOperator(
            task_id="a", catalog_name="c", catalog_role_name="w", grants=one
        ).execute({})
        PolarisGrantPrivilegeOperator(
            task_id="b", catalog_name="c", catalog_role_name="w", grants=[one, two]
        ).execute({})
        assert management_hook.add_grant.call_args_list == [
            mock.call("c", "w", one),
            mock.call("c", "w", one),
            mock.call("c", "w", two),
        ]

    def test_revoke(self, management_hook):
        grant = {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}
        PolarisRevokePrivilegeOperator(
            task_id="a", catalog_name="c", catalog_role_name="w", grants=[grant], cascade=True
        ).execute({})
        management_hook.revoke_grant.assert_called_once_with("c", "w", grant, cascade=True, if_missing="skip")


class TestEndToEnd:
    """Operators through the real hook and SDK against the fake server."""

    def test_revoking_an_absent_assignment_is_skipped(self, fake_polaris, polaris_conn):
        fake_polaris.route(
            "DELETE",
            "/api/management/v1/principal-roles/pr/catalog-roles/lake/cr",
            (
                400,
                {
                    "error": {
                        "message": "Operation failed: GRANT_NOT_FOUND",
                        "type": "GRANT_NOT_FOUND",
                        "code": 400,
                    }
                },
            ),
        )
        op = PolarisRevokeCatalogRoleOperator(
            task_id="t",
            principal_role_name="pr",
            catalog_name="lake",
            catalog_role_name="cr",
            polaris_conn_id=polaris_conn(),
        )
        assert op.execute({}) is False


def test_operators_serialize():
    with DAG("polaris_rbac_operators", schedule=None, start_date=datetime(2026, 1, 1)) as dag:
        PolarisCreatePrincipalRoleOperator(task_id="create_principal_role", role_name="r")
        PolarisDeletePrincipalRoleOperator(task_id="delete_principal_role", role_name="r")
        PolarisCreateCatalogRoleOperator(task_id="create_catalog_role", catalog_name="c", role_name="w")
        PolarisDeleteCatalogRoleOperator(task_id="delete_catalog_role", catalog_name="c", role_name="w")
        PolarisAssignPrincipalRoleOperator(
            task_id="assign_principal_role", principal_name="p", principal_role_name="r"
        )
        PolarisRevokePrincipalRoleOperator(
            task_id="revoke_principal_role", principal_name="p", principal_role_name="r"
        )
        PolarisAssignCatalogRoleOperator(
            task_id="assign_catalog_role", principal_role_name="r", catalog_name="c", catalog_role_name="w"
        )
        PolarisRevokeCatalogRoleOperator(
            task_id="revoke_catalog_role", principal_role_name="r", catalog_name="c", catalog_role_name="w"
        )
        PolarisGrantPrivilegeOperator(
            task_id="grant", catalog_name="c", catalog_role_name="w", grants=[GRANT]
        )
        PolarisRevokePrivilegeOperator(
            task_id="revoke", catalog_name="c", catalog_role_name="w", grants=GRANT
        )
    restored = DagSerialization.from_dict(DagSerialization.to_dict(dag))
    assert sorted(restored.task_ids) == sorted(dag.task_ids)
    assert restored.get_task("grant").grants == [GRANT]

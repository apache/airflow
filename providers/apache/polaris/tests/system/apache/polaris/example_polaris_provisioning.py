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
"""
Provision a catalog with least-privilege access for one team, then tear it all down.

Every step is idempotent, so the Dag can be re-run or retried safely. Requires a ``polaris_default``
connection whose principal can manage catalogs and roles (for example ``service_admin``).
"""

from __future__ import annotations

import datetime
import os

from airflow.providers.apache.polaris.operators.catalog import (
    PolarisCreateCatalogOperator,
    PolarisDeleteCatalogOperator,
    PolarisUpdateCatalogOperator,
)
from airflow.providers.apache.polaris.operators.principal import (
    PolarisCreatePrincipalOperator,
    PolarisDeletePrincipalOperator,
)
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
from airflow.providers.common.compat.sdk import DAG, TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "example_polaris_provisioning"
CATALOG = f"team_sales_{ENV_ID}"
BASE_LOCATION = os.environ.get("POLARIS_BASE_LOCATION", f"file:///tmp/polaris/{CATALOG}")

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    tags=["example", "polaris"],
) as dag:
    # [START howto_operator_polaris_create_catalog]
    create_catalog = PolarisCreateCatalogOperator(
        task_id="create_catalog",
        catalog={
            "type": "INTERNAL",
            "name": CATALOG,
            "properties": {"default-base-location": BASE_LOCATION},
            "storageConfigInfo": {"storageType": "FILE", "allowedLocations": [BASE_LOCATION]},
        },
    )
    # [END howto_operator_polaris_create_catalog]

    # [START howto_operator_polaris_update_catalog]
    update_catalog = PolarisUpdateCatalogOperator(
        task_id="update_catalog", catalog_name=CATALOG, properties={"owner": "sales"}
    )
    # [END howto_operator_polaris_update_catalog]

    # [START howto_operator_polaris_create_principal]
    create_principal = PolarisCreatePrincipalOperator(
        task_id="create_principal",
        principal_name=f"sales_etl_{ENV_ID}",
        properties={"team": "sales"},
    )
    # [END howto_operator_polaris_create_principal]

    # [START howto_operator_polaris_roles]
    create_principal_role = PolarisCreatePrincipalRoleOperator(
        task_id="create_principal_role", role_name=f"sales_etl_role_{ENV_ID}"
    )
    create_catalog_role = PolarisCreateCatalogRoleOperator(
        task_id="create_catalog_role", catalog_name=CATALOG, role_name="writer"
    )
    assign_principal_role = PolarisAssignPrincipalRoleOperator(
        task_id="assign_principal_role",
        principal_name=f"sales_etl_{ENV_ID}",
        principal_role_name=f"sales_etl_role_{ENV_ID}",
    )
    assign_catalog_role = PolarisAssignCatalogRoleOperator(
        task_id="assign_catalog_role",
        principal_role_name=f"sales_etl_role_{ENV_ID}",
        catalog_name=CATALOG,
        catalog_role_name="writer",
    )
    # [END howto_operator_polaris_roles]

    # [START howto_operator_polaris_grant]
    grant = PolarisGrantPrivilegeOperator(
        task_id="grant",
        catalog_name=CATALOG,
        catalog_role_name="writer",
        grants=[
            {"type": "catalog", "privilege": "TABLE_READ_DATA"},
            {"type": "catalog", "privilege": "TABLE_WRITE_DATA"},
            {"type": "catalog", "privilege": "NAMESPACE_CREATE"},
        ],
    )
    # [END howto_operator_polaris_grant]

    # [START howto_operator_polaris_revoke]
    revoke_privilege = PolarisRevokePrivilegeOperator(
        task_id="revoke_privilege",
        catalog_name=CATALOG,
        catalog_role_name="writer",
        grants=[{"type": "catalog", "privilege": "NAMESPACE_CREATE"}],
    )
    revoke_catalog_role = PolarisRevokeCatalogRoleOperator(
        task_id="revoke_catalog_role",
        principal_role_name=f"sales_etl_role_{ENV_ID}",
        catalog_name=CATALOG,
        catalog_role_name="writer",
    )
    revoke_principal_role = PolarisRevokePrincipalRoleOperator(
        task_id="revoke_principal_role",
        principal_name=f"sales_etl_{ENV_ID}",
        principal_role_name=f"sales_etl_role_{ENV_ID}",
    )
    # [END howto_operator_polaris_revoke]

    # [START howto_operator_polaris_teardown]
    delete_catalog_role = PolarisDeleteCatalogRoleOperator(
        task_id="delete_catalog_role",
        catalog_name=CATALOG,
        role_name="writer",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_principal_role = PolarisDeletePrincipalRoleOperator(
        task_id="delete_principal_role",
        role_name=f"sales_etl_role_{ENV_ID}",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_principal = PolarisDeletePrincipalOperator(
        task_id="delete_principal",
        principal_name=f"sales_etl_{ENV_ID}",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_catalog = PolarisDeleteCatalogOperator(
        task_id="delete_catalog", catalog_name=CATALOG, trigger_rule=TriggerRule.ALL_DONE
    )
    # [END howto_operator_polaris_teardown]

    create_catalog >> update_catalog >> create_catalog_role
    create_principal >> create_principal_role >> assign_principal_role
    [create_catalog_role, assign_principal_role] >> assign_catalog_role >> grant
    grant >> revoke_privilege >> revoke_catalog_role >> revoke_principal_role
    revoke_principal_role >> [delete_catalog_role, delete_principal_role, delete_principal] >> delete_catalog

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the Dag
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example Dag with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)

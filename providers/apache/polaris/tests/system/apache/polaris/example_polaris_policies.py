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
Declare a table-maintenance policy in Polaris and fan out one task per (table, policy).

Polaris stores and resolves maintenance policies but does not run them. This Dag shows the
Airflow side: ``PolarisGetApplicablePoliciesOperator(per_table=True)`` resolves the policies in
effect on every table of a namespace (inheritance applied by Polaris), and dynamic task mapping
runs one maintenance task per item. Here the mapped task turns each item into the Iceberg
procedure call with ``to_spark_sql``; a real deployment would run it on Spark.

The Dag creates its own catalog, namespace and table and removes them afterwards. It uses the ``polaris_default`` connection, whose principal
must be allowed to create catalogs.
"""

from __future__ import annotations

import datetime
import os
from typing import Any

from airflow.providers.apache.polaris.operators.catalog import (
    PolarisCreateCatalogOperator,
    PolarisDeleteCatalogOperator,
)
from airflow.providers.apache.polaris.operators.namespace import (
    PolarisCreateNamespaceOperator,
    PolarisDropNamespaceOperator,
)
from airflow.providers.apache.polaris.operators.policy import (
    PolarisAttachPolicyOperator,
    PolarisCreatePolicyOperator,
    PolarisDetachPolicyOperator,
    PolarisDropPolicyOperator,
    PolarisGetApplicablePoliciesOperator,
)
from airflow.providers.apache.polaris.operators.table import (
    PolarisCreateTableOperator,
    PolarisDropTableOperator,
)
from airflow.providers.apache.polaris.utils.maintenance import to_spark_sql
from airflow.providers.common.compat.sdk import DAG, TriggerRule, task

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "example_polaris_policies"
CATALOG = f"maintenance_{ENV_ID}"
BASE_LOCATION = os.environ.get("POLARIS_BASE_LOCATION", f"file:///tmp/polaris/{CATALOG}")
NAMESPACE = "sales"
TABLE = "orders"
POLICY = f"snapshot_expiry_{ENV_ID}"

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    tags=["example", "polaris"],
) as dag:
    create_catalog = PolarisCreateCatalogOperator(
        task_id="create_catalog",
        catalog={
            "type": "INTERNAL",
            "name": CATALOG,
            "properties": {"default-base-location": BASE_LOCATION},
            "storageConfigInfo": {"storageType": "FILE", "allowedLocations": [BASE_LOCATION]},
        },
    )
    create_namespace = PolarisCreateNamespaceOperator(
        task_id="create_namespace", catalog=CATALOG, namespace=NAMESPACE
    )
    create_table = PolarisCreateTableOperator(
        task_id="create_table",
        catalog=CATALOG,
        namespace=NAMESPACE,
        table=TABLE,
        schema={"type": "struct", "fields": [{"id": 1, "name": "id", "type": "long", "required": False}]},
    )

    # [START howto_operator_polaris_create_policy]
    create_policy = PolarisCreatePolicyOperator(
        task_id="create_policy",
        catalog=CATALOG,
        namespace=NAMESPACE,
        policy_name=POLICY,
        policy_type="system.snapshot-expiry",
        content={
            "version": "2025-02-03",
            "enable": True,
            "config": {"min_snapshot_to_keep": 10, "max_snapshot_age_days": 7},
        },
        description="Keep a week of snapshots, at least ten.",
    )
    # [END howto_operator_polaris_create_policy]

    # [START howto_operator_polaris_attach_policy]
    attach_policy = PolarisAttachPolicyOperator(
        task_id="attach_policy",
        catalog=CATALOG,
        namespace=NAMESPACE,
        policy_name=POLICY,
        target={"type": "namespace", "path": [NAMESPACE]},
    )
    # [END howto_operator_polaris_attach_policy]

    # [START howto_operator_polaris_get_applicable_policies]
    resolve_policies = PolarisGetApplicablePoliciesOperator(
        task_id="resolve_policies",
        catalog=CATALOG,
        namespace=NAMESPACE,
        policy_types=["system.snapshot-expiry"],
        per_table=True,
    )

    @task
    def run_maintenance(item: dict[str, Any]) -> str:
        # A real Dag would run this on Spark, e.g. with SparkSqlOperator.
        return to_spark_sql(item)

    maintenance = run_maintenance.expand(item=resolve_policies.output)
    # [END howto_operator_polaris_get_applicable_policies]

    @task
    def check_resolved(items: list[dict[str, Any]]) -> None:
        resolved = [(item["table"], item["policy"]["name"], item["policy"]["inherited"]) for item in items]
        if resolved != [(TABLE, POLICY, True)]:
            raise ValueError(f"Expected the namespace policy to apply to {TABLE!r}, got {resolved}")

    @task
    def check_sql(statements) -> None:
        expected = (
            f"CALL {CATALOG}.system.expire_snapshots(table => '{CATALOG}.{NAMESPACE}.{TABLE}', older_than => "
        )
        if len(statements) != 1 or not statements[0].startswith(expected):
            raise ValueError(f"Unexpected maintenance SQL: {statements}")

    # [START howto_operator_polaris_detach_and_drop_policy]
    detach_policy = PolarisDetachPolicyOperator(
        task_id="detach_policy",
        catalog=CATALOG,
        namespace=NAMESPACE,
        policy_name=POLICY,
        target={"type": "namespace", "path": [NAMESPACE]},
        trigger_rule=TriggerRule.ALL_DONE,
    )
    drop_policy = PolarisDropPolicyOperator(
        task_id="drop_policy",
        catalog=CATALOG,
        namespace=NAMESPACE,
        policy_name=POLICY,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_polaris_detach_and_drop_policy]

    drop_table = PolarisDropTableOperator(
        task_id="drop_table",
        catalog=CATALOG,
        namespace=NAMESPACE,
        table=TABLE,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    drop_namespace = PolarisDropNamespaceOperator(
        task_id="drop_namespace", catalog=CATALOG, namespace=NAMESPACE, trigger_rule=TriggerRule.ALL_DONE
    )

    delete_catalog = PolarisDeleteCatalogOperator(
        task_id="delete_catalog", catalog_name=CATALOG, trigger_rule=TriggerRule.ALL_DONE
    )

    create_catalog >> create_namespace >> create_table >> create_policy >> attach_policy >> resolve_policies
    [check_sql(maintenance), check_resolved(resolve_policies.output)] >> detach_policy
    detach_policy >> drop_policy >> drop_table >> drop_namespace >> delete_catalog

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the Dag
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example Dag with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)

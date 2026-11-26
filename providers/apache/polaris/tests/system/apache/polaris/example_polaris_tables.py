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
Manage namespaces and Iceberg tables in a Polaris catalog.

The Dag creates a catalog with a namespace and a partitioned, sorted table, updates namespace
and table properties, renames the table, drops it while keeping its files, and registers it again
from its metadata file. Teardown tasks remove everything, even
after a failure. It uses the ``polaris_default`` connection, whose principal must be allowed to
create catalogs.
"""

from __future__ import annotations

import datetime
import os

from airflow.providers.apache.polaris.hooks.polaris import PolarisCatalogHook
from airflow.providers.apache.polaris.operators.catalog import (
    PolarisCreateCatalogOperator,
    PolarisDeleteCatalogOperator,
)
from airflow.providers.apache.polaris.operators.namespace import (
    PolarisCreateNamespaceOperator,
    PolarisDropNamespaceOperator,
    PolarisUpdateNamespacePropertiesOperator,
)
from airflow.providers.apache.polaris.operators.table import (
    PolarisCreateTableOperator,
    PolarisDropTableOperator,
    PolarisRegisterTableOperator,
    PolarisRenameTableOperator,
    PolarisUpdateTablePropertiesOperator,
)
from airflow.providers.common.compat.sdk import DAG, TriggerRule, task

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "example_polaris_tables"
CATALOG = f"tables_{ENV_ID}"
BASE_LOCATION = os.environ.get("POLARIS_BASE_LOCATION", f"file:///tmp/polaris/{CATALOG}")

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

    # [START howto_operator_polaris_create_namespace]
    create_namespace = PolarisCreateNamespaceOperator(
        task_id="create_namespace",
        catalog=CATALOG,
        namespace="sales",
        properties={"owner": "data-eng"},
    )
    # [END howto_operator_polaris_create_namespace]

    # [START howto_operator_polaris_update_namespace_properties]
    update_namespace_properties = PolarisUpdateNamespacePropertiesOperator(
        task_id="update_namespace_properties",
        catalog=CATALOG,
        namespace="sales",
        updates={"owner": "sales-eng", "tier": "gold"},
    )
    # [END howto_operator_polaris_update_namespace_properties]

    # [START howto_operator_polaris_create_table]
    create_table = PolarisCreateTableOperator(
        task_id="create_table",
        catalog=CATALOG,
        namespace="sales",
        table="orders",
        schema={
            "type": "struct",
            "fields": [
                {"id": 1, "name": "order_id", "type": "long", "required": True},
                {"id": 2, "name": "order_date", "type": "date", "required": False},
                {"id": 3, "name": "amount", "type": "decimal(12, 2)", "required": False},
            ],
        },
        partition_spec={
            "fields": [{"source-id": 2, "field-id": 1000, "name": "order_day", "transform": "day"}]
        },
        sort_order={
            "fields": [
                {"source-id": 1, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}
            ]
        },
        properties={"write.format.default": "parquet"},
    )
    # [END howto_operator_polaris_create_table]

    # [START howto_operator_polaris_update_table_properties]
    update_table_properties = PolarisUpdateTablePropertiesOperator(
        task_id="update_table_properties",
        catalog=CATALOG,
        namespace="sales",
        table="orders",
        updates={"history.expire.max-snapshot-age-ms": "604800000"},
    )
    # [END howto_operator_polaris_update_table_properties]

    # [START howto_operator_polaris_rename_table]
    rename_table = PolarisRenameTableOperator(
        task_id="rename_table",
        catalog=CATALOG,
        namespace="sales",
        table="orders",
        new_table="orders_2026",
    )
    # [END howto_operator_polaris_rename_table]

    @task
    def get_metadata_location() -> str | None:
        return PolarisCatalogHook().load_table("sales", "orders_2026", catalog=CATALOG)["metadata-location"]

    metadata_location = get_metadata_location()

    # [START howto_operator_polaris_drop_table]
    drop_table = PolarisDropTableOperator(
        task_id="drop_table",
        catalog=CATALOG,
        namespace="sales",
        table="orders_2026",
    )
    # [END howto_operator_polaris_drop_table]

    # [START howto_operator_polaris_register_table]
    register_table = PolarisRegisterTableOperator(
        task_id="register_table",
        catalog=CATALOG,
        namespace="sales",
        table="orders",
        # The XCom of a task that read the table's metadata location before it was dropped.
        metadata_location=metadata_location,
    )
    # [END howto_operator_polaris_register_table]

    drop_registered_table = PolarisDropTableOperator(
        task_id="drop_registered_table",
        catalog=CATALOG,
        namespace="sales",
        table="orders",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    drop_renamed_table = PolarisDropTableOperator(
        task_id="drop_renamed_table",
        catalog=CATALOG,
        namespace="sales",
        table="orders_2026",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [START howto_operator_polaris_drop_namespace]
    drop_namespace = PolarisDropNamespaceOperator(
        task_id="drop_namespace",
        catalog=CATALOG,
        namespace="sales",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_polaris_drop_namespace]
    delete_catalog = PolarisDeleteCatalogOperator(
        task_id="delete_catalog", catalog_name=CATALOG, trigger_rule=TriggerRule.ALL_DONE
    )

    create_catalog >> create_namespace >> update_namespace_properties >> create_table
    create_table >> update_table_properties >> rename_table >> metadata_location
    metadata_location >> drop_table >> register_table
    register_table >> [drop_registered_table, drop_renamed_table]
    [drop_registered_table, drop_renamed_table] >> drop_namespace >> delete_catalog

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the Dag
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example Dag with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)

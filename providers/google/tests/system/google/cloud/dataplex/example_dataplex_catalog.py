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
Example Airflow DAG that shows how to use Dataplex Catalog.
"""

from __future__ import annotations

import datetime
import os

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataplex import (
    DataplexCatalogCreateAspectTypeOperator,
    DataplexCatalogCreateEntryGroupOperator,
    DataplexCatalogCreateEntryOperator,
    DataplexCatalogCreateEntryTypeOperator,
    DataplexCatalogDeleteAspectTypeOperator,
    DataplexCatalogDeleteEntryGroupOperator,
    DataplexCatalogDeleteEntryOperator,
    DataplexCatalogDeleteEntryTypeOperator,
    DataplexCatalogGetAspectTypeOperator,
    DataplexCatalogGetEntryGroupOperator,
    DataplexCatalogGetEntryOperator,
    DataplexCatalogGetEntryTypeOperator,
    DataplexCatalogListAspectTypesOperator,
    DataplexCatalogListEntriesOperator,
    DataplexCatalogListEntryGroupsOperator,
    DataplexCatalogListEntryTypesOperator,
    DataplexCatalogLookupEntryOperator,
    DataplexCatalogSearchEntriesOperator,
    DataplexCatalogUpdateAspectTypeOperator,
    DataplexCatalogUpdateEntryGroupOperator,
    DataplexCatalogUpdateEntryOperator,
    DataplexCatalogUpdateEntryTypeOperator,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "dataplex_catalog"
GCP_LOCATION = "us-central1"

ENTRY_GROUP_NAME = f"{DAG_ID}_entry_group_{ENV_ID}".replace("_", "-")
# [START howto_dataplex_entry_group_configuration]
ENTRY_GROUP_BODY = {"display_name": "Display Name", "description": "Some description"}
# [END howto_dataplex_entry_group_configuration]

ENTRY_TYPE_NAME = f"{DAG_ID}_entry_type_{ENV_ID}".replace("_", "-")
# [START howto_dataplex_entry_type_configuration]
ENTRY_TYPE_BODY = {"display_name": "Display Name", "description": "Some description"}
# [END howto_dataplex_entry_type_configuration]

ASPECT_TYPE_NAME = f"{DAG_ID}_aspect_type_{ENV_ID}".replace("_", "-")
# [START howto_dataplex_aspect_type_configuration]
ASPECT_TYPE_BODY = {
    "display_name": "Sample AspectType",
    "description": "A simple AspectType for demonstration purposes.",
    "metadata_template": {
        "name": "sample_field",
        "type": "record",
        "annotations": {
            "display_name": "Sample Field",
            "description": "A sample field within the AspectType.",
        },
    },
}
# [END howto_dataplex_aspect_type_configuration]

ENTRY_NAME = f"{DAG_ID}_entry_{ENV_ID}".replace("_", "-")
# [START howto_dataplex_entry_configuration]
ENTRY_BODY = {
    "name": f"projects/{PROJECT_ID}/locations/{GCP_LOCATION}/entryGroups/{ENTRY_GROUP_NAME}/entries/{ENTRY_NAME}",
    "entry_type": f"projects/{PROJECT_ID}/locations/{GCP_LOCATION}/entryTypes/{ENTRY_TYPE_NAME}",
}
# [END howto_dataplex_entry_configuration]

with DAG(
    DAG_ID,
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@once",
    tags=["example", "dataplex_catalog"],
) as dag:
    # [START howto_operator_dataplex_catalog_create_entry_group]
    create_entry_group = DataplexCatalogCreateEntryGroupOperator(
        task_id="create_entry_group",
        project_id=PROJECT_ID,
        location=GCP_LOCATION,
        entry_group_id=ENTRY_GROUP_NAME,
        entry_group_configuration=ENTRY_GROUP_BODY,
        validate_request=False,
    )
    # [END howto_operator_dataplex_catalog_create_entry_group]

    # [START howto_operator_dataplex_catalog_create_entry_type]
    create_entry_type = DataplexCatalogCreateEntryTypeOperator(
        task_id="create_entry_type",
        project_id=PROJECT_ID,
        location=GCP_LOCATION,
        entry_type_id=ENTRY_TYPE_NAME,
        entry_type_configuration=ENTRY_TYPE_BODY,
        validate_request=False,
    )
    # [END howto_operator_dataplex_catalog_create_entry_type]

    # [START howto_operator_dataplex_catalog_create_aspect_type]
    create_aspect_type = DataplexCatalogCreateAspectTypeOperator(
        task_id="create_aspect_type",
        project_id=PROJECT_ID,
        location=GCP_LOCATION,
        aspect_type_id=ASPECT_TYPE_NAME,
        aspect_type_configuration=ASPECT_TYPE_BODY,
        validate_request=False,
    )
    # [END howto_operator_dataplex_catalog_create_aspect_type]

    # [START howto_operator_dataplex_catalog_create_entry]
    create_entry = DataplexCatalogCreateEntryOperator(
        task_id="create_entry",
        project_id=PROJECT_ID,
        location=GCP_LOCATION,
        entry_id=ENTRY_NAME,
        entry_group_id=ENTRY_GROUP_NAME,
        entry_configuration=ENTRY_BODY,
    )
    # [END howto_operator_dataplex_catalog_create_entry]

    # [START howto_operator_dataplex_catalog_get_entry_group]
    get_entry_group = DataplexCatalogGetEntryGroupOperator(
        task_id="get_entry_group",
        project_id=PROJECT_ID,
        location=GCP_LOCATION,
        entry_group_id=ENTRY_GROUP_NAME,
    )
    # [END howto_operator_dataplex_catalog_get_entry_group]

    # [START howto_operator_dataplex_catalog_get_entry_type]
    get_entry_type = DataplexCatalogGetEntryTypeOperator(
        task_id="get_entry_type",
        project_id=PROJECT_ID,
        location=GCP_LOCATION,
        entry_type_id=ENTRY_TYPE_NAME,
    )
    # [END howto_operator_dataplex_catalog_get_entry_type]

    # [START howto_operator_dataplex_catalog_get_aspect_type]
    get_aspect_type = DataplexCatalogGetAspectTypeOperator(
        task_id="get_aspect_type",
        project_id=PROJECT_ID,
        location=GCP_LOCATION,
        aspect_type_id=ASPECT_TYPE_NAME,
    )
    # [END howto_operator_dataplex_catalog_get_aspect_type]

    # [START howto_operator_dataplex_catalog_get_entry]
    get_entry = DataplexCatalogGetEntryOperator(
        task_id="get_entry",
        project_id=PROJECT_ID,
        location=GCP_LOCATION,
        entry_id=ENTRY_NAME,
        entry_group_id=ENTRY_GROUP_NAME,
    )
    # [END howto_operator_dataplex_catalog_get_entry]

    # [START howto_operator_dataplex_catalog_list_entry_groups]
    list_entry_group = DataplexCatalogListEntryGroupsOperator(
        task_id="list_entry_group",
        project_id=PROJECT_ID,
        location=GCP_LOCATION,
        order_by="name",
        filter_by='display_name = "Display Name"',
    )
    # [END howto_operator_dataplex_catalog_list_entry_groups]

    # [START howto_operator_dataplex_catalog_list_entry_types]
    list_entry_type = DataplexCatalogListEntryTypesOperator(
        task_id="list_entry_type",
        project_id=PROJECT_ID,
        location=GCP_LOCATION,
        order_by="name",
        filter_by='display_name = "Display Name"',
    )
    # [END howto_operator_dataplex_catalog_list_entry_types]

    # [START howto_operator_dataplex_catalog_list_aspect_types]
    list_aspect_type = DataplexCatalogListAspectTypesOperator(
        task_id="list_aspect_type",
        project_id=PROJECT_ID,
        location=GCP_LOCATION,
        order_by="name",
        filter_by='display_name = "Display Name"',
    )
    # [END howto_operator_dataplex_catalog_list_aspect_types]

    # [START howto_operator_dataplex_catalog_list_entries]
    list_entry = DataplexCatalogListEntriesOperator(
        task_id="list_entry",
        project_id=PROJECT_ID,
        location=GCP_LOCATION,
        entry_group_id=ENTRY_GROUP_NAME,
    )
    # [END howto_operator_dataplex_catalog_list_entries]

    # [START howto_operator_dataplex_catalog_update_entry_group]
    update_entry_group = DataplexCatalogUpdateEntryGroupOperator(
        task_id="update_entry_group",
        project_id=PROJECT_ID,
        location=GCP_LOCATION,
        entry_group_id=ENTRY_GROUP_NAME,
        entry_group_configuration={"display_name": "Updated Display Name"},
        update_mask=["display_name"],
    )
    # [END howto_operator_dataplex_catalog_update_entry_group]

    # [START howto_operator_dataplex_catalog_update_entry_type]
    update_entry_type = DataplexCatalogUpdateEntryTypeOperator(
        task_id="update_entry_type",
        project_id=PROJECT_ID,
        location=GCP_LOCATION,
        entry_type_id=ENTRY_TYPE_NAME,
        entry_type_configuration={"display_name": "Updated Display Name"},
        update_mask=["display_name"],
    )
    # [END howto_operator_dataplex_catalog_update_entry_type]

    # [START howto_operator_dataplex_catalog_update_aspect_type]
    update_aspect_type = DataplexCatalogUpdateAspectTypeOperator(
        task_id="update_aspect_type",
        project_id=PROJECT_ID,
        location=GCP_LOCATION,
        aspect_type_id=ASPECT_TYPE_NAME,
        aspect_type_configuration={"display_name": "Updated Display Name"},
        update_mask=["display_name"],
    )
    # [END howto_operator_dataplex_catalog_update_aspect_type]

    # [START howto_operator_dataplex_catalog_update_entry]
    update_entry = DataplexCatalogUpdateEntryOperator(
        task_id="update_entry",
        project_id=PROJECT_ID,
        location=GCP_LOCATION,
        entry_id=ENTRY_NAME,
        entry_group_id=ENTRY_GROUP_NAME,
        entry_configuration={
            "fully_qualified_name": f"dataplex:{PROJECT_ID}.{GCP_LOCATION}.{ENTRY_GROUP_NAME}.some-entry"
        },
        update_mask=["fully_qualified_name"],
    )
    # [END howto_operator_dataplex_catalog_update_entry]

    # [START howto_operator_dataplex_catalog_search_entry]
    search_entry = DataplexCatalogSearchEntriesOperator(
        task_id="search_entry",
        project_id=PROJECT_ID,
        location=GCP_LOCATION,
        query=f"name={ENTRY_NAME}",
    )
    # [END howto_operator_dataplex_catalog_search_entry]

    # [START howto_operator_dataplex_catalog_lookup_entry]
    lookup_entry = DataplexCatalogLookupEntryOperator(
        task_id="lookup_entry",
        project_id=PROJECT_ID,
        location=GCP_LOCATION,
        entry_id=ENTRY_NAME,
        entry_group_id=ENTRY_GROUP_NAME,
    )
    # [END howto_operator_dataplex_catalog_lookup_entry]

    # [START howto_operator_dataplex_catalog_delete_entry_group]
    delete_entry_group = DataplexCatalogDeleteEntryGroupOperator(
        task_id="delete_entry_group",
        project_id=PROJECT_ID,
        location=GCP_LOCATION,
        entry_group_id=ENTRY_GROUP_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_dataplex_catalog_delete_entry_group]

    # [START howto_operator_dataplex_catalog_delete_entry_type]
    delete_entry_type = DataplexCatalogDeleteEntryTypeOperator(
        task_id="delete_entry_type",
        project_id=PROJECT_ID,
        location=GCP_LOCATION,
        entry_type_id=ENTRY_TYPE_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_dataplex_catalog_delete_entry_type]

    # [START howto_operator_dataplex_catalog_delete_aspect_type]
    delete_aspect_type = DataplexCatalogDeleteAspectTypeOperator(
        task_id="delete_aspect_type",
        project_id=PROJECT_ID,
        location=GCP_LOCATION,
        aspect_type_id=ASPECT_TYPE_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_dataplex_catalog_delete_aspect_type]

    # [START howto_operator_dataplex_catalog_delete_entry]
    delete_entry = DataplexCatalogDeleteEntryOperator(
        task_id="delete_entry",
        project_id=PROJECT_ID,
        location=GCP_LOCATION,
        entry_id=ENTRY_NAME,
        entry_group_id=ENTRY_GROUP_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_dataplex_catalog_delete_entry]

    (
        create_entry_group
        >> create_entry_type
        >> create_aspect_type
        >> create_entry
        >> search_entry
        >> [get_entry_group, get_entry_type, get_aspect_type, get_entry]
        >> lookup_entry
        >> [list_entry, list_entry_group, list_entry_type, list_aspect_type]
        >> update_entry_group
        >> update_entry_type
        >> update_entry
        >> update_aspect_type
        >> delete_entry
        >> delete_aspect_type
        >> delete_entry_type
        >> delete_entry_group
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)

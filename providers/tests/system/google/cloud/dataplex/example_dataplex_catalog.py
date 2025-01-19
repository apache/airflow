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
    DataplexCatalogCreateEntryGroupOperator,
    DataplexCatalogDeleteEntryGroupOperator,
    DataplexCatalogGetEntryGroupOperator,
    DataplexCatalogListEntryGroupsOperator,
    DataplexCatalogUpdateEntryGroupOperator,
)
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "dataplex_catalog"
GCP_LOCATION = "us-central1"

ENTRY_GROUP_NAME = f"{DAG_ID}_entry_group_{ENV_ID}".replace("_", "-")
# [START howto_dataplex_entry_group_configuration]
ENTRY_GROUP_BODY = {"display_name": "Display Name", "description": "Some description"}
# [END howto_dataplex_entry_group_configuration]

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

    # [START howto_operator_dataplex_catalog_get_entry_group]
    get_entry_group = DataplexCatalogGetEntryGroupOperator(
        task_id="get_entry_group",
        project_id=PROJECT_ID,
        location=GCP_LOCATION,
        entry_group_id=ENTRY_GROUP_NAME,
    )
    # [END howto_operator_dataplex_catalog_get_entry_group]

    # [START howto_operator_dataplex_catalog_list_entry_groups]
    list_entry_group = DataplexCatalogListEntryGroupsOperator(
        task_id="list_entry_group",
        project_id=PROJECT_ID,
        location=GCP_LOCATION,
        order_by="name",
        filter_by='display_name = "Display Name"',
    )
    # [END howto_operator_dataplex_catalog_list_entry_groups]

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

    # [START howto_operator_dataplex_catalog_delete_entry_group]
    delete_entry_group = DataplexCatalogDeleteEntryGroupOperator(
        task_id="delete_entry_group",
        project_id=PROJECT_ID,
        location=GCP_LOCATION,
        entry_group_id=ENTRY_GROUP_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_dataplex_catalog_delete_entry_group]

    create_entry_group >> get_entry_group >> list_entry_group >> update_entry_group >> delete_entry_group

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)

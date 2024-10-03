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

import os
from datetime import datetime

from google.protobuf.field_mask_pb2 import FieldMask

from airflow.models.dag import DAG
from airflow.models.xcom_arg import XComArg
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.datacatalog import (
    CloudDataCatalogCreateEntryGroupOperator,
    CloudDataCatalogCreateEntryOperator,
    CloudDataCatalogDeleteEntryGroupOperator,
    CloudDataCatalogDeleteEntryOperator,
    CloudDataCatalogGetEntryGroupOperator,
    CloudDataCatalogGetEntryOperator,
    CloudDataCatalogLookupEntryOperator,
    CloudDataCatalogUpdateEntryOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "datacatalog_entries"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
LOCATION = "us-central1"
ENTRY_GROUP_ID = f"id_{DAG_ID}_{ENV_ID}"
ENTRY_GROUP_NAME = f"name {DAG_ID} {ENV_ID}"
ENTRY_ID = "python_files"
ENTRY_NAME = "Wizard"

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    create_bucket = GCSCreateBucketOperator(task_id="create_bucket", bucket_name=BUCKET_NAME)

    # Create
    # [START howto_operator_gcp_datacatalog_create_entry_group]
    create_entry_group = CloudDataCatalogCreateEntryGroupOperator(
        task_id="create_entry_group",
        location=LOCATION,
        entry_group_id=ENTRY_GROUP_ID,
        entry_group={"display_name": ENTRY_GROUP_NAME},
    )
    # [END howto_operator_gcp_datacatalog_create_entry_group]

    # [START howto_operator_gcp_datacatalog_create_entry_group_result]
    create_entry_group_result = BashOperator(
        task_id="create_entry_group_result",
        bash_command=f"echo {XComArg(create_entry_group, key='entry_group_id')}",
    )
    # [END howto_operator_gcp_datacatalog_create_entry_group_result]

    # [START howto_operator_gcp_datacatalog_create_entry_gcs]
    create_entry_gcs = CloudDataCatalogCreateEntryOperator(
        task_id="create_entry_gcs",
        location=LOCATION,
        entry_group=ENTRY_GROUP_ID,
        entry_id=ENTRY_ID,
        entry={
            "display_name": ENTRY_NAME,
            "type_": "FILESET",
            "gcs_fileset_spec": {"file_patterns": [f"gs://{BUCKET_NAME}/**"]},
        },
    )
    # [END howto_operator_gcp_datacatalog_create_entry_gcs]

    # [START howto_operator_gcp_datacatalog_create_entry_gcs_result]
    create_entry_gcs_result = BashOperator(
        task_id="create_entry_gcs_result",
        bash_command=f"echo {XComArg(create_entry_gcs, key='entry_id')}",
    )
    # [END howto_operator_gcp_datacatalog_create_entry_gcs_result]

    # Get
    # [START howto_operator_gcp_datacatalog_get_entry_group]
    get_entry_group = CloudDataCatalogGetEntryGroupOperator(
        task_id="get_entry_group",
        location=LOCATION,
        entry_group=ENTRY_GROUP_ID,
        read_mask=FieldMask(paths=["name", "display_name"]),
    )
    # [END howto_operator_gcp_datacatalog_get_entry_group]

    # [START howto_operator_gcp_datacatalog_get_entry_group_result]
    get_entry_group_result = BashOperator(
        task_id="get_entry_group_result",
        bash_command=f"echo {get_entry_group.output}",
    )
    # [END howto_operator_gcp_datacatalog_get_entry_group_result]

    # [START howto_operator_gcp_datacatalog_get_entry]
    get_entry = CloudDataCatalogGetEntryOperator(
        task_id="get_entry", location=LOCATION, entry_group=ENTRY_GROUP_ID, entry=ENTRY_ID
    )
    # [END howto_operator_gcp_datacatalog_get_entry]

    # [START howto_operator_gcp_datacatalog_get_entry_result]
    get_entry_result = BashOperator(task_id="get_entry_result", bash_command=f"echo {get_entry.output}")
    # [END howto_operator_gcp_datacatalog_get_entry_result]

    # Lookup
    # [START howto_operator_gcp_datacatalog_lookup_entry_linked_resource]
    current_entry_template = (
        "//datacatalog.googleapis.com/projects/{project_id}/locations/{location}/"
        "entryGroups/{entry_group}/entries/{entry}"
    )
    lookup_entry_linked_resource = CloudDataCatalogLookupEntryOperator(
        task_id="lookup_entry",
        linked_resource=current_entry_template.format(
            project_id=PROJECT_ID, location=LOCATION, entry_group=ENTRY_GROUP_ID, entry=ENTRY_ID
        ),
    )
    # [END howto_operator_gcp_datacatalog_lookup_entry_linked_resource]

    # [START howto_operator_gcp_datacatalog_lookup_entry_result]
    lookup_entry_result = BashOperator(
        task_id="lookup_entry_result",
        bash_command="echo \"{{ task_instance.xcom_pull('lookup_entry')['display_name'] }}\"",
    )
    # [END howto_operator_gcp_datacatalog_lookup_entry_result]

    # Update
    # [START howto_operator_gcp_datacatalog_update_entry]
    update_entry = CloudDataCatalogUpdateEntryOperator(
        task_id="update_entry",
        entry={"display_name": f"{ENTRY_NAME} UPDATED"},
        update_mask={"paths": ["display_name"]},
        location=LOCATION,
        entry_group=ENTRY_GROUP_ID,
        entry_id=ENTRY_ID,
    )
    # [END howto_operator_gcp_datacatalog_update_entry]

    # Delete
    # [START howto_operator_gcp_datacatalog_delete_entry]
    delete_entry = CloudDataCatalogDeleteEntryOperator(
        task_id="delete_entry", location=LOCATION, entry_group=ENTRY_GROUP_ID, entry=ENTRY_ID
    )
    # [END howto_operator_gcp_datacatalog_delete_entry]
    delete_entry.trigger_rule = TriggerRule.ALL_DONE

    # [START howto_operator_gcp_datacatalog_delete_entry_group]
    delete_entry_group = CloudDataCatalogDeleteEntryGroupOperator(
        task_id="delete_entry_group", location=LOCATION, entry_group=ENTRY_GROUP_ID
    )
    # [END howto_operator_gcp_datacatalog_delete_entry_group]
    delete_entry_group.trigger_rule = TriggerRule.ALL_DONE

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        create_bucket
        # TEST BODY
        >> create_entry_group
        >> create_entry_group_result
        >> get_entry_group
        >> get_entry_group_result
        >> create_entry_gcs
        >> create_entry_gcs_result
        >> get_entry
        >> get_entry_result
        >> lookup_entry_linked_resource
        >> lookup_entry_result
        >> update_entry
        >> delete_entry
        >> delete_entry_group
        # TEST TEARDOWN
        >> delete_bucket
    )

    # ### Everything below this line is not part of example ###
    # ### Just for system tests purpose ###
    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)

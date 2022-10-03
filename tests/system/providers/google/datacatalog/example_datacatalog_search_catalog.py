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
from typing import cast

from google.cloud.datacatalog import TagField, TagTemplateField

from airflow import models
from airflow.models.xcom_arg import XComArg
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.datacatalog import (
    CloudDataCatalogCreateEntryGroupOperator,
    CloudDataCatalogCreateEntryOperator,
    CloudDataCatalogCreateTagOperator,
    CloudDataCatalogCreateTagTemplateOperator,
    CloudDataCatalogDeleteEntryGroupOperator,
    CloudDataCatalogDeleteEntryOperator,
    CloudDataCatalogDeleteTagOperator,
    CloudDataCatalogDeleteTagTemplateOperator,
    CloudDataCatalogSearchCatalogOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")

DAG_ID = "datacatalog_search_catalog"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
LOCATION = "us-central1"
ENTRY_GROUP_ID = f"id_{DAG_ID}_{ENV_ID}"
ENTRY_GROUP_NAME = f"name {DAG_ID} {ENV_ID}"
ENTRY_ID = "python_files"
ENTRY_NAME = "Wizard"
TEMPLATE_ID = "template_id"
TAG_TEMPLATE_DISPLAY_NAME = f"Data Catalog {DAG_ID} {ENV_ID}"
FIELD_NAME_1 = "first"

with models.DAG(
    DAG_ID,
    schedule='@once',
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

    # [START howto_operator_gcp_datacatalog_create_tag]
    create_tag = CloudDataCatalogCreateTagOperator(
        task_id="create_tag",
        location=LOCATION,
        entry_group=ENTRY_GROUP_ID,
        entry=ENTRY_ID,
        template_id=TEMPLATE_ID,
        tag={"fields": {FIELD_NAME_1: TagField(string_value="example-value-string")}},
    )
    # [END howto_operator_gcp_datacatalog_create_tag]

    tag_id = cast(str, XComArg(create_tag, key='tag_id'))

    # [START howto_operator_gcp_datacatalog_create_tag_result]
    create_tag_result = BashOperator(
        task_id="create_tag_result",
        bash_command=f"echo {tag_id}",
    )
    # [END howto_operator_gcp_datacatalog_create_tag_result]

    # [START howto_operator_gcp_datacatalog_create_tag_template]
    create_tag_template = CloudDataCatalogCreateTagTemplateOperator(
        task_id="create_tag_template",
        location=LOCATION,
        tag_template_id=TEMPLATE_ID,
        tag_template={
            "display_name": TAG_TEMPLATE_DISPLAY_NAME,
            "fields": {
                FIELD_NAME_1: TagTemplateField(
                    display_name="first-field", type_=dict(primitive_type="STRING")
                )
            },
        },
    )
    # [END howto_operator_gcp_datacatalog_create_tag_template]

    # [START howto_operator_gcp_datacatalog_create_tag_template_result]
    create_tag_template_result = BashOperator(
        task_id="create_tag_template_result",
        bash_command=f"echo {XComArg(create_tag_template, key='tag_template_id')}",
    )
    # [END howto_operator_gcp_datacatalog_create_tag_template_result]

    # Search
    # [START howto_operator_gcp_datacatalog_search_catalog]
    search_catalog = CloudDataCatalogSearchCatalogOperator(
        task_id="search_catalog", scope={"include_project_ids": [PROJECT_ID]}, query=f"projectid:{PROJECT_ID}"
    )
    # [END howto_operator_gcp_datacatalog_search_catalog]

    # [START howto_operator_gcp_datacatalog_search_catalog_result]
    search_catalog_result = BashOperator(
        task_id="search_catalog_result",
        bash_command=f"echo {search_catalog.output}",
    )
    # [END howto_operator_gcp_datacatalog_search_catalog_result]

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

    # [START howto_operator_gcp_datacatalog_delete_tag]
    delete_tag = CloudDataCatalogDeleteTagOperator(
        task_id="delete_tag",
        location=LOCATION,
        entry_group=ENTRY_GROUP_ID,
        entry=ENTRY_ID,
        tag=tag_id,
    )
    # [END howto_operator_gcp_datacatalog_delete_tag]
    delete_tag.trigger_rule = TriggerRule.ALL_DONE

    # [START howto_operator_gcp_datacatalog_delete_tag_template]
    delete_tag_template = CloudDataCatalogDeleteTagTemplateOperator(
        task_id="delete_tag_template", location=LOCATION, tag_template=TEMPLATE_ID, force=True
    )
    # [END howto_operator_gcp_datacatalog_delete_tag_template]
    delete_tag_template.trigger_rule = TriggerRule.ALL_DONE

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        create_bucket
        # TEST BODY
        >> create_entry_group
        >> create_entry_group_result
        >> create_entry_gcs
        >> create_entry_gcs_result
        >> create_tag_template
        >> create_tag_template_result
        >> create_tag
        >> create_tag_result
        >> search_catalog
        >> search_catalog_result
        >> delete_tag
        >> delete_tag_template
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

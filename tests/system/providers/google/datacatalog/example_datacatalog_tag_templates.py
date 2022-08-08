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

import os
from datetime import datetime

from google.cloud.datacatalog import FieldType, TagTemplateField

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.datacatalog import (
    CloudDataCatalogCreateTagTemplateFieldOperator,
    CloudDataCatalogCreateTagTemplateOperator,
    CloudDataCatalogDeleteTagTemplateFieldOperator,
    CloudDataCatalogDeleteTagTemplateOperator,
    CloudDataCatalogGetTagTemplateOperator,
    CloudDataCatalogRenameTagTemplateFieldOperator,
    CloudDataCatalogUpdateTagTemplateFieldOperator,
    CloudDataCatalogUpdateTagTemplateOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")

DAG_ID = "datacatalog_tag_templates"

LOCATION = "us-central1"
TEMPLATE_ID = "template_id"
TAG_TEMPLATE_DISPLAY_NAME = f"Data Catalog {DAG_ID} {ENV_ID}"
FIELD_NAME_1 = "first"
FIELD_NAME_2 = "second"
FIELD_NAME_3 = "first-rename"

with models.DAG(
    DAG_ID,
    schedule_interval='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    # Create
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
        bash_command=f"echo {create_tag_template.output['tag_template_id']}",
    )
    # [END howto_operator_gcp_datacatalog_create_tag_template_result]

    # [START howto_operator_gcp_datacatalog_create_tag_template_field]
    create_tag_template_field = CloudDataCatalogCreateTagTemplateFieldOperator(
        task_id="create_tag_template_field",
        location=LOCATION,
        tag_template=TEMPLATE_ID,
        tag_template_field_id=FIELD_NAME_2,
        tag_template_field=TagTemplateField(
            display_name="second-field", type_=FieldType(primitive_type="STRING")
        ),
    )
    # [END howto_operator_gcp_datacatalog_create_tag_template_field]

    # [START howto_operator_gcp_datacatalog_create_tag_template_field_result]
    create_tag_template_field_result = BashOperator(
        task_id="create_tag_template_field_result",
        bash_command=f"echo {create_tag_template_field.output['tag_template_field_id']}",
    )
    # [END howto_operator_gcp_datacatalog_create_tag_template_field_result]

    # Get
    # [START howto_operator_gcp_datacatalog_get_tag_template]
    get_tag_template = CloudDataCatalogGetTagTemplateOperator(
        task_id="get_tag_template", location=LOCATION, tag_template=TEMPLATE_ID
    )
    # [END howto_operator_gcp_datacatalog_get_tag_template]

    # [START howto_operator_gcp_datacatalog_get_tag_template_result]
    get_tag_template_result = BashOperator(
        task_id="get_tag_template_result",
        bash_command=f"echo {get_tag_template.output}",
    )
    # [END howto_operator_gcp_datacatalog_get_tag_template_result]

    # Rename
    # [START howto_operator_gcp_datacatalog_rename_tag_template_field]
    rename_tag_template_field = CloudDataCatalogRenameTagTemplateFieldOperator(
        task_id="rename_tag_template_field",
        location=LOCATION,
        tag_template=TEMPLATE_ID,
        field=FIELD_NAME_1,
        new_tag_template_field_id=FIELD_NAME_3,
    )
    # [END howto_operator_gcp_datacatalog_rename_tag_template_field]

    # Update
    # [START howto_operator_gcp_datacatalog_update_tag_template]
    update_tag_template = CloudDataCatalogUpdateTagTemplateOperator(
        task_id="update_tag_template",
        tag_template={"display_name": f"{TAG_TEMPLATE_DISPLAY_NAME} UPDATED"},
        update_mask={"paths": ["display_name"]},
        location=LOCATION,
        tag_template_id=TEMPLATE_ID,
    )
    # [END howto_operator_gcp_datacatalog_update_tag_template]

    # [START howto_operator_gcp_datacatalog_update_tag_template_field]
    update_tag_template_field = CloudDataCatalogUpdateTagTemplateFieldOperator(
        task_id="update_tag_template_field",
        tag_template_field={"display_name": "Updated template field"},
        update_mask={"paths": ["display_name"]},
        location=LOCATION,
        tag_template=TEMPLATE_ID,
        tag_template_field_id=FIELD_NAME_1,
    )
    # [END howto_operator_gcp_datacatalog_update_tag_template_field]

    # Delete
    # [START howto_operator_gcp_datacatalog_delete_tag_template_field]
    delete_tag_template_field = CloudDataCatalogDeleteTagTemplateFieldOperator(
        task_id="delete_tag_template_field",
        location=LOCATION,
        tag_template=TEMPLATE_ID,
        field=FIELD_NAME_2,
        force=True,
    )
    # [END howto_operator_gcp_datacatalog_delete_tag_template_field]
    delete_tag_template_field.trigger_rule = TriggerRule.ALL_DONE

    # [START howto_operator_gcp_datacatalog_delete_tag_template]
    delete_tag_template = CloudDataCatalogDeleteTagTemplateOperator(
        task_id="delete_tag_template", location=LOCATION, tag_template=TEMPLATE_ID, force=True
    )
    # [END howto_operator_gcp_datacatalog_delete_tag_template]
    delete_tag_template.trigger_rule = TriggerRule.ALL_DONE

    (
        # TEST BODY
        create_tag_template
        >> create_tag_template_result
        >> create_tag_template_field
        >> create_tag_template_field_result
        >> get_tag_template
        >> get_tag_template_result
        >> update_tag_template
        >> update_tag_template_field
        >> rename_tag_template_field
        >> delete_tag_template_field
        >> delete_tag_template
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

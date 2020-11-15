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

"""
Example Airflow DAG that interacts with Google Data Catalog service
"""
from google.cloud.datacatalog_v1beta1.proto.tags_pb2 import FieldType, TagField, TagTemplateField

from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.datacatalog import (
    CloudDataCatalogCreateEntryGroupOperator,
    CloudDataCatalogCreateEntryOperator,
    CloudDataCatalogCreateTagOperator,
    CloudDataCatalogCreateTagTemplateFieldOperator,
    CloudDataCatalogCreateTagTemplateOperator,
    CloudDataCatalogDeleteEntryGroupOperator,
    CloudDataCatalogDeleteEntryOperator,
    CloudDataCatalogDeleteTagOperator,
    CloudDataCatalogDeleteTagTemplateFieldOperator,
    CloudDataCatalogDeleteTagTemplateOperator,
    CloudDataCatalogGetEntryGroupOperator,
    CloudDataCatalogGetEntryOperator,
    CloudDataCatalogGetTagTemplateOperator,
    CloudDataCatalogListTagsOperator,
    CloudDataCatalogLookupEntryOperator,
    CloudDataCatalogRenameTagTemplateFieldOperator,
    CloudDataCatalogSearchCatalogOperator,
    CloudDataCatalogUpdateEntryOperator,
    CloudDataCatalogUpdateTagOperator,
    CloudDataCatalogUpdateTagTemplateFieldOperator,
    CloudDataCatalogUpdateTagTemplateOperator,
)
from airflow.utils.dates import days_ago
from airflow.utils.helpers import chain

PROJECT_ID = "polidea-airflow"
LOCATION = "us-central1"
ENTRY_GROUP_ID = "important_data_jan_2019"
ENTRY_ID = "python_files"
TEMPLATE_ID = "template_id"
FIELD_NAME_1 = "first"
FIELD_NAME_2 = "second"
FIELD_NAME_3 = "first-rename"

with models.DAG("example_gcp_datacatalog", start_date=days_ago(1), schedule_interval=None) as dag:
    # Create
    # [START howto_operator_gcp_datacatalog_create_entry_group]
    create_entry_group = CloudDataCatalogCreateEntryGroupOperator(
        task_id="create_entry_group",
        location=LOCATION,
        entry_group_id=ENTRY_GROUP_ID,
        entry_group={"display_name": "analytics data - jan 2011"},
    )
    # [END howto_operator_gcp_datacatalog_create_entry_group]

    # [START howto_operator_gcp_datacatalog_create_entry_group_result]
    create_entry_group_result = BashOperator(
        task_id="create_entry_group_result",
        bash_command="echo \"{{ task_instance.xcom_pull('create_entry_group', key='entry_group_id') }}\"",
    )
    # [END howto_operator_gcp_datacatalog_create_entry_group_result]

    # [START howto_operator_gcp_datacatalog_create_entry_group_result2]
    create_entry_group_result2 = BashOperator(
        task_id="create_entry_group_result2",
        bash_command="echo \"{{ task_instance.xcom_pull('create_entry_group') }}\"",
    )
    # [END howto_operator_gcp_datacatalog_create_entry_group_result2]

    # [START howto_operator_gcp_datacatalog_create_entry_gcs]
    create_entry_gcs = CloudDataCatalogCreateEntryOperator(
        task_id="create_entry_gcs",
        location=LOCATION,
        entry_group=ENTRY_GROUP_ID,
        entry_id=ENTRY_ID,
        entry={
            "display_name": "Wizard",
            "type": "FILESET",
            "gcs_fileset_spec": {"file_patterns": ["gs://test-datacatalog/**"]},
        },
    )
    # [END howto_operator_gcp_datacatalog_create_entry_gcs]

    # [START howto_operator_gcp_datacatalog_create_entry_gcs_result]
    create_entry_gcs_result = BashOperator(
        task_id="create_entry_gcs_result",
        bash_command="echo \"{{ task_instance.xcom_pull('create_entry_gcs', key='entry_id') }}\"",
    )
    # [END howto_operator_gcp_datacatalog_create_entry_gcs_result]

    # [START howto_operator_gcp_datacatalog_create_entry_gcs_result2]
    create_entry_gcs_result2 = BashOperator(
        task_id="create_entry_gcs_result2",
        bash_command="echo \"{{ task_instance.xcom_pull('create_entry_gcs') }}\"",
    )
    # [END howto_operator_gcp_datacatalog_create_entry_gcs_result2]

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

    # [START howto_operator_gcp_datacatalog_create_tag_result]
    create_tag_result = BashOperator(
        task_id="create_tag_result",
        bash_command="echo \"{{ task_instance.xcom_pull('create_tag', key='tag_id') }}\"",
    )
    # [END howto_operator_gcp_datacatalog_create_tag_result]

    # [START howto_operator_gcp_datacatalog_create_tag_result2]
    create_tag_result2 = BashOperator(
        task_id="create_tag_result2", bash_command="echo \"{{ task_instance.xcom_pull('create_tag') }}\""
    )
    # [END howto_operator_gcp_datacatalog_create_tag_result2]

    # [START howto_operator_gcp_datacatalog_create_tag_template]
    create_tag_template = CloudDataCatalogCreateTagTemplateOperator(
        task_id="create_tag_template",
        location=LOCATION,
        tag_template_id=TEMPLATE_ID,
        tag_template={
            "display_name": "Awesome Tag Template",
            "fields": {
                FIELD_NAME_1: TagTemplateField(
                    display_name="first-field", type=FieldType(primitive_type="STRING")
                )
            },
        },
    )
    # [END howto_operator_gcp_datacatalog_create_tag_template]

    # [START howto_operator_gcp_datacatalog_create_tag_template_result]
    create_tag_template_result = BashOperator(
        task_id="create_tag_template_result",
        bash_command="echo \"{{ task_instance.xcom_pull('create_tag_template', key='tag_template_id') }}\"",
    )
    # [END howto_operator_gcp_datacatalog_create_tag_template_result]

    # [START howto_operator_gcp_datacatalog_create_tag_template_result2]
    create_tag_template_result2 = BashOperator(
        task_id="create_tag_template_result2",
        bash_command="echo \"{{ task_instance.xcom_pull('create_tag_template') }}\"",
    )
    # [END howto_operator_gcp_datacatalog_create_tag_template_result2]

    # [START howto_operator_gcp_datacatalog_create_tag_template_field]
    create_tag_template_field = CloudDataCatalogCreateTagTemplateFieldOperator(
        task_id="create_tag_template_field",
        location=LOCATION,
        tag_template=TEMPLATE_ID,
        tag_template_field_id=FIELD_NAME_2,
        tag_template_field=TagTemplateField(
            display_name="second-field", type=FieldType(primitive_type="STRING")
        ),
    )
    # [END howto_operator_gcp_datacatalog_create_tag_template_field]

    # [START howto_operator_gcp_datacatalog_create_tag_template_field_result]
    create_tag_template_field_result = BashOperator(
        task_id="create_tag_template_field_result",
        bash_command=(
            "echo \"{{ task_instance.xcom_pull('create_tag_template_field',"
            + " key='tag_template_field_id') }}\""
        ),
    )
    # [END howto_operator_gcp_datacatalog_create_tag_template_field_result]

    # [START howto_operator_gcp_datacatalog_create_tag_template_field_result2]
    create_tag_template_field_result2 = BashOperator(
        task_id="create_tag_template_field_result2",
        bash_command="echo \"{{ task_instance.xcom_pull('create_tag_template_field') }}\"",
    )
    # [END howto_operator_gcp_datacatalog_create_tag_template_field_result2]

    # Delete
    # [START howto_operator_gcp_datacatalog_delete_entry]
    delete_entry = CloudDataCatalogDeleteEntryOperator(
        task_id="delete_entry", location=LOCATION, entry_group=ENTRY_GROUP_ID, entry=ENTRY_ID
    )
    # [END howto_operator_gcp_datacatalog_delete_entry]

    # [START howto_operator_gcp_datacatalog_delete_entry_group]
    delete_entry_group = CloudDataCatalogDeleteEntryGroupOperator(
        task_id="delete_entry_group", location=LOCATION, entry_group=ENTRY_GROUP_ID
    )
    # [END howto_operator_gcp_datacatalog_delete_entry_group]

    # [START howto_operator_gcp_datacatalog_delete_tag]
    delete_tag = CloudDataCatalogDeleteTagOperator(
        task_id="delete_tag",
        location=LOCATION,
        entry_group=ENTRY_GROUP_ID,
        entry=ENTRY_ID,
        tag="{{ task_instance.xcom_pull('create_tag', key='tag_id') }}",
    )
    # [END howto_operator_gcp_datacatalog_delete_tag]

    # [START howto_operator_gcp_datacatalog_delete_tag_template_field]
    delete_tag_template_field = CloudDataCatalogDeleteTagTemplateFieldOperator(
        task_id="delete_tag_template_field",
        location=LOCATION,
        tag_template=TEMPLATE_ID,
        field=FIELD_NAME_2,
        force=True,
    )
    # [END howto_operator_gcp_datacatalog_delete_tag_template_field]

    # [START howto_operator_gcp_datacatalog_delete_tag_template]
    delete_tag_template = CloudDataCatalogDeleteTagTemplateOperator(
        task_id="delete_tag_template", location=LOCATION, tag_template=TEMPLATE_ID, force=True
    )
    # [END howto_operator_gcp_datacatalog_delete_tag_template]

    # Get
    # [START howto_operator_gcp_datacatalog_get_entry_group]
    get_entry_group = CloudDataCatalogGetEntryGroupOperator(
        task_id="get_entry_group",
        location=LOCATION,
        entry_group=ENTRY_GROUP_ID,
        read_mask={"paths": ["name", "display_name"]},
    )
    # [END howto_operator_gcp_datacatalog_get_entry_group]

    # [START howto_operator_gcp_datacatalog_get_entry_group_result]
    get_entry_group_result = BashOperator(
        task_id="get_entry_group_result",
        bash_command="echo \"{{ task_instance.xcom_pull('get_entry_group') }}\"",
    )
    # [END howto_operator_gcp_datacatalog_get_entry_group_result]

    # [START howto_operator_gcp_datacatalog_get_entry]
    get_entry = CloudDataCatalogGetEntryOperator(
        task_id="get_entry", location=LOCATION, entry_group=ENTRY_GROUP_ID, entry=ENTRY_ID
    )
    # [END howto_operator_gcp_datacatalog_get_entry]

    # [START howto_operator_gcp_datacatalog_get_entry_result]
    get_entry_result = BashOperator(
        task_id="get_entry_result", bash_command="echo \"{{ task_instance.xcom_pull('get_entry') }}\""
    )
    # [END howto_operator_gcp_datacatalog_get_entry_result]

    # [START howto_operator_gcp_datacatalog_get_tag_template]
    get_tag_template = CloudDataCatalogGetTagTemplateOperator(
        task_id="get_tag_template", location=LOCATION, tag_template=TEMPLATE_ID
    )
    # [END howto_operator_gcp_datacatalog_get_tag_template]

    # [START howto_operator_gcp_datacatalog_get_tag_template_result]
    get_tag_template_result = BashOperator(
        task_id="get_tag_template_result",
        bash_command="echo \"{{ task_instance.xcom_pull('get_tag_template') }}\"",
    )
    # [END howto_operator_gcp_datacatalog_get_tag_template_result]

    # List
    # [START howto_operator_gcp_datacatalog_list_tags]
    list_tags = CloudDataCatalogListTagsOperator(
        task_id="list_tags", location=LOCATION, entry_group=ENTRY_GROUP_ID, entry=ENTRY_ID
    )
    # [END howto_operator_gcp_datacatalog_list_tags]

    # [START howto_operator_gcp_datacatalog_list_tags_result]
    list_tags_result = BashOperator(
        task_id="list_tags_result", bash_command="echo \"{{ task_instance.xcom_pull('list_tags') }}\""
    )
    # [END howto_operator_gcp_datacatalog_list_tags_result]

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
        bash_command="echo \"{{ task_instance.xcom_pull('lookup_entry')['displayName'] }}\"",
    )
    # [END howto_operator_gcp_datacatalog_lookup_entry_result]

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

    # Search
    # [START howto_operator_gcp_datacatalog_search_catalog]
    search_catalog = CloudDataCatalogSearchCatalogOperator(
        task_id="search_catalog", scope={"include_project_ids": [PROJECT_ID]}, query=f"projectid:{PROJECT_ID}"
    )
    # [END howto_operator_gcp_datacatalog_search_catalog]

    # [START howto_operator_gcp_datacatalog_search_catalog_result]
    search_catalog_result = BashOperator(
        task_id="search_catalog_result",
        bash_command="echo \"{{ task_instance.xcom_pull('search_catalog') }}\"",
    )
    # [END howto_operator_gcp_datacatalog_search_catalog_result]

    # Update
    # [START howto_operator_gcp_datacatalog_update_entry]
    update_entry = CloudDataCatalogUpdateEntryOperator(
        task_id="update_entry",
        entry={"display_name": "New Wizard"},
        update_mask={"paths": ["display_name"]},
        location=LOCATION,
        entry_group=ENTRY_GROUP_ID,
        entry_id=ENTRY_ID,
    )
    # [END howto_operator_gcp_datacatalog_update_entry]

    # [START howto_operator_gcp_datacatalog_update_tag]
    update_tag = CloudDataCatalogUpdateTagOperator(
        task_id="update_tag",
        tag={"fields": {FIELD_NAME_1: TagField(string_value="new-value-string")}},
        update_mask={"paths": ["fields"]},
        location=LOCATION,
        entry_group=ENTRY_GROUP_ID,
        entry=ENTRY_ID,
        tag_id="{{ task_instance.xcom_pull('create_tag', key='tag_id') }}",
    )
    # [END howto_operator_gcp_datacatalog_update_tag]

    # [START howto_operator_gcp_datacatalog_update_tag_template]
    update_tag_template = CloudDataCatalogUpdateTagTemplateOperator(
        task_id="update_tag_template",
        tag_template={"display_name": "Awesome Tag Template"},
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

    # Create
    create_tasks = [
        create_entry_group,
        create_entry_gcs,
        create_tag_template,
        create_tag_template_field,
        create_tag,
    ]
    chain(*create_tasks)

    create_entry_group >> delete_entry_group
    create_entry_group >> create_entry_group_result
    create_entry_group >> create_entry_group_result2

    create_entry_gcs >> delete_entry
    create_entry_gcs >> create_entry_gcs_result
    create_entry_gcs >> create_entry_gcs_result2

    create_tag_template >> delete_tag_template_field
    create_tag_template >> create_tag_template_result
    create_tag_template >> create_tag_template_result2

    create_tag_template_field >> delete_tag_template_field
    create_tag_template_field >> create_tag_template_field_result
    create_tag_template_field >> create_tag_template_field_result2

    create_tag >> delete_tag
    create_tag >> create_tag_result
    create_tag >> create_tag_result2

    # Delete
    delete_tasks = [
        delete_tag,
        delete_tag_template_field,
        delete_tag_template,
        delete_entry,
        delete_entry_group,
    ]
    chain(*delete_tasks)

    # Get
    create_tag_template >> get_tag_template >> delete_tag_template
    get_tag_template >> get_tag_template_result

    create_entry_gcs >> get_entry >> delete_entry
    get_entry >> get_entry_result

    create_entry_group >> get_entry_group >> delete_entry_group
    get_entry_group >> get_entry_group_result

    # List
    create_tag >> list_tags >> delete_tag
    list_tags >> list_tags_result

    # Lookup
    create_entry_gcs >> lookup_entry_linked_resource >> delete_entry
    lookup_entry_linked_resource >> lookup_entry_result

    # Rename
    create_tag_template_field >> rename_tag_template_field >> delete_tag_template_field

    # Search
    chain(create_tasks, search_catalog, delete_tasks)
    search_catalog >> search_catalog_result

    # Update
    create_entry_gcs >> update_entry >> delete_entry
    create_tag >> update_tag >> delete_tag
    create_tag_template >> update_tag_template >> delete_tag_template
    create_tag_template_field >> update_tag_template_field >> rename_tag_template_field

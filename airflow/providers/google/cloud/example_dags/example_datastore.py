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
Example Airflow DAG that shows how to use Datastore operators.

This example requires that your project contains Datastore instance.
"""

import os

from airflow import models
from airflow.providers.google.cloud.operators.datastore import (
    CloudDatastoreExportEntitiesOperator, CloudDatastoreImportEntitiesOperator,
)
from airflow.utils import dates

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")
BUCKET = os.environ.get("GCP_DATASTORE_BUCKET", "datastore-system-test")

with models.DAG(
    "example_gcp_datastore",
    schedule_interval=None,  # Override to match your needs
    start_date=dates.days_ago(1),
    tags=['example'],
) as dag:
    export_task = CloudDatastoreExportEntitiesOperator(
        task_id="export_task",
        bucket=BUCKET,
        project_id=GCP_PROJECT_ID,
        overwrite_existing=True,
    )

    import_task = CloudDatastoreImportEntitiesOperator(
        task_id="import_task",
        bucket="{{ task_instance.xcom_pull('export_task')['response']['outputUrl'].split('/')[2] }}",
        file="{{ '/'.join(task_instance.xcom_pull('export_task')['response']['outputUrl'].split('/')[3:]) }}",
        project_id=GCP_PROJECT_ID
    )

    export_task >> import_task

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

import pytest

from airflow import models
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator

try:
    from airflow.providers.google.cloud.transfers.mssql_to_gcs import MSSQLToGCSOperator
except ImportError:
    pytest.skip("MSSQL not available", allow_module_level=True)

from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")
DAG_ID = "example_mssql_to_gcs"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"

FILENAME = "test_file"

SQL_QUERY = "USE airflow SELECT * FROM Country;"

with models.DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "mssql"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )

    # [START howto_operator_mssql_to_gcs]
    upload_mssql_to_gcs = MSSQLToGCSOperator(
        task_id="mssql_to_gcs",
        mssql_conn_id="airflow_mssql",
        sql=SQL_QUERY,
        bucket=BUCKET_NAME,
        filename=FILENAME,
        export_format="csv",
    )
    # [END howto_operator_mssql_to_gcs]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        create_bucket
        # TEST BODY
        >> upload_mssql_to_gcs
        # TEST TEARDOWN
        >> delete_bucket
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)

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
Example Airflow DAG that shows how to use MongoToGCSOperator.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.mongo_to_gcs import MongoToGCSOperator

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "mongo_to_gcs"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
FILE_NAME = "mongo_export_{}.ndjson"
SCHEMA_FILE_NAME = "mongo_export_schema.json"

MONGO_CONN_ID = os.environ.get("SYSTEM_TESTS_MONGO_CONN_ID", "mongo_default")
MONGO_DATABASE = os.environ.get("SYSTEM_TESTS_MONGO_DATABASE", "test_db")
MONGO_COLLECTION = os.environ.get("SYSTEM_TESTS_MONGO_COLLECTION", "test_collection")


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "mongo", "gcs"],
) as dag:
    create_gcs_bucket = GCSCreateBucketOperator(
        task_id="create_gcs_bucket",
        bucket_name=BUCKET_NAME,
    )

    # [START howto_operator_mongo_to_gcs]
    mongo_to_gcs_find = MongoToGCSOperator(
        task_id="mongo_to_gcs_find",
        mongo_conn_id=MONGO_CONN_ID,
        mongo_db=MONGO_DATABASE,
        mongo_collection=MONGO_COLLECTION,
        mongo_query={"status": "active"},
        mongo_projection={"_id": 1, "name": 1, "status": 1},
        bucket=BUCKET_NAME,
        filename=FILE_NAME,
        schema_filename=SCHEMA_FILE_NAME,
        export_format="json",
    )
    # [END howto_operator_mongo_to_gcs]

    # [START howto_operator_mongo_to_gcs_aggregation]
    mongo_to_gcs_aggregate = MongoToGCSOperator(
        task_id="mongo_to_gcs_aggregate",
        mongo_conn_id=MONGO_CONN_ID,
        mongo_db=MONGO_DATABASE,
        mongo_collection=MONGO_COLLECTION,
        mongo_query=[
            {"$match": {"status": "active"}},
            {"$group": {"_id": "$category", "total": {"$sum": 1}}},
        ],
        bucket=BUCKET_NAME,
        filename="mongo_aggregate_{}.ndjson",
        export_format="json",
    )
    # [END howto_operator_mongo_to_gcs_aggregation]

    delete_gcs_bucket = GCSDeleteBucketOperator(
        task_id="delete_gcs_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_gcs_bucket >> [mongo_to_gcs_find, mongo_to_gcs_aggregate] >> delete_gcs_bucket

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)

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
Example Airflow DAG that shows how to use Dataplex Scan Data.
"""

from __future__ import annotations

import os
from datetime import datetime

from google.cloud import dataplex_v1
from google.cloud.dataplex_v1 import DataQualitySpec
from google.protobuf.field_mask_pb2 import FieldMask

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.dataplex import (
    DataplexCreateAssetOperator,
    DataplexCreateLakeOperator,
    DataplexCreateOrUpdateDataQualityScanOperator,
    DataplexCreateZoneOperator,
    DataplexDeleteAssetOperator,
    DataplexDeleteDataQualityScanOperator,
    DataplexDeleteLakeOperator,
    DataplexDeleteZoneOperator,
    DataplexGetDataQualityScanOperator,
    DataplexGetDataQualityScanResultOperator,
    DataplexRunDataQualityScanOperator,
)
from airflow.providers.google.cloud.sensors.dataplex import DataplexDataQualityJobStatusSensor

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "dataplex_data_quality"

LAKE_ID = f"lake-{DAG_ID}-{ENV_ID}".replace("_", "-")
REGION = "us-west1"

DATASET = f"dataset_bq_{DAG_ID}_{ENV_ID}"

TABLE_1 = "table0"
TABLE_2 = "table1"

SCHEMA = [
    {"name": "value", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "dt", "type": "STRING", "mode": "NULLABLE"},
]

INSERT_DATE = datetime.now().strftime("%Y-%m-%d")
INSERT_ROWS_QUERY = f"INSERT {DATASET}.{TABLE_1} VALUES (1, 'test test2', '{INSERT_DATE}');"
LOCATION = "us"

TRIGGER_SPEC_TYPE = "ON_DEMAND"

ZONE_ID = f"zone-id-{DAG_ID}-{ENV_ID}".replace("_", "-")
DATA_SCAN_ID = f"data-scan-id-{DAG_ID}-{ENV_ID}".replace("_", "-")

EXAMPLE_LAKE_BODY = {
    "display_name": "test_display_name",
    "labels": [],
    "description": "test_description",
    "metastore": {"service": ""},
}

# [START howto_dataplex_zone_configuration]
EXAMPLE_ZONE = {
    "type_": "RAW",
    "resource_spec": {"location_type": "SINGLE_REGION"},
}
# [END howto_dataplex_zone_configuration]

ASSET_ID = f"asset-id-{DAG_ID}-{ENV_ID}".replace("_", "-")

# [START howto_dataplex_asset_configuration]
EXAMPLE_ASSET = {
    "resource_spec": {"name": f"projects/{PROJECT_ID}/datasets/{DATASET}", "type_": "BIGQUERY_DATASET"},
    "discovery_spec": {"enabled": True},
}
# [END howto_dataplex_asset_configuration]

# [START howto_dataplex_data_quality_configuration]
EXAMPLE_DATA_SCAN = dataplex_v1.DataScan()
EXAMPLE_DATA_SCAN.data.entity = (
    f"projects/{PROJECT_ID}/locations/{REGION}/lakes/{LAKE_ID}/zones/{ZONE_ID}/entities/{TABLE_1}"
)
EXAMPLE_DATA_SCAN.data.resource = (
    f"//bigquery.googleapis.com/projects/{PROJECT_ID}/datasets/{DATASET}/tables/{TABLE_1}"
)
EXAMPLE_DATA_SCAN.data_quality_spec = DataQualitySpec(
    {
        "rules": [
            {
                "range_expectation": {
                    "min_value": "0",
                    "max_value": "10000",
                },
                "column": "value",
                "dimension": "VALIDITY",
            }
        ],
    }
)
# [END howto_dataplex_data_quality_configuration]
UPDATE_MASK = FieldMask(paths=["data_quality_spec"])
ENTITY = f"projects/{PROJECT_ID}/locations/{REGION}/lakes/{LAKE_ID}/zones/{ZONE_ID}/entities/{TABLE_1}"
EXAMPLE_DATA_SCAN_UPDATE = {
    "data": {
        "entity": ENTITY,
        "resource": f"//bigquery.googleapis.com/projects/{PROJECT_ID}/datasets/{DATASET}/tables/{TABLE_1}",
    },
    "data_quality_spec": {
        "rules": [
            {
                "range_expectation": {
                    "min_value": "1",
                    "max_value": "50000",
                },
                "column": "value",
                "dimension": "VALIDITY",
            }
        ],
    },
}


with DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule="@once",
    tags=["example", "dataplex", "data_quality"],
) as dag:
    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create_dataset", dataset_id=DATASET)
    create_table_1 = BigQueryCreateTableOperator(
        task_id="create_table_1",
        dataset_id=DATASET,
        table_id=TABLE_1,
        table_resource={
            "schema": {"fields": SCHEMA},
        },
        location=LOCATION,
    )
    create_table_2 = BigQueryCreateTableOperator(
        task_id="create_table_2",
        dataset_id=DATASET,
        table_id=TABLE_2,
        table_resource={
            "schema": {"fields": SCHEMA},
        },
        location=LOCATION,
    )
    insert_query_job = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration={
            "query": {
                "query": INSERT_ROWS_QUERY,
                "useLegacySql": False,
            }
        },
    )
    create_lake = DataplexCreateLakeOperator(
        task_id="create_lake", project_id=PROJECT_ID, region=REGION, body=EXAMPLE_LAKE_BODY, lake_id=LAKE_ID
    )
    # [START howto_dataplex_create_zone_operator]
    create_zone = DataplexCreateZoneOperator(
        task_id="create_zone",
        project_id=PROJECT_ID,
        region=REGION,
        lake_id=LAKE_ID,
        body=EXAMPLE_ZONE,
        zone_id=ZONE_ID,
    )
    # [END howto_dataplex_create_zone_operator]
    # [START howto_dataplex_create_asset_operator]
    create_asset = DataplexCreateAssetOperator(
        task_id="create_asset",
        project_id=PROJECT_ID,
        region=REGION,
        body=EXAMPLE_ASSET,
        lake_id=LAKE_ID,
        zone_id=ZONE_ID,
        asset_id=ASSET_ID,
    )
    # [END howto_dataplex_create_asset_operator]
    # [START howto_dataplex_create_data_quality_operator]
    create_data_scan = DataplexCreateOrUpdateDataQualityScanOperator(
        task_id="create_data_scan",
        project_id=PROJECT_ID,
        region=REGION,
        body=EXAMPLE_DATA_SCAN,
        data_scan_id=DATA_SCAN_ID,
    )
    # [END howto_dataplex_create_data_quality_operator]
    update_data_scan = DataplexCreateOrUpdateDataQualityScanOperator(
        task_id="update_data_scan",
        project_id=PROJECT_ID,
        region=REGION,
        update_mask=UPDATE_MASK,
        body=EXAMPLE_DATA_SCAN_UPDATE,
        data_scan_id=DATA_SCAN_ID,
    )
    # [START howto_dataplex_get_data_quality_operator]
    get_data_scan = DataplexGetDataQualityScanOperator(
        task_id="get_data_scan",
        project_id=PROJECT_ID,
        region=REGION,
        data_scan_id=DATA_SCAN_ID,
    )
    # [END howto_dataplex_get_data_quality_operator]
    run_data_scan_sync = DataplexRunDataQualityScanOperator(
        task_id="run_data_scan_sync",
        project_id=PROJECT_ID,
        region=REGION,
        data_scan_id=DATA_SCAN_ID,
    )
    get_data_scan_job_result = DataplexGetDataQualityScanResultOperator(
        task_id="get_data_scan_job_result",
        project_id=PROJECT_ID,
        region=REGION,
        data_scan_id=DATA_SCAN_ID,
    )
    # [START howto_dataplex_run_data_quality_operator]
    run_data_scan_async = DataplexRunDataQualityScanOperator(
        task_id="run_data_scan_async",
        project_id=PROJECT_ID,
        region=REGION,
        data_scan_id=DATA_SCAN_ID,
        asynchronous=True,
    )
    # [END howto_dataplex_run_data_quality_operator]
    # [START howto_dataplex_data_scan_job_state_sensor]
    get_data_scan_job_status = DataplexDataQualityJobStatusSensor(
        task_id="get_data_scan_job_status",
        project_id=PROJECT_ID,
        region=REGION,
        data_scan_id=DATA_SCAN_ID,
        job_id="{{ task_instance.xcom_pull('run_data_scan_async') }}",
    )
    # [END howto_dataplex_data_scan_job_state_sensor]
    # [START howto_dataplex_get_data_quality_job_operator]
    get_data_scan_job_result_2 = DataplexGetDataQualityScanResultOperator(
        task_id="get_data_scan_job_result_2",
        project_id=PROJECT_ID,
        region=REGION,
        data_scan_id=DATA_SCAN_ID,
    )
    # [END howto_dataplex_get_data_quality_job_operator]
    # [START howto_dataplex_run_data_quality_def_operator]
    run_data_scan_def = DataplexRunDataQualityScanOperator(
        task_id="run_data_scan_def",
        project_id=PROJECT_ID,
        region=REGION,
        data_scan_id=DATA_SCAN_ID,
        deferrable=True,
    )
    # [END howto_dataplex_run_data_quality_def_operator]
    run_data_scan_async_2 = DataplexRunDataQualityScanOperator(
        task_id="run_data_scan_async_2",
        project_id=PROJECT_ID,
        region=REGION,
        data_scan_id=DATA_SCAN_ID,
        asynchronous=True,
    )
    # [START howto_dataplex_get_data_quality_job_def_operator]
    get_data_scan_job_result_def = DataplexGetDataQualityScanResultOperator(
        task_id="get_data_scan_job_result_def",
        project_id=PROJECT_ID,
        region=REGION,
        data_scan_id=DATA_SCAN_ID,
        deferrable=True,
    )
    # [END howto_dataplex_get_data_quality_job_def_operator]
    # [START howto_dataplex_delete_asset_operator]
    delete_asset = DataplexDeleteAssetOperator(
        task_id="delete_asset",
        project_id=PROJECT_ID,
        region=REGION,
        lake_id=LAKE_ID,
        zone_id=ZONE_ID,
        asset_id=ASSET_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_dataplex_delete_asset_operator]
    # [START howto_dataplex_delete_zone_operator]
    delete_zone = DataplexDeleteZoneOperator(
        task_id="delete_zone",
        project_id=PROJECT_ID,
        region=REGION,
        lake_id=LAKE_ID,
        zone_id=ZONE_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_dataplex_delete_zone_operator]
    # [START howto_dataplex_delete_data_quality_operator]
    delete_data_scan = DataplexDeleteDataQualityScanOperator(
        task_id="delete_data_scan",
        project_id=PROJECT_ID,
        region=REGION,
        data_scan_id=DATA_SCAN_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_dataplex_delete_data_quality_operator]
    delete_lake = DataplexDeleteLakeOperator(
        project_id=PROJECT_ID,
        region=REGION,
        lake_id=LAKE_ID,
        task_id="delete_lake",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=DATASET,
        project_id=PROJECT_ID,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        # TEST SETUP
        create_dataset,
        [create_table_1, create_table_2],
        insert_query_job,
        create_lake,
        create_zone,
        create_asset,
        # TEST BODY
        create_data_scan,
        update_data_scan,
        get_data_scan,
        run_data_scan_sync,
        get_data_scan_job_result,
        run_data_scan_async,
        get_data_scan_job_status,
        get_data_scan_job_result_2,
        run_data_scan_def,
        run_data_scan_async_2,
        get_data_scan_job_result_def,
        # TEST TEARDOWN
        delete_asset,
        delete_zone,
        delete_data_scan,
        [delete_lake, delete_dataset],
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)

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
Example Airflow DAG that senses blob(s) in Azure Blob Storage.

This DAG relies on the following OS environment variables

* CONTAINER_NAME - The container under which to look for the blob.
* BLOB_NAME - The name of the blob to match.
* PREFIX - The blob with the specified prefix to match.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models import DAG
from airflow.providers.microsoft.azure.sensors.wasb import (
    WasbBlobSensor,
    WasbPrefixSensor,
)

CONTAINER_NAME = os.environ.get("CONTAINER_NAME", "example-container-name")
BLOB_NAME = os.environ.get("BLOB_NAME", "example-blob-name")
PREFIX = os.environ.get("PREFIX", "example-prefix")


with DAG(
    "example_wasb_sensors",
    start_date=datetime(2022, 8, 8),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    # [START wasb_blob_sensor]
    azure_wasb_sensor = WasbBlobSensor(
        container_name=CONTAINER_NAME,
        blob_name=BLOB_NAME,
        task_id="wasb_sense_blob",
    )
    # [END wasb_blob_sensor]

    # [START wasb_prefix_sensor]
    azure_wasb_prefix_sensor = WasbPrefixSensor(
        container_name=CONTAINER_NAME,
        prefix=PREFIX,
        task_id="wasb_sense_prefix",
    )
    # [END wasb_prefix_sensor]


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)

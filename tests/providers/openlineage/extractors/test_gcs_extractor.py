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

import logging

from openlineage.client.run import Dataset

from airflow.models import DAG
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.openlineage.extractors.base import OperatorLineage
from airflow.providers.openlineage.extractors.gcs import GCSToGCSExtractor
from airflow.utils import timezone

log = logging.getLogger(__name__)


def _get_copy_task():
    dag = DAG(dag_id="TestGCSToGCSExtractor")
    task = GCSToGCSOperator(
        task_id="task_id",
        source_bucket="source-bucket",
        source_object="path/to/source_file.csv",
        destination_bucket="destination-bucket",
        destination_object="path/to/destination_file.csv",
        dag=dag,
        start_date=timezone.datetime(2016, 2, 1, 0, 0, 0),
    )
    return task


def test_extract():
    task = _get_copy_task()
    extractor = GCSToGCSExtractor(operator=task)

    expected_return_value = OperatorLineage(
        inputs=[
            Dataset(
                namespace="gs://source-bucket",
                name="gs://source-bucket/path/to/source_file.csv",
                facets={},
            )
        ],
        outputs=[
            Dataset(
                namespace="gs://destination-bucket",
                name="gs://destination-bucket/path/to/destination_file.csv",
                facets={},
            )
        ],
    )
    return_value = extractor.extract()
    assert return_value == expected_return_value

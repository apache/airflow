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
"""Read a pinned public dataset, map it, and validate the durable XCom handoff."""
from airflow.sdk import DAG, task

from airflow.providers.huggingface.operators.datasets import LoadDatasetOperator, MapDatasetOperator

SOURCE_REPO = "nyu-mll/glue"
SOURCE_REVISION = "bcdcba79d07bc864c1c254ccfcedcce55bcc9a8c"


def add_pair_text(example):
    return {"pair_text": f"{example['sentence1']} [SEP] {example['sentence2']}"}


with DAG(dag_id="huggingface_system_read_only", tags=["huggingface", "system-test"]) as dag:
    load = LoadDatasetOperator(
        task_id="load_public_dataset",
        path=SOURCE_REPO,
        name="mrpc",
        split="train[:8]",
        revision=SOURCE_REVISION,
        output_uri="file:///tmp/hf-provider-system/read-only-source.json",
        huggingface_conn_id=None,
    )
    mapped = MapDatasetOperator(
        task_id="map_public_dataset",
        input_uri=load.output,
        output_uri="file:///tmp/hf-provider-system/read-only-mapped.json",
        transform=add_pair_text,
    )

    @task
    def verify_xcom_and_artifact(load_ref, mapped_ref):
        assert load_ref["revision"] == SOURCE_REVISION
        dataset_ref = mapped_ref["splits"]["dataset"]
        assert dataset_ref["rows"] == 8
        assert "pair_text" in dataset_ref["features"]
        return {"revision": load_ref["revision"], "rows": dataset_ref["rows"]}

    verify_xcom_and_artifact(load.output, mapped.output)

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run

test_run = get_test_run(dag)

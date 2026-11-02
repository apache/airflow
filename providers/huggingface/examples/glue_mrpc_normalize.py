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
examples/glue_mrpc_normalize.py
--------------------------------
Load the GLUE MRPC training split, combine sentence pairs into a single
``text`` field, and publish the result to the Hub.

Use-case
~~~~~~~~
MRPC (Microsoft Research Paraphrase Corpus) ships two sentence columns and
a binary similarity label.  Fine-tuning encoders on it typically needs a
single fused text field.  This DAG shows the canonical load → map →
publish path using durable Parquet artifacts for
a dataset small enough to fit in worker memory.

Prerequisites
~~~~~~~~~~~~~
* Install the provider::

    pip install "apache-airflow-providers-huggingface @ git+https://github.com/ParagEkbote/hf-datasets-airflow-provider"

* Create an Airflow connection ``huggingface_default`` with type
  ``huggingface`` and a write-access token in the password field.

* Ensure the example artifact directory is visible to all task workers. The
  local tutorial paths use ``/tmp/hf-datasets/<dag_id>/<run_id>``; change the
  base path for a shared mount in a multi-worker deployment.


Airflow 3 Asset scheduling
~~~~~~~~~~~~~~~~~~~~~~~~~~
``PushToHubOperator`` emits an asset for its Hub repository after successful
publication. A downstream DAG can schedule on ``Asset("hf://datasets/<repo>")``.
"""

import os

from airflow.sdk import DAG
from airflow.providers.huggingface.operators.datasets import (
    LoadDatasetOperator,
    MapDatasetOperator,
    PushToHubOperator,
)


def normalize_glue_pair(batch):
    """Fuse MRPC sentence pairs while retaining their labels."""
    return {
        "text": [f"{' '.join(a.split()).lower()} [SEP] {' '.join(b.split()).lower()}"
                 for a, b in zip(batch["sentence1"], batch["sentence2"])],
        "label": batch["label"],
    }


MRPC_REPO_ID = os.environ.get("HF_MRPC_REPO_ID", "my-org/glue-mrpc-normalized")

with DAG(
    dag_id="glue_mrpc_normalize",
    tags=["huggingface", "glue", "example"],
) as dag:

    load = LoadDatasetOperator(
        task_id="load_mrpc",
        path="nyu-mll/glue",
        name="mrpc",          # dataset configuration
        split="train",
        revision="bcdcba79d07bc864c1c254ccfcedcce55bcc9a8c",
        output_uri="file:///tmp/hf-datasets/{{ dag_id }}/{{ run_id }}/mrpc_raw.json",
        # cache_dir keeps downloaded Arrow files across retries.
        cache_dir="/tmp/hf-datasets/cache",
    )

    transform = MapDatasetOperator(
        task_id="normalize_pairs",
        # Pass the whole XCom artifact reference so provenance is forwarded.
        input_uri=load.output,
        output_uri="file:///tmp/hf-datasets/{{ dag_id }}/{{ run_id }}/mrpc_normalized.json",
        transform=normalize_glue_pair,
        map_kwargs={
            # Combine both sentences in a single pass and drop the raw columns.
            "batched": True,
            "batch_size": 512,
            "remove_columns": ["sentence1", "sentence2", "idx"],
        },
    )

    publish = PushToHubOperator(
        task_id="publish",
        input_uri=transform.output,
        repo_id=MRPC_REPO_ID,
        private=True,
        commit_message="Airflow: normalized MRPC training split",
    )

    load >> transform >> publish



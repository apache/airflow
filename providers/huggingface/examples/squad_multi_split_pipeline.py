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
"""Load SQuAD's split dataset, map each split, and publish the DatasetDict."""

import os

from airflow.sdk import DAG
from airflow.providers.huggingface.operators.datasets import (
    LoadDatasetOperator,
    MapDatasetOperator,
    PushToHubOperator,
)


def flatten_squad_example(example):
    """Create a compact question/context text and retain the first answer."""
    answers = example["answers"]["text"]
    return {
        "text": f"{example['question']} {example['context']}",
        "answer": answers[0] if answers else "",
    }


with DAG(dag_id="huggingface_squad_multi_split_pipeline", tags=["huggingface", "squad", "example"]) as dag:
    load = LoadDatasetOperator(
        task_id="load_splits",
        path="rajpurkar/squad",
        revision="7b6d24c440a36b6815f21b70d25016731768db1f",
        output_uri="file:///tmp/hf-datasets/{{ dag_id }}/{{ run_id }}/squad/source.json",
    )
    transform = MapDatasetOperator(
        task_id="flatten_splits",
        input_uri=load.output,
        output_uri="file:///tmp/hf-datasets/{{ dag_id }}/{{ run_id }}/squad/flattened.json",
        transform=flatten_squad_example,
        map_kwargs={"remove_columns": ["id", "title", "context", "question", "answers"]},
    )
    publish = PushToHubOperator(
        task_id="publish",
        input_uri=transform.output,
        repo_id=os.environ.get("HF_SQUAD_REPO_ID", "my-org/squad-flattened"),
        private=True,
        commit_message="Airflow: flatten SQuAD splits",
    )
    load >> transform >> publish

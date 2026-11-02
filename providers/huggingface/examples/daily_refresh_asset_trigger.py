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
"""Publish a refreshed dataset and schedule a consumer from its asset."""
from airflow.sdk import Asset, DAG
import os
from airflow.providers.huggingface.operators.datasets import LoadDatasetOperator, MapDatasetOperator, PushToHubOperator

SOURCE_REPO = "tatsu-lab/alpaca"
PRODUCER_REPO = os.environ.get("HF_PRODUCER_REPO_ID", "my-org/alpaca-long-output")
CONSUMER_REPO = os.environ.get("HF_CONSUMER_REPO_ID", "my-org/alpaca-long-output-consumed")
PUBLISHED = Asset(f"hf://datasets/{PRODUCER_REPO}")

def add_char_count(batch):
    """Add a count from instruction, input, and output text."""
    inputs = batch.get("input", [""] * len(batch["instruction"]))
    batch["char_count"] = [len(a) + len(b) + len(c) for a, b, c in zip(batch["instruction"], inputs, batch["output"])]
    return batch


with DAG("huggingface_daily_refresh_producer", schedule="@daily", tags=["huggingface", "assets", "example"]) as producer:
    load = LoadDatasetOperator(task_id="load_source", path=SOURCE_REPO, split="train",
        revision="dce01c9b08f87459cf36a430d809084718273017",
        output_uri="file:///tmp/hf-datasets/{{ dag_id }}/{{ run_id }}/alpaca/source.json")
    enrich = MapDatasetOperator(task_id="add_character_count", input_uri=load.output,
        output_uri="file:///tmp/hf-datasets/{{ dag_id }}/{{ run_id }}/alpaca/enriched.json", transform=add_char_count,
        map_kwargs={"batched": True})
    publish = PushToHubOperator(task_id="publish", input_uri=enrich.output,
        repo_id=PRODUCER_REPO, private=True)
    load >> enrich >> publish

with DAG("huggingface_daily_refresh_consumer", schedule=[PUBLISHED], tags=["huggingface", "assets", "example"]) as consumer:
    load = LoadDatasetOperator(task_id="load_published_dataset", path=PRODUCER_REPO, split="train",
        # Pin the load to the commit carried by the event that triggered this run.
        revision="{{ (triggering_asset_events.values() | first | first).extra['commit_sha'] }}",
        output_uri="file:///tmp/hf-datasets/{{ dag_id }}/{{ run_id }}/alpaca/consumer_input.json")
    remap = MapDatasetOperator(task_id="refresh_character_count", input_uri=load.output,
        output_uri="file:///tmp/hf-datasets/{{ dag_id }}/{{ run_id }}/alpaca/consumer_output.json", transform=add_char_count,
        map_kwargs={"batched": True})
    publish = PushToHubOperator(task_id="publish_consumer_output", input_uri=remap.output,
        repo_id=CONSUMER_REPO, private=True)
    load >> remap >> publish

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
"""Transform and publish a streamed Hugging Face dataset directly to the Hub."""
import os

from airflow.sdk import DAG
from airflow.providers.huggingface.operators.datasets import StreamDatasetOperator


def normalize_review(example):
    return {"text": " ".join(example["text"].split()), "label": example["label"]}


def keep_nonempty_review(example):
    return bool(example["text"])


with DAG("huggingface_rotten_tomatoes_stream", tags=["huggingface", "streaming", "example"]) as dag:
    publish = StreamDatasetOperator(
        task_id="stream_transform_publish",
        path="cornell-movie-review-data/rotten_tomatoes",
        split="train",
        repo_id=os.environ.get("HF_STREAM_REPO_ID", "my-org/rotten-tomatoes-streamed"),
        private=True,
        transform=normalize_review,
        predicate=keep_nonempty_review,
    )

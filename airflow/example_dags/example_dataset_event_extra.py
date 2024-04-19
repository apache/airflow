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
Example DAG to demonstrate annotating a dataset event with extra information.
"""

from __future__ import annotations

import datetime

from airflow.datasets import Dataset
from airflow.datasets.metadata import Metadata
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

ds = Dataset("s3://output/1.txt")

with DAG(
    dag_id="dataset_with_extra_by_yield",
    catchup=False,
    start_date=datetime.datetime.min,
    schedule="@daily",
    tags=["produces"],
):

    @task(outlets=[ds])
    def dataset_with_extra_by_yield():
        yield Metadata(ds, {"hi": "bye"})

    dataset_with_extra_by_yield()

with DAG(
    dag_id="dataset_with_extra_by_context",
    catchup=False,
    start_date=datetime.datetime.min,
    schedule="@daily",
    tags=["produces"],
):

    @task(outlets=[ds])
    def dataset_with_extra_by_context(*, dataset_events=None):
        dataset_events[ds].extra = {"hi": "bye"}

    dataset_with_extra_by_context()

with DAG(
    dag_id="dataset_with_extra_from_classic_operator",
    catchup=False,
    start_date=datetime.datetime.min,
    schedule="@daily",
    tags=["produces"],
):

    def _dataset_with_extra_from_classic_operator_post_execute(context):
        context["dataset_events"].extra = {"hi": "bye"}

    BashOperator(
        task_id="dataset_with_extra_from_classic_operator",
        outlets=[ds],
        bash_command=":",
        post_execute=_dataset_with_extra_from_classic_operator_post_execute,
    )

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
import datetime
import logging
from pathlib import Path
from typing import List

import grpc
from kubernetes.client import models as k8s
from rich.console import Console

from airflow.callbacks.callback_requests import (
    CallbackRequest,
    DagCallbackRequest,
    SlaCallbackRequest,
    TaskCallbackRequest,
)
from airflow.dag_processing.processor import DagFileProcessor
from airflow.models.taskinstance import SimpleTaskInstance, TaskInstanceKey
from airflow.utils import cli as cli_utils

console = Console(width=400, color_system="standard")


def process_example_files(num_callbacks: int, processor: DagFileProcessor):
    example_dags_folder = Path(__file__).parents[3] / "airflow" / "example_dags"
    callback_base = [
        TaskCallbackRequest(
            full_filepath=str(example_dags_folder / "example_bash_operator.py"),
            simple_task_instance=SimpleTaskInstance(
                task_id="run_this_last",
                dag_id="example_python_operator",
                run_id="run_id",
                start_date=datetime.datetime.now(),
                end_date=datetime.datetime.now(),
                try_number=1,
                map_index=1,
                state="RUNNING",
                executor_config={
                    "test": "test",
                    "pod_override": k8s.V1Pod(metadata=k8s.V1ObjectMeta(annotations={"test": "annotation"})),
                },
                pool="pool",
                queue="queue",
                key=TaskInstanceKey(dag_id="dag", task_id="task_id", run_id="run_id"),
                run_as_user="user",
            ),
        ),
        DagCallbackRequest(
            full_filepath="file",
            dag_id="example_bash_operator",
            run_id="run_this_last",
            is_failure_callback=False,
            msg="Error Message",
        ),
        SlaCallbackRequest(full_filepath="file", dag_id="example_bash_operator", msg="Error message"),
    ]
    callbacks: List[CallbackRequest] = []
    for i in range(num_callbacks):
        callbacks.extend(callback_base)
    sum_dags = 0
    sum_errors = 0
    for file in example_dags_folder.iterdir():
        if file.is_file() and file.name.endswith(".py"):
            dags, errors = processor.process_file(
                file_path=str(file), callback_requests=callbacks, pickle_dags=True
            )
            sum_dags += dags
            sum_errors += errors
    console.print(f"Found {sum_dags} dags with {sum_errors} errors")
    return sum_dags, sum_errors


def file_processor_test(num_callbacks: int, processor: DagFileProcessor, num_repeats: int):
    total_dags = 0
    total_errors = 0
    for i in range(num_repeats):
        dags, errors = process_example_files(num_callbacks, processor)
        total_dags += dags
        total_errors += errors
    console.print(f"Total found {total_dags} dags with {total_errors} errors")


@cli_utils.action_cli
def internal_api_client(args):
    use_grpc = args.use_grpc
    num_repeats = args.num_repeats
    processor = DagFileProcessor(
        dag_ids=[],
        log=logging.getLogger('airflow'),
        use_grpc=use_grpc,
        channel=grpc.insecure_channel('localhost:50051') if use_grpc else None,
    )
    if args.test == "file_processor":
        file_processor_test(args.num_callbacks, processor=processor, num_repeats=num_repeats)
    else:
        console.print(f"[red]Wrong test {args.test}")

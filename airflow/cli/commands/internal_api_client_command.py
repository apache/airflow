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
from typing import List, Tuple

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
from airflow.api_internal.internal_api_decorator import internal_api_call

console = Console(width=400, color_system="standard")

example_dags_folder ='/opt/airflow/airflow/example_dags'

processor = DagFileProcessor(
    dag_ids=[],
    log=logging.getLogger("airflow"),
    dag_directory=example_dags_folder,
)


@internal_api_call("process_file")
def process_file(
    file_path: str, callback_requests: List[CallbackRequest], pickle_dags: bool = False
) -> Tuple[int, int]:
    return processor.process_file(
        file_path=file_path,
        callback_requests=callback_requests,
        pickle_dags=pickle_dags,
    )


def process_example_files(num_callbacks: int):
    callback_base = [
        TaskCallbackRequest(
            full_filepath=f"{example_dags_folder}/example_python_operator.py",
            processor_subdir=example_dags_folder,
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
                    # "pod_override": k8s.V1Pod(metadata=k8s.V1ObjectMeta(annotations={"test": "annotation"})),
                },
                pool="pool",
                queue="queue",
                key=TaskInstanceKey(dag_id="dag", task_id="task_id", run_id="run_id"),
                run_as_user="user",
            ),
        ),
        DagCallbackRequest(
            full_filepath=f"{example_dags_folder}/example_bash_operator.py",
            dag_id="example_bash_operator",
            run_id="run_after_loop",
            is_failure_callback=False,
            msg="Error Message",
            processor_subdir=example_dags_folder,
        ),
        SlaCallbackRequest(
            full_filepath=f"{example_dags_folder}/example_bash_operator.py",
            dag_id="example_bash_operator",
            msg="Error message",
            processor_subdir=example_dags_folder,
        ),
    ]
    callbacks: List[CallbackRequest] = []
    for i in range(num_callbacks):
        callbacks.extend(callback_base)
    sum_dags = 0
    sum_errors = 0
    for file in Path(example_dags_folder).iterdir():
        if file.is_file() and file.name.endswith(".py"):
            dags, errors = process_file(file_path=str(file), callback_requests=callbacks, pickle_dags=True)
            sum_dags += dags
            sum_errors += errors
    console.print(f"Found {sum_dags} dags with {sum_errors} errors")
    return sum_dags, sum_errors


def file_processor_test(num_callbacks: int, num_repeats: int):
    total_dags = 0
    total_errors = 0
    for i in range(num_repeats):
        dags, errors = process_example_files(num_callbacks)
        total_dags += dags
        total_errors += errors
    console.print(f"Total found {total_dags} dags with {total_errors} errors")


@cli_utils.action_cli
def internal_api_client(args):
    num_repeats = args.num_repeats
    if args.test == "file_processor":
        file_processor_test(args.num_callbacks, num_repeats=num_repeats)
    else:
        console.print(f"[red]Wrong test {args.test}")

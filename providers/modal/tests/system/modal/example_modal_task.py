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
"""Run Python functions on Modal with ``@task.modal``, in images that do not contain Airflow."""

from __future__ import annotations

import modal

from airflow.sdk import dag, task


@dag(tags=["example", "modal"])
def example_modal_task():
    @task
    def extract() -> list[dict]:
        return [
            {"id": 1, "email": "ada@example.com", "score": "0.91"},
            {"id": 2, "email": "grace@example.com", "score": "0.47"},
        ]

    # [START howto_decorator_modal]
    @task.modal(
        image=modal.Image.debian_slim().pip_install("pydantic<2"),
        memory=1024,
        timeout=600,
    )
    def validate(records: list[dict]) -> dict:
        # pydantic 1 cannot be installed next to Airflow; inside the sandbox it is the only one.
        import pydantic

        class Record(pydantic.BaseModel):
            id: int
            email: str
            score: float

        validated = [Record(**record).dict() for record in records]
        print(f"Validated {len(validated)} records with pydantic {pydantic.VERSION}")
        return {"pydantic_version": pydantic.VERSION, "records": validated}

    # [END howto_decorator_modal]

    # [START howto_decorator_modal_deferrable]
    @task.modal(gpu="T4", timeout=900, deferrable=True)
    def gpu_report() -> str:
        import subprocess

        return subprocess.run(
            ["nvidia-smi", "--query-gpu=name,memory.total", "--format=csv,noheader"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()

    # [END howto_decorator_modal_deferrable]

    validate(extract())
    gpu_report()


example_dag = example_modal_task()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example Dag with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(example_dag)

#
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

import re
from datetime import datetime, timedelta

from airflow.providers.apache.seatunnel.operators.seatunnel_operator import (
    SeaTunnelOperator,
)
from airflow.providers.apache.seatunnel.sensors.seatunnel_sensor import (
    SeaTunnelJobSensor,
)
from airflow.sdk import DAG
from airflow.sdk.definitions.decorators import task

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Function to extract job_id from command output
@task
def extract_job_id():
    # Get SeaTunnel task output from XCom
    from airflow.sdk.definitions.context import get_current_context

    context = get_current_context()
    seatunnel_output = context["ti"].xcom_pull(task_ids="start_seatunnel_job")

    # Use regex to find job_id, adjust this pattern based on actual output format
    # Example regex assuming job_id appears as "Job id:" or "Job ID:" or similar format
    # Could also be "Job xxx successfully started" format
    job_id_pattern = r"[Jj]ob\s+(?:[Ii][Dd]:\s*)?([a-zA-Z0-9-]+)"

    match = re.search(job_id_pattern, seatunnel_output)
    if match:
        job_id = match.group(1)
        print(f"Extracted job ID: {job_id}")
        return job_id
    # If unable to extract job_id, return a default error message
    error_msg = "Could not extract job ID from SeaTunnel output"
    print(error_msg)
    raise ValueError(error_msg)


with DAG(
    "seatunnel_sensor_example",
    default_args=default_args,
    description="Example DAG demonstrating the SeaTunnelJobSensor",
    schedule=None,
    start_date=datetime.now() - timedelta(days=1),
    tags=["example", "seatunnel", "sensor"],
) as dag:
    # First, we start a SeaTunnel job
    # Note: This example works with Zeta engine only, as it exposes a REST API
    start_job = SeaTunnelOperator(
        task_id="start_seatunnel_job",
        config_content="""
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  # This is a example source plugin **only for test and demonstrate
  # the feature source plugin**
  FakeSource {
    plugin_output = "fake"
    parallelism = 1
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

transform {
}

sink {
  console {
    plugin_input="fake"
  }
}
        """,
        engine="zeta",
        seatunnel_conn_id="seatunnel_default",
    )

    # Add task to extract job_id
    extract_id = extract_job_id()

    # Use extracted job_id for monitoring
    wait_for_job = SeaTunnelJobSensor(
        task_id="wait_for_job_completion",
        job_id="{{ task_instance.xcom_pull(task_ids='extract_job_id') }}",
        # Use the extracted job_id
        target_states=["FINISHED"],
        seatunnel_conn_id="seatunnel_default",
        poke_interval=10,  # Check every 10 seconds
        timeout=600,  # Timeout after 10 minutes
    )

    # Define the task dependency
    start_job >> extract_id >> wait_for_job

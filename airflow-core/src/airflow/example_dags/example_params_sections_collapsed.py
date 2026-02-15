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
"""Example DAG demonstrating the section_collapsed parameter for trigger form sections.

This DAG shows how to use the ``section_collapsed`` attribute on ``Param`` to control
which sections of the trigger form start expanded and which start collapsed.
The first ``Param`` in each section that sets ``section_collapsed=True`` will cause
the entire section to be rendered collapsed by default. Sections without this flag
start expanded. Users can still manually expand or collapse any section.
"""

from __future__ import annotations

import datetime
import json
from pathlib import Path

from airflow.sdk import DAG, Param, task

with DAG(
    dag_id=Path(__file__).stem,
    dag_display_name="Params Sections Collapsed",
    description=__doc__.partition(".")[0],
    doc_md=__doc__,
    schedule=None,
    start_date=datetime.datetime(2022, 3, 4),
    catchup=False,
    tags=["example", "params", "ui"],
    params={
        # --- Section: Basic settings (expanded by default) ---
        "environment": Param(
            "production",
            type="string",
            title="Target environment",
            description="Select the environment to deploy to.",
            enum=["development", "staging", "production"],
            section="Basic settings",
        ),
        "dry_run": Param(
            False,
            type="boolean",
            title="Dry run",
            description="If enabled, no actual changes will be made.",
            section="Basic settings",
        ),
        # --- Section: Notification settings (expanded by default) ---
        "notify_on_success": Param(
            True,
            type="boolean",
            title="Notify on success",
            description="Send a notification when the DAG run succeeds.",
            section="Notification settings",
        ),
        "notify_on_failure": Param(
            True,
            type="boolean",
            title="Notify on failure",
            description="Send a notification when the DAG run fails.",
            section="Notification settings",
        ),
        "notification_email": Param(
            "team@example.com",
            type="string",
            title="Notification email",
            description="Email address for notifications.",
            section="Notification settings",
        ),
        # --- Section: Advanced options (collapsed by default) ---
        "max_retries": Param(
            3,
            type="integer",
            title="Max retries",
            description="Maximum number of retries for failed tasks.",
            minimum=0,
            maximum=10,
            section="Advanced options",
            section_collapsed=True,
        ),
        "retry_delay_seconds": Param(
            60,
            type="integer",
            title="Retry delay (seconds)",
            description="Delay between retries in seconds.",
            minimum=10,
            maximum=600,
            section="Advanced options",
        ),
        "timeout_minutes": Param(
            30,
            type="integer",
            title="Timeout (minutes)",
            description="Overall timeout for the DAG run in minutes.",
            minimum=1,
            maximum=120,
            section="Advanced options",
        ),
        # --- Section: Debug (collapsed by default) ---
        "verbose_logging": Param(
            False,
            type="boolean",
            title="Verbose logging",
            description="Enable verbose logging for debugging purposes.",
            section="Debug",
            section_collapsed=True,
        ),
        "log_level": Param(
            "INFO",
            type="string",
            title="Log level",
            description="Set the log level.",
            enum=["DEBUG", "INFO", "WARNING", "ERROR"],
            section="Debug",
        ),
        "dump_config": Param(
            False,
            type="boolean",
            title="Dump config",
            description="Print the full configuration to the logs before running.",
            section="Debug",
        ),
    },
) as dag:

    @task(task_display_name="Show configuration")
    def show_config(**kwargs) -> None:
        params = kwargs["params"]
        print(f"DAG triggered with configuration:\n\n{json.dumps(params, indent=4)}\n")

    show_config()

#!/usr/bin/env python
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
Complete Data Lifecycle Management Example DAG

This DAG demonstrates a complete data lifecycle workflow:
1. Lists all files in a landing directory (OzoneListOperator)
2. Runs a processing step over the discovered list of files
3. Archives processed files to a new location (OzoneMoveOperator)
4. Creates a disaster-recovery snapshot (OzoneBackupOperator)
5. Cleans up original files from the landing zone

This example showcases:
- Data archiving and lifecycle management
- Backup and disaster recovery workflows
"""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import PurePosixPath

from airflow.providers.apache.ozone.operators.ozone import (
    OzoneCreateBucketOperator,
    OzoneCreatePathOperator,
    OzoneCreateVolumeOperator,
    OzoneDeleteKeyOperator,
    OzoneListOperator,
    OzoneMoveOperator,
)
from airflow.providers.apache.ozone.transfers.ozone_backup import OzoneBackupOperator
from airflow.providers.apache.ozone.version_compat import AIRFLOW_V_3_0_PLUS, timezone

if AIRFLOW_V_3_0_PLUS:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.sdk import DAG, TaskGroup
    from airflow.sdk.definitions.param import Param
else:
    from airflow import DAG  # type: ignore[attr-defined,no-redef]
    from airflow.models.param import Param  # type: ignore[no-redef]
    from airflow.operators.bash import BashOperator  # type: ignore[no-redef]
    from airflow.utils.task_group import TaskGroup  # type: ignore[attr-defined,no-redef]

DEFAULT_OM_HOST = os.getenv("OZONE_EXAMPLE_OM_HOST") or "om"
DEFAULT_CONN_ID = os.getenv("OZONE_EXAMPLE_LIFECYCLE_CONN_ID") or "ozone_admin_default"
DEFAULT_LANDING_VOLUME = os.getenv("OZONE_EXAMPLE_LIFECYCLE_LANDING_VOLUME") or "landing"
DEFAULT_LANDING_BUCKET = os.getenv("OZONE_EXAMPLE_LIFECYCLE_LANDING_BUCKET") or "raw"
DEFAULT_ARCHIVE_VOLUME = os.getenv("OZONE_EXAMPLE_LIFECYCLE_ARCHIVE_VOLUME") or "archive"
DEFAULT_ARCHIVE_BUCKET = os.getenv("OZONE_EXAMPLE_LIFECYCLE_ARCHIVE_BUCKET") or "processed"
DEFAULT_SNAPSHOT_PREFIX = os.getenv("OZONE_EXAMPLE_LIFECYCLE_SNAPSHOT_PREFIX") or "snap"

with DAG(
    dag_id="example_ozone_data_lifecycle",
    start_date=timezone.datetime(2025, 1, 1),
    catchup=False,
    schedule=None,
    tags=["ozone", "example"],
    params={
        "om_host": Param(DEFAULT_OM_HOST, type="string", title="OM host / Ozone authority"),
        "lifecycle_conn_id": Param(DEFAULT_CONN_ID, type="string", title="Ozone connection ID"),
        "landing_volume": Param(DEFAULT_LANDING_VOLUME, type="string", title="Landing volume"),
        "landing_bucket": Param(DEFAULT_LANDING_BUCKET, type="string", title="Landing bucket"),
        "archive_volume": Param(DEFAULT_ARCHIVE_VOLUME, type="string", title="Archive volume"),
        "archive_bucket": Param(DEFAULT_ARCHIVE_BUCKET, type="string", title="Archive bucket"),
        "snapshot_prefix": Param(
            DEFAULT_SNAPSHOT_PREFIX,
            type="string",
            title="Snapshot name prefix",
            description="Run date suffix is added automatically as ds_nodash.",
        ),
    },
    doc_md="""
    ### Data Lifecycle Example (with Hive Registration)

    This DAG demonstrates a complete data lifecycle management workflow:
    1. **List**: Finds all files in a landing directory using `OzoneListOperator`.
    2. **Process**: Runs a processing step over the file list from XCom.
    3. **Archive**: Moves the processed source files to an archive path using `OzoneMoveOperator`.
    4. **Backup**: Creates a disaster-recovery snapshot of the bucket using `OzoneBackupOperator`.
    5. **Cleanup**: Deletes the original files from the landing directory.

    Runtime values can be overridden from the Trigger UI or via `dag_run.conf`.
    """,
) as dag:
    LANDING_PATH = (
        f"ofs://{{{{ params.om_host }}}}/"
        f"{PurePosixPath('{{ params.landing_volume }}', '{{ params.landing_bucket }}')}"
    )
    ARCHIVE_BASE_PATH = (
        f"ofs://{{{{ params.om_host }}}}/"
        f"{PurePosixPath('{{ params.archive_volume }}', '{{ params.archive_bucket }}')}"
    )

    # 0. Ensure required volumes and buckets exist.
    create_landing_volume = OzoneCreateVolumeOperator(
        task_id="create_landing_volume",
        volume_name="{{ params.landing_volume }}",
        ozone_conn_id="{{ params.lifecycle_conn_id }}",
        execution_timeout=timedelta(minutes=1),
    )
    create_archive_volume = OzoneCreateVolumeOperator(
        task_id="create_archive_volume",
        volume_name="{{ params.archive_volume }}",
        ozone_conn_id="{{ params.lifecycle_conn_id }}",
        execution_timeout=timedelta(minutes=1),
    )

    create_landing_bucket = OzoneCreateBucketOperator(
        task_id="create_landing_bucket",
        volume_name="{{ params.landing_volume }}",
        bucket_name="{{ params.landing_bucket }}",
        ozone_conn_id="{{ params.lifecycle_conn_id }}",
        execution_timeout=timedelta(minutes=1),
    )
    create_archive_bucket = OzoneCreateBucketOperator(
        task_id="create_archive_bucket",
        volume_name="{{ params.archive_volume }}",
        bucket_name="{{ params.archive_bucket }}",
        ozone_conn_id="{{ params.lifecycle_conn_id }}",
        execution_timeout=timedelta(minutes=1),
    )

    # 1. List all files in the landing directory. The result is pushed to XComs.
    list_files_in_landing_zone = OzoneListOperator(
        task_id="list_files_in_landing_zone",
        path=LANDING_PATH,
        ozone_conn_id="{{ params.lifecycle_conn_id }}",
        execution_timeout=timedelta(minutes=1),
    )

    # 2. This TaskGroup emulates file processing in a single task.
    with TaskGroup(group_id="dynamic_file_processing") as processing_group:
        process_files = BashOperator(
            task_id="process_files",
            bash_command=(
                'echo "Processing files: {{ ti.xcom_pull(task_ids="list_files_in_landing_zone") }}"'
            ),
            execution_timeout=timedelta(minutes=1),
        )

    # 3. Create archive directory for date-based partitioning
    create_archive_dir = OzoneCreatePathOperator(
        task_id="create_archive_dir",
        path=ARCHIVE_BASE_PATH + "/ds={{ ds }}",
        ozone_conn_id="{{ params.lifecycle_conn_id }}",
        execution_timeout=timedelta(minutes=1),
    )

    # 4. After processing, move the original files to an archive directory.
    archive_files = OzoneMoveOperator(
        task_id="archive_landing_files",
        source_path=f"{LANDING_PATH}/*",  # Move all files
        dest_path=ARCHIVE_BASE_PATH + "/ds={{ ds }}",
        ozone_conn_id="{{ params.lifecycle_conn_id }}",
        execution_timeout=timedelta(minutes=5),
    )

    # 5. Create a snapshot of the entire archive volume for backup.
    backup_archive = OzoneBackupOperator(
        task_id="backup_archive_via_snapshot",
        volume="{{ params.archive_volume }}",
        bucket="{{ params.archive_bucket }}",
        snapshot_name="{{ params.snapshot_prefix }}-{{ ds_nodash }}",
        ozone_conn_id="{{ params.lifecycle_conn_id }}",
        execution_timeout=timedelta(minutes=5),
    )

    # 6. Cleanup original files from landing zone.
    cleanup_landing_zone = OzoneDeleteKeyOperator(
        task_id="cleanup_landing_zone",
        path=f"{LANDING_PATH}/*",
        ozone_conn_id="{{ params.lifecycle_conn_id }}",
        execution_timeout=timedelta(minutes=1),
    )

    create_landing_volume >> create_landing_bucket
    create_archive_volume >> create_archive_bucket

    create_landing_bucket >> list_files_in_landing_zone

    list_files_in_landing_zone >> processing_group
    create_archive_bucket >> create_archive_dir
    processing_group >> create_archive_dir >> archive_files
    archive_files >> backup_archive
    backup_archive >> cleanup_landing_zone

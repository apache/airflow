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
Basic Ozone Usage Example DAG

This DAG demonstrates fundamental native Ozone operations:
- Creates a volume and bucket via Native Admin CLI
- Creates directories and uploads files via Ozone Filesystem (ofs://)
- Uses a sensor to wait for a file to appear

The same DAG works for plain, SSL, Kerberos, and SSL+Kerberos modes.
The active mode is defined by the selected Airflow connection and its
``Connection Extra``. Runtime values can be overridden from the Trigger UI or
via ``dag_run.conf``; legacy ``OZONE_EXAMPLE_*`` environment variables are kept
as defaults for compatibility.
"""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import PurePosixPath

from airflow.providers.apache.ozone.hooks.ozone import OzoneAdminHook
from airflow.providers.apache.ozone.operators.ozone import (
    OzoneCreateBucketOperator,
    OzoneCreatePathOperator,
    OzoneCreateVolumeOperator,
    OzoneUploadContentOperator,
)
from airflow.providers.apache.ozone.sensors.ozone import OzoneKeySensor
from airflow.providers.apache.ozone.utils.errors import OzoneProviderError
from airflow.providers.apache.ozone.version_compat import AIRFLOW_V_3_0_PLUS, timezone

if AIRFLOW_V_3_0_PLUS:
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.sdk import DAG
    from airflow.sdk.definitions.param import Param
else:
    from airflow import DAG  # type: ignore[attr-defined,no-redef]
    from airflow.models.param import Param  # type: ignore[no-redef]
    from airflow.operators.python import PythonOperator  # type: ignore[no-redef]

DEFAULT_OM_HOST = os.getenv("OZONE_EXAMPLE_OM_HOST") or "om"
DEFAULT_VOLUME = os.getenv("OZONE_EXAMPLE_USAGE_VOLUME") or "vol1"
DEFAULT_BUCKET = os.getenv("OZONE_EXAMPLE_USAGE_BUCKET") or "bucket-native"
DEFAULT_DIR = os.getenv("OZONE_EXAMPLE_USAGE_DIR") or "data_dir"
DEFAULT_FILE = os.getenv("OZONE_EXAMPLE_USAGE_FILE") or "file.txt"
DEFAULT_CONN_ID = os.getenv("OZONE_EXAMPLE_USAGE_ADMIN_CONN_ID") or "ozone_admin_default"
DEFAULT_VOLUME_QUOTA = os.getenv("OZONE_EXAMPLE_USAGE_VOLUME_QUOTA") or "10GB"
DEFAULT_BUCKET_QUOTA = os.getenv("OZONE_EXAMPLE_USAGE_BUCKET_QUOTA") or "10GB"


def _check_ozone_auth(**context) -> None:
    conn_id = context["params"]["admin_conn_id"]
    hook = OzoneAdminHook(ozone_conn_id=conn_id)
    ok, message = hook.test_connection()
    if not ok:
        raise OzoneProviderError(message)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "example_ozone_usage",
    start_date=timezone.datetime(2024, 1, 1),
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["ozone", "example"],
    params={
        "om_host": Param(DEFAULT_OM_HOST, type="string", title="OM host / Ozone authority"),
        "admin_conn_id": Param(DEFAULT_CONN_ID, type="string", title="Ozone admin connection ID"),
        "volume": Param(DEFAULT_VOLUME, type="string", title="Volume name"),
        "bucket": Param(DEFAULT_BUCKET, type="string", title="Bucket name"),
        "directory": Param(DEFAULT_DIR, type="string", title="Directory path inside bucket"),
        "file_name": Param(DEFAULT_FILE, type="string", title="File name"),
        "volume_quota": Param(
            DEFAULT_VOLUME_QUOTA,
            type="string",
            title="Volume quota",
            description="Quota string accepted by Ozone CLI, for example 10GB.",
        ),
        "bucket_quota": Param(
            DEFAULT_BUCKET_QUOTA,
            type="string",
            title="Bucket quota",
            description="Quota string accepted by Ozone CLI, for example 10GB.",
        ),
    },
) as dag:
    FS_DIR_PATH = (
        f"ofs://{{{{ params.om_host }}}}/"
        f"{PurePosixPath('{{ params.volume }}', '{{ params.bucket }}', '{{ params.directory }}')}"
    )
    FS_FILE_PATH = (
        f"ofs://{{{{ params.om_host }}}}/"
        f"{PurePosixPath('{{ params.volume }}', '{{ params.bucket }}', '{{ params.directory }}', '{{ params.file_name }}')}"
    )

    check_ozone_auth = PythonOperator(
        task_id="check_ozone_auth",
        python_callable=_check_ozone_auth,
        execution_timeout=timedelta(seconds=30),
    )

    create_vol = OzoneCreateVolumeOperator(
        task_id="create_volume",
        volume_name="{{ params.volume }}",
        quota="{{ params.volume_quota }}",
        ozone_conn_id="{{ params.admin_conn_id }}",
        execution_timeout=timedelta(minutes=5),
    )

    create_bucket_native = OzoneCreateBucketOperator(
        task_id="create_bucket_native",
        volume_name="{{ params.volume }}",
        bucket_name="{{ params.bucket }}",
        quota="{{ params.bucket_quota }}",
        ozone_conn_id="{{ params.admin_conn_id }}",
        execution_timeout=timedelta(minutes=5),
    )

    fs_mkdir = OzoneCreatePathOperator(
        task_id="fs_mkdir",
        path=FS_DIR_PATH,
        ozone_conn_id="{{ params.admin_conn_id }}",
        execution_timeout=timedelta(minutes=5),
    )

    fs_put = OzoneUploadContentOperator(
        task_id="fs_put_file",
        content="Hello from FS Layer",
        remote_path=FS_FILE_PATH,
        ozone_conn_id="{{ params.admin_conn_id }}",
        if_exists="overwrite",
        execution_timeout=timedelta(minutes=5),
    )

    wait_fs_file = OzoneKeySensor(
        task_id="wait_fs_file",
        path=FS_FILE_PATH,
        ozone_conn_id="{{ params.admin_conn_id }}",
        mode="reschedule",
        timeout=60,
    )

    check_ozone_auth >> create_vol >> create_bucket_native >> fs_mkdir >> fs_put >> wait_fs_file

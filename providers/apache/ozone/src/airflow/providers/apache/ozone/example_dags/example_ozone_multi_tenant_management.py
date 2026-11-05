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
Multi-Tenant Management Example DAG

This DAG demonstrates how to automate the setup of storage resources for new projects/tenants:
1. Creates a dedicated volume for a new project
2. Sets quota limits to prevent resource overuse
3. Creates standard directory structure (landing, processed, etc.)

This example shows how Airflow can be used for administrative tasks,
automating the provisioning of storage resources for new teams or projects.
Useful for multi-tenant environments where each project needs isolated storage.

Runtime values can be overridden from the Trigger UI or via `dag_run.conf`;
legacy `OZONE_EXAMPLE_*` environment variables are kept as defaults for
compatibility.
"""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import PurePosixPath

from airflow.providers.apache.ozone.operators.ozone import (
    OzoneCreateBucketOperator,
    OzoneCreatePathOperator,
    OzoneCreateVolumeOperator,
    OzoneSetQuotaOperator,
)
from airflow.providers.apache.ozone.version_compat import AIRFLOW_V_3_0_PLUS, timezone

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG
    from airflow.sdk.definitions.param import Param
else:
    from airflow import DAG  # type: ignore[attr-defined,no-redef]
    from airflow.models.param import Param  # type: ignore[no-redef]

DEFAULT_OM_HOST = os.getenv("OZONE_EXAMPLE_OM_HOST") or "om"
DEFAULT_CONN_ID = os.getenv("OZONE_EXAMPLE_MULTI_TENANT_CONN_ID") or "ozone_admin_default"
DEFAULT_PROJECT_VOLUME = os.getenv("OZONE_EXAMPLE_MULTI_TENANT_PROJECT_VOLUME") or "project-alpha"
DEFAULT_PROJECT_QUOTA = os.getenv("OZONE_EXAMPLE_MULTI_TENANT_PROJECT_QUOTA") or "10GB"
DEFAULT_LANDING_BUCKET = os.getenv("OZONE_EXAMPLE_MULTI_TENANT_LANDING_BUCKET") or "landing"
DEFAULT_PROCESSED_BUCKET = os.getenv("OZONE_EXAMPLE_MULTI_TENANT_PROCESSED_BUCKET") or "processed"
DEFAULT_BUCKET_QUOTA = os.getenv("OZONE_EXAMPLE_MULTI_TENANT_BUCKET_QUOTA") or "1GB"

with DAG(
    dag_id="example_ozone_multi_tenant_management",
    start_date=timezone.datetime(2025, 1, 1),
    catchup=False,
    schedule=None,
    tags=["ozone", "example"],
    params={
        "om_host": Param(DEFAULT_OM_HOST, type="string", title="OM host / Ozone authority"),
        "multi_tenant_conn_id": Param(DEFAULT_CONN_ID, type="string", title="Ozone connection ID"),
        "project_volume": Param(DEFAULT_PROJECT_VOLUME, type="string", title="Project volume"),
        "project_quota": Param(DEFAULT_PROJECT_QUOTA, type="string", title="Project quota"),
        "landing_bucket": Param(DEFAULT_LANDING_BUCKET, type="string", title="Landing bucket"),
        "processed_bucket": Param(DEFAULT_PROCESSED_BUCKET, type="string", title="Processed bucket"),
        "bucket_quota": Param(DEFAULT_BUCKET_QUOTA, type="string", title="Bucket quota"),
    },
) as dag:
    LANDING_DATA_PATH = (
        f"ofs://{{{{ params.om_host }}}}/"
        f"{PurePosixPath('{{ params.project_volume }}', '{{ params.landing_bucket }}', 'data')}"
    )
    PROCESSED_DATA_PATH = (
        f"ofs://{{{{ params.om_host }}}}/"
        f"{PurePosixPath('{{ params.project_volume }}', '{{ params.processed_bucket }}', 'data')}"
    )

    # 1. Create a dedicated volume for the new project
    create_volume = OzoneCreateVolumeOperator(
        task_id="create_project_volume",
        volume_name="{{ params.project_volume }}",
        ozone_conn_id="{{ params.multi_tenant_conn_id }}",
        execution_timeout=timedelta(minutes=1),
    )

    # 2. Set a hard limit on the volume to prevent overuse
    set_quota = OzoneSetQuotaOperator(
        task_id="set_project_quota",
        volume="{{ params.project_volume }}",
        quota="{{ params.project_quota }}",
        ozone_conn_id="{{ params.multi_tenant_conn_id }}",
        execution_timeout=timedelta(minutes=1),
    )

    # 3. Create buckets with quotas (required when volume has quota)
    create_landing_bucket = OzoneCreateBucketOperator(
        task_id="create_landing_bucket",
        volume_name="{{ params.project_volume }}",
        bucket_name="{{ params.landing_bucket }}",
        quota="{{ params.bucket_quota }}",
        ozone_conn_id="{{ params.multi_tenant_conn_id }}",
        execution_timeout=timedelta(minutes=1),
    )

    create_processed_bucket = OzoneCreateBucketOperator(
        task_id="create_processed_bucket",
        volume_name="{{ params.project_volume }}",
        bucket_name="{{ params.processed_bucket }}",
        quota="{{ params.bucket_quota }}",
        ozone_conn_id="{{ params.multi_tenant_conn_id }}",
        execution_timeout=timedelta(minutes=1),
    )

    # 4. Create standard subdirectories inside the buckets
    create_landing_dir = OzoneCreatePathOperator(
        task_id="create_landing_dir",
        path=LANDING_DATA_PATH,
        ozone_conn_id="{{ params.multi_tenant_conn_id }}",
        execution_timeout=timedelta(minutes=1),
    )

    create_processed_dir = OzoneCreatePathOperator(
        task_id="create_processed_dir",
        path=PROCESSED_DATA_PATH,
        ozone_conn_id="{{ params.multi_tenant_conn_id }}",
        execution_timeout=timedelta(minutes=1),
    )

    create_volume >> set_quota
    set_quota >> [create_landing_bucket, create_processed_bucket]
    create_landing_bucket >> create_landing_dir
    create_processed_bucket >> create_processed_dir

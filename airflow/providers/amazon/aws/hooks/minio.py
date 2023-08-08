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
"""Interact with MinIO Simple Storage Service (S3)."""
from __future__ import annotations

from functools import cached_property
from typing import Any

import boto3

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class MinIOHook(S3Hook):
    """Interact with MinIO Simple Storage Service (S3)."""

    conn_name_attr = "minio_conn_id"
    default_conn_name = "minio_default"
    conn_type = "minio"
    hook_name = "MinIO storage service"

    def __init__(
        self,
        minio_conn_id: str = default_conn_name,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.minio_conn_id = minio_conn_id

    @cached_property
    def conn(self) -> boto3.client:
        """
        Get the S3 boto3 client (cached).

        :return: boto3.client
        """
        conn = self.get_connection(self.minio_conn_id)
        return boto3.client(
            "s3", aws_access_key_id=conn.login, aws_secret_access_key=conn.password, endpoint_url=conn.host
        )

    def test_connection(self):
        """Check if connection to MinIO is valid."""
        try:
            self.get_conn().list_buckets()
            return True, "Connection successfully tested"
        except Exception as err:
            return False, f"Connection failed: {err}"

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Returns custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port", "extra"],
            "relabeling": {
                "host": "MinIO Server Endpoint",
                "login": "Access Key ID",
                "password": "Secret Access Key",
            },
        }

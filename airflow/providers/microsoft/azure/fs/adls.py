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

from typing import TYPE_CHECKING

from airflow.hooks.base import BaseHook
from airflow.providers.microsoft.azure.utils import get_field

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem

schemes = ["abfs", "abfss", "adl"]


def get_fs(conn_id: str | None) -> AbstractFileSystem:
    from adlfs import AzureBlobFileSystem

    if conn_id is None:
        return AzureBlobFileSystem()

    conn = BaseHook.get_connection(conn_id)
    extras = conn.extra_dejson

    connection_string = get_field(
        conn_id=conn_id, conn_type="azure_data_lake", extras=extras, field_name="connection_string"
    )
    account_name = get_field(
        conn_id=conn_id, conn_type="azure_data_lake", extras=extras, field_name="account_name"
    )
    account_key = get_field(
        conn_id=conn_id, conn_type="azure_data_lake", extras=extras, field_name="account_key"
    )
    sas_token = get_field(conn_id=conn_id, conn_type="azure_data_lake", extras=extras, field_name="sas_token")
    tenant = get_field(conn_id=conn_id, conn_type="azure_data_lake", extras=extras, field_name="tenant")

    return AzureBlobFileSystem(
        connection_string=connection_string,
        account_name=account_name,
        account_key=account_key,
        sas_token=sas_token,
        tenant_id=tenant,
        client_id=conn.login,
        client_secret=conn.password,
    )

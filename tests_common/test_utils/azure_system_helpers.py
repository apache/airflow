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

import json
import os
import random
import string
from contextlib import contextmanager

import pytest

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.fileshare import AzureFileShareHook
from airflow.utils.process_utils import patch_environ

from dev.tests_common.test_utils import AIRFLOW_MAIN_FOLDER
from dev.tests_common.test_utils.system_tests_class import SystemTest

AZURE_DAG_FOLDER = os.path.join(
    AIRFLOW_MAIN_FOLDER, "airflow", "providers", "microsoft", "azure", "example_dags"
)
WASB_CONNECTION_ID = os.environ.get("WASB_CONNECTION_ID", "wasb_default")

DATA_LAKE_CONNECTION_ID = os.environ.get("AZURE_DATA_LAKE_CONNECTION_ID", "azure_data_lake_default")
DATA_LAKE_CONNECTION_TYPE = os.environ.get("AZURE_DATA_LAKE_CONNECTION_TYPE", "azure_data_lake")


@contextmanager
def provide_wasb_default_connection(key_file_path: str):
    """
    Context manager to provide a temporary value for wasb_default connection

    :param key_file_path: Path to file with wasb_default credentials .json file.
    """
    if not key_file_path.endswith(".json"):
        raise AirflowException("Use a JSON key file.")
    with open(key_file_path) as credentials:
        creds = json.load(credentials)
    conn = Connection(
        conn_id=WASB_CONNECTION_ID,
        conn_type="wasb",
        host=creds.get("host", None),
        login=creds.get("login", None),
        password=creds.get("password", None),
        extra=json.dumps(creds.get("extra", None)),
    )
    with patch_environ({f"AIRFLOW_CONN_{conn.conn_id.upper()}": conn.get_uri()}):
        yield


@contextmanager
def provide_azure_data_lake_default_connection(key_file_path: str):
    """
    Context manager to provide a temporary value for azure_data_lake_default connection
    :param key_file_path: Path to file with azure_data_lake_default credentials .json file.
    """
    required_fields = {"login", "password", "extra"}

    if not key_file_path.endswith(".json"):
        raise AirflowException("Use a JSON key file.")
    with open(key_file_path) as credentials:
        creds = json.load(credentials)
    missing_keys = required_fields - creds.keys()
    if missing_keys:
        message = f"{missing_keys} fields are missing"
        raise AirflowException(message)
    conn = Connection(
        conn_id=DATA_LAKE_CONNECTION_ID,
        conn_type=DATA_LAKE_CONNECTION_TYPE,
        host=creds.get("host", None),
        login=creds.get("login", None),
        password=creds.get("password", None),
        extra=json.dumps(creds.get("extra", None)),
    )
    with patch_environ({f"AIRFLOW_CONN_{conn.conn_id.upper()}": conn.get_uri()}):
        yield


@contextmanager
def provide_azure_fileshare(share_name: str, azure_fileshare_conn_id: str, file_name: str, directory: str):
    AzureSystemTest.prepare_share(
        share_name=share_name,
        azure_fileshare_conn_id=azure_fileshare_conn_id,
        file_name=file_name,
        directory=directory,
    )
    yield
    AzureSystemTest.delete_share(share_name=share_name, azure_fileshare_conn_id=azure_fileshare_conn_id)


@pytest.mark.system("azure")
class AzureSystemTest(SystemTest):
    @classmethod
    def create_share(cls, share_name: str, azure_fileshare_conn_id: str):
        hook = AzureFileShareHook(azure_fileshare_conn_id=azure_fileshare_conn_id)
        hook.create_share(share_name)

    @classmethod
    def delete_share(cls, share_name: str, azure_fileshare_conn_id: str):
        hook = AzureFileShareHook(azure_fileshare_conn_id=azure_fileshare_conn_id)
        hook.delete_share(share_name=share_name)

    @classmethod
    def create_directory(cls, share_name: str, azure_fileshare_conn_id: str, directory: str):
        hook = AzureFileShareHook(
            azure_fileshare_conn_id=azure_fileshare_conn_id, share_name=share_name, directory_path=directory
        )
        hook.create_directory()

    @classmethod
    def upload_file_from_string(
        cls,
        string_data: str,
        share_name: str,
        azure_fileshare_conn_id: str,
        file_name: str,
    ):
        hook = AzureFileShareHook(
            azure_fileshare_conn_id=azure_fileshare_conn_id, share_name=share_name, file_path=file_name
        )
        hook.load_data(string_data=string_data)

    @classmethod
    def prepare_share(cls, share_name: str, azure_fileshare_conn_id: str, file_name: str, directory: str):
        """
        Create share with a file in given directory. If directory is None, file is in root dir.
        """
        hook = AzureFileShareHook(
            azure_fileshare_conn_id=azure_fileshare_conn_id,
            share_name=share_name,
            directory_path=directory,
            file_path=file_name,
        )
        hook.create_share(share_name)
        hook.create_directory()

        string_data = "".join(random.choices(string.ascii_letters, k=1024))
        hook.load_data(string_data)

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

import random
import string
from contextlib import contextmanager
from typing import Optional

import pytest

from airflow.providers.microsoft.azure.hooks.azure_fileshare import AzureFileShareHook
from tests.test_utils.system_tests_class import SystemTest


@contextmanager
def provide_azure_fileshare(
    share_name: str, wasb_conn_id: str, file_name: str, directory: Optional[str] = None
):
    AzureSystemTest.prepare_share(
        share_name=share_name,
        wasb_conn_id=wasb_conn_id,
        file_name=file_name,
        directory=directory,
    )
    yield
    AzureSystemTest.delete_share(share_name=share_name, wasb_conn_id=wasb_conn_id)


@pytest.mark.system("azure")
class AzureSystemTest(SystemTest):

    @classmethod
    def create_share(cls, share_name: str, wasb_conn_id: str):
        hook = AzureFileShareHook(wasb_conn_id=wasb_conn_id)
        hook.create_share(share_name)

    @classmethod
    def delete_share(cls, share_name: str, wasb_conn_id: str):
        hook = AzureFileShareHook(wasb_conn_id=wasb_conn_id)
        hook.delete_share(share_name)

    @classmethod
    def create_directory(cls, share_name: str, wasb_conn_id: str, directory: Optional[str] = None):
        hook = AzureFileShareHook(wasb_conn_id=wasb_conn_id)
        hook.create_directory(share_name=share_name, directory_name=directory)

    @classmethod
    def upload_file_from_string(
        cls,
        string_data: str,
        share_name: str,
        wasb_conn_id: str,
        file_name: str,
        directory: Optional[str] = None
    ):
        hook = AzureFileShareHook(wasb_conn_id=wasb_conn_id)
        hook.load_string(
            string_data=string_data,
            share_name=share_name,
            directory_name=directory,
            file_name=file_name,
        )

    @classmethod
    def prepare_share(
        cls, share_name: str, wasb_conn_id: str, file_name: str, directory: Optional[str] = None
    ):
        """
        Create share with a file in given directory. If directory is None, file is in root dir.
        """
        cls.create_share(share_name=share_name, wasb_conn_id=wasb_conn_id)
        cls.create_directory(share_name=share_name, wasb_conn_id=wasb_conn_id, directory=directory)
        string_data = "".join(random.choice(string.ascii_letters) for _ in range(1024))
        cls.upload_file_from_string(
            string_data=string_data,
            share_name=share_name,
            wasb_conn_id=wasb_conn_id,
            file_name=file_name,
            directory=directory,
        )

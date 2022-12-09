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

from typing import Any

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import (
    DataLakeDirectoryClient,
    DataLakeFileClient,
    DataLakeServiceClient,
    FileSystemClient,
)

from airflow.hooks.base import BaseHook


class AzureDataLakeStorageV2(BaseHook):

    conn_name_attr = "adls_v2_conn_id"
    default_conn_name = "adls_v2_default"
    conn_type = "adls_v2"
    hook_name = "Azure Date Lake Storage"

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import PasswordField, StringField

        return {
            "extra__adls_v2__connection_string": PasswordField(
                lazy_gettext("Blob Storage Connection String (optional)"), widget=BS3PasswordFieldWidget()
            ),
            "extra__adls_v2__tenant_id": StringField(
                lazy_gettext("Tenant Id (Active Directory Auth)"), widget=BS3TextFieldWidget()
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ["schema", "port"],
            "relabeling": {
                "login": "Blob Storage Login (optional)",
                "password": "Blob Storage Key (optional)",
                "host": "Account Name (Active Directory Auth)",
            },
            "placeholders": {
                "login": "account name",
                "password": "secret",
                "host": "account url",
                "extra__adls_v2__connection_string": "connection string auth",
                "extra__adls_v2__tenant_id": "tenant",
            },
        }

    def __init__(self, adls_v2_conn_id: str = default_conn_name, public_read: bool = False) -> None:
        super().__init__()
        self.conn_id = adls_v2_conn_id
        self.public_read = public_read
        self.service_client = self.get_conn()

    def get_conn(self) -> DataLakeServiceClient:
        """Return the DataLakeServiceClient object."""
        conn = self.get_connection(self.conn_id)
        extra = conn.extra_dejson or {}

        connection_string = extra.pop(
            "connection_string", extra.pop("extra__adls_v2__connection_string", None)
        )
        if connection_string:
            # connection_string auth takes priority
            return DataLakeServiceClient.from_connection_string(connection_string, **extra)

        tenant = extra.pop("tenant_id", extra.pop("extra__adls_v2__tenant_id", None))
        if tenant:
            # use Active Directory auth
            app_id = conn.login
            app_secret = conn.password
            token_credential = ClientSecretCredential(tenant, app_id, app_secret)
            return DataLakeServiceClient(
                account_url=f"https://{conn.login}.dfs.core.windows.net", credential=token_credential, **extra
            )
        credential = conn.password
        return DataLakeServiceClient(
            account_url=f"https://{conn.login}.dfs.core.windows.net",
            credential=credential,
            **extra,
        )

    def create_file_system(self, file_system_name: str) -> None:
        """
        A container acts as a file system for your files. Creates a new file system under
        the specified account.

        If the file system with the same name already exists, a ResourceExistsError will
        be raised. This method returns a client with which to interact with the newly
        created file system.
        """
        try:
            file_system_client = self.service_client.create_file_system(file_system=file_system_name)
            print("file_system_client ", file_system_client.file_system_name)
            self.log.info("Created file system: %s", file_system_client.file_system_name)
        except ResourceExistsError:
            self.log.info("Attempted to create file system %r but it already exists.", file_system_name)
        except Exception as e:
            self.log.info("Error while attempting to create file system %r: %s", file_system_name, e)
            raise

    def get_file_system(self, file_system_name: str) -> FileSystemClient:
        try:
            file_system_client = self.service_client.get_file_system_client(file_system=file_system_name)
            return file_system_client
        except ResourceNotFoundError:
            self.log.info("file system %r doesn't exists.", file_system_name)
        except Exception as e:
            self.log.info("Error while attempting to get file system %r: %s", file_system_name, e)
            raise

    def create_directory(self, file_system_name: str, directory_name: str) -> None:
        try:
            file_system = self.get_file_system(file_system_name)
            file_system.create_directory(directory_name)
        except Exception as e:
            self.log.info(e)
            raise

    def get_directory_client(self, file_system_name: str, directory_name: str) -> DataLakeDirectoryClient:
        try:
            file_system_client = self.get_file_system(file_system_name)
            directory_client = file_system_client.get_directory_client(directory_name)
            return directory_client
        except ResourceNotFoundError:
            self.log.info(
                "Directory %s doesn't exists in the file system %s", directory_name, file_system_name
            )
        except Exception as e:
            self.log.info(e)
            raise

    def create_file(self, file_system_name: str, file_name: str) -> DataLakeFileClient:
        try:
            file_system = self.get_file_system(file_system_name)
            file_client = file_system.create_file(file_name)
            return file_client
        except Exception as e:
            self.log.info(e)
            raise

    def upload_file(self,
                    file_system_name: str,
                    file_name: str,
                    file_path: str,
                    overwrite: bool = False) -> None:
        file_client = self.create_file(file_system_name, file_name)
        with open(file_path, "rb") as data:
            file_client.upload_data(data, overwrite=overwrite)

    def upload_file_to_directory(
        self,
        file_system_name: str,
        directory_name: str,
        file_name: str,
        file_path: str,
        overwrite: bool = False,
    ) -> None:
        directory_client = self.get_directory_client(file_system_name, directory_name=directory_name)
        file_client = directory_client.create_file(file_name)
        with open(file_path, "rb") as data:
            file_client.upload_data(data, overwrite=overwrite)

    def list_directory(self, file_system_name: str, directory_name: str) -> list[str]:
        file_system_client = self.get_file_system(file_system_name=file_system_name)
        paths = file_system_client.get_paths(directory_name)
        directory_lists = []
        for path in paths:
            directory_lists.append(path.name)
        return directory_lists

    def list_file_system(self, prefix: str | None = None, include_metadata: bool = False) -> list[str]:
        file_system = self.service_client.list_file_systems(
            name_starts_with=prefix, include_metadata=include_metadata
        )
        file_system_list = []
        for fs in file_system:
            file_system_list.append(fs.name)
        return file_system_list

    def delete_file_system(self, file_system_name: str) -> None:
        """Delete file system"""
        try:
            self.service_client.delete_file_system(file_system_name)
            self.log.info("Deleted file system: %s", file_system_name)
        except ResourceNotFoundError:
            self.log.info("file system %r doesn't exists.", file_system_name)
        except Exception as e:
            self.log.info("Error while attempting to deleting file system %r: %s", file_system_name, e)
            raise

    def delete_directory(self, file_system_name: str, directory_name: str) -> None:
        directory_client = self.get_directory_client(file_system_name, directory_name)
        directory_client.delete_directory()

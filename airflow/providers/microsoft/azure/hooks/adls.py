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

from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import (
    DataLakeDirectoryClient,
    DataLakeFileClient,
    DataLakeServiceClient,
    FileSystemClient,
)


class AzureDataLakeStorageGen2(WasbHook):

    def __init__(self, adls_v2_conn_id: str, public_read: bool = False) -> None:
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

    def create_directory(self, file_system_name: str, directory_name: str) -> DataLakeDirectoryClient:
        result = self.get_file_system(file_system_name).create_directory(directory_name)
        return result

    def get_directory_client(self, file_system_name: str, directory_name: str) -> DataLakeDirectoryClient:
        try:
            directory_client = self.get_file_system(file_system_name).get_directory_client(directory_name)
            return directory_client
        except ResourceNotFoundError:
            self.log.info(
                "Directory %s doesn't exists in the file system %s", directory_name, file_system_name
            )
        except Exception as e:
            self.log.info(e)
            raise

    def create_file(self, file_system_name: str, file_name: str) -> DataLakeFileClient:
        file_client = self.get_file_system(file_system_name).create_file(file_name)
        return file_client

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
        **kwargs: any,
    ) -> None:
        """
        Create a new file and return the file client to be interacted with and then
        upload data to a file
        """
        directory_client = self.get_directory_client(file_system_name, directory_name=directory_name)
        file_client = directory_client.create_file(file_name, kwargs=kwargs)
        with open(file_path, "rb") as data:
            file_client.upload_data(data, overwrite=overwrite, kwargs=kwargs)

    def list_files_directory(self, file_system_name: str, directory_name: str) -> list[str]:
        """Get the list of files or directories under the specified file system"""
        paths = self.get_file_system(file_system_name=file_system_name).get_paths(directory_name)
        directory_lists = []
        for path in paths:
            directory_lists.append(path.name)
        return directory_lists

    def list_file_system(self, prefix: str | None = None, include_metadata: bool = False) -> list[str]:
        """Get the list the file systems under the specified account."""
        file_system = self.service_client.list_file_systems(
            name_starts_with=prefix, include_metadata=include_metadata
        )
        file_system_list = []
        for fs in file_system:
            file_system_list.append(fs.name)
        return file_system_list

    def delete_file_system(self, file_system_name: str) -> None:
        """Deletes the file system"""
        try:
            self.service_client.delete_file_system(file_system_name)
            self.log.info("Deleted file system: %s", file_system_name)
        except ResourceNotFoundError:
            self.log.info("file system %r doesn't exists.", file_system_name)
        except Exception as e:
            self.log.info("Error while attempting to deleting file system %r: %s", file_system_name, e)
            raise

    def delete_directory(self, file_system_name: str, directory_name: str) -> None:
        """Deletes specified directory in file system"""
        directory_client = self.get_directory_client(file_system_name, directory_name)
        directory_client.delete_directory()

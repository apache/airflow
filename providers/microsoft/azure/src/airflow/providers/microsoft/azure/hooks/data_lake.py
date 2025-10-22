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
from __future__ import annotations

from functools import cached_property
from typing import Any, cast

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.datalake.store import core, lib, multithread
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.storage.filedatalake import (
    DataLakeDirectoryClient,
    DataLakeFileClient,
    DataLakeServiceClient,
    DirectoryProperties,
    FileSystemClient,
    FileSystemProperties,
)

from airflow.exceptions import AirflowException
from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.microsoft.azure.utils import (
    AzureIdentityCredentialAdapter,
    add_managed_identity_connection_widgets,
    get_field,
    get_sync_default_azure_credential,
)

Credentials = ClientSecretCredential | AzureIdentityCredentialAdapter | DefaultAzureCredential


class AzureDataLakeHook(BaseHook):
    """
    Integration with Azure Data Lake.

    AzureDataLakeHook communicates via a REST API compatible with WebHDFS. Make
    sure that a Airflow connection of type ``azure_data_lake`` exists.
    Authorization can be done by supplying a *login* (=Client ID), *password*
    (=Client Secret), and extra fields *tenant* (Tenant) and *account_name*
    (Account Name). See connection ``azure_data_lake_default`` for an example.

    Client ID and secret should be in user and password parameters.
    Tenant and account name should be extra field as
    ``{"tenant": "<TENANT>", "account_name": "ACCOUNT_NAME"}``.

    :param azure_data_lake_conn_id: Reference to
        :ref:`Azure Data Lake connection<howto/connection:adl>`.
    """

    conn_name_attr = "azure_data_lake_conn_id"
    default_conn_name = "azure_data_lake_default"
    conn_type = "azure_data_lake"
    hook_name = "Azure Data Lake"

    @classmethod
    @add_managed_identity_connection_widgets
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "tenant": StringField(lazy_gettext("Azure Tenant ID"), widget=BS3TextFieldWidget()),
            "account_name": StringField(
                lazy_gettext("Azure DataLake Store Name"), widget=BS3TextFieldWidget()
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port", "host", "extra"],
            "relabeling": {
                "login": "Azure Client ID",
                "password": "Azure Client Secret",
            },
            "placeholders": {
                "login": "client id",
                "password": "secret",
                "tenant": "tenant id",
                "account_name": "datalake store",
            },
        }

    def __init__(self, azure_data_lake_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.conn_id = azure_data_lake_conn_id
        self._conn: core.AzureDLFileSystem | None = None
        self.account_name: str | None = None

    def _get_field(self, extras, name):
        return get_field(
            conn_id=self.conn_id,
            conn_type=self.conn_type,
            extras=extras,
            field_name=name,
        )

    def get_conn(self) -> core.AzureDLFileSystem:
        """Return a AzureDLFileSystem object."""
        if not self._conn:
            conn = self.get_connection(self.conn_id)
            extras = conn.extra_dejson
            self.account_name = self._get_field(extras, "account_name")

            credential: Credentials
            tenant = self._get_field(extras, "tenant")
            if tenant:
                credential = lib.auth(tenant_id=tenant, client_secret=conn.password, client_id=conn.login)
            else:
                managed_identity_client_id = self._get_field(extras, "managed_identity_client_id")
                workload_identity_tenant_id = self._get_field(extras, "workload_identity_tenant_id")
                credential = AzureIdentityCredentialAdapter(
                    managed_identity_client_id=managed_identity_client_id,
                    workload_identity_tenant_id=workload_identity_tenant_id,
                )
            self._conn = core.AzureDLFileSystem(credential, store_name=self.account_name)
            self._conn.connect()
        return self._conn

    def check_for_file(self, file_path: str) -> bool:
        """
        Check if a file exists on Azure Data Lake.

        :param file_path: Path and name of the file.
        :return: True if the file exists, False otherwise.
        """
        try:
            files = self.get_conn().glob(file_path, details=False, invalidate_cache=True)
            return len(files) == 1
        except FileNotFoundError:
            return False

    def upload_file(
        self,
        local_path: str,
        remote_path: str,
        nthreads: int = 64,
        overwrite: bool = True,
        buffersize: int = 4194304,
        blocksize: int = 4194304,
        **kwargs,
    ) -> None:
        """
        Upload a file to Azure Data Lake.

        :param local_path: local path. Can be single file, directory (in which case,
            upload recursively) or glob pattern. Recursive glob patterns using `**`
            are not supported.
        :param remote_path: Remote path to upload to; if multiple files, this is the
            directory root to write within.
        :param nthreads: Number of threads to use. If None, uses the number of cores.
        :param overwrite: Whether to forcibly overwrite existing files/directories.
            If False and remote path is a directory, will quit regardless if any files
            would be overwritten or not. If True, only matching filenames are actually
            overwritten.
        :param buffersize: int [2**22]
            Number of bytes for internal buffer. This block cannot be bigger than
            a chunk and cannot be smaller than a block.
        :param blocksize: int [2**22]
            Number of bytes for a block. Within each chunk, we write a smaller
            block for each API call. This block cannot be bigger than a chunk.
        """
        multithread.ADLUploader(
            self.get_conn(),
            lpath=local_path,
            rpath=remote_path,
            nthreads=nthreads,
            overwrite=overwrite,
            buffersize=buffersize,
            blocksize=blocksize,
            **kwargs,
        )

    def download_file(
        self,
        local_path: str,
        remote_path: str,
        nthreads: int = 64,
        overwrite: bool = True,
        buffersize: int = 4194304,
        blocksize: int = 4194304,
        **kwargs,
    ) -> None:
        """
        Download a file from Azure Blob Storage.

        :param local_path: local path. If downloading a single file, will write to this
            specific file, unless it is an existing directory, in which case a file is
            created within it. If downloading multiple files, this is the root
            directory to write within. Will create directories as required.
        :param remote_path: remote path/globstring to use to find remote files.
            Recursive glob patterns using `**` are not supported.
        :param nthreads: Number of threads to use. If None, uses the number of cores.
        :param overwrite: Whether to forcibly overwrite existing files/directories.
            If False and remote path is a directory, will quit regardless if any files
            would be overwritten or not. If True, only matching filenames are actually
            overwritten.
        :param buffersize: int [2**22]
            Number of bytes for internal buffer. This block cannot be bigger than
            a chunk and cannot be smaller than a block.
        :param blocksize: int [2**22]
            Number of bytes for a block. Within each chunk, we write a smaller
            block for each API call. This block cannot be bigger than a chunk.
        """
        multithread.ADLDownloader(
            self.get_conn(),
            lpath=local_path,
            rpath=remote_path,
            nthreads=nthreads,
            overwrite=overwrite,
            buffersize=buffersize,
            blocksize=blocksize,
            **kwargs,
        )

    def list(self, path: str) -> list:
        """
        List files in Azure Data Lake Storage.

        :param path: full path/globstring to use to list files in ADLS
        """
        if "*" in path:
            return self.get_conn().glob(path)
        return self.get_conn().walk(path)

    def remove(self, path: str, recursive: bool = False, ignore_not_found: bool = True) -> None:
        """
        Remove files in Azure Data Lake Storage.

        :param path: A directory or file to remove in ADLS
        :param recursive: Whether to loop into directories in the location and remove the files
        :param ignore_not_found: Whether to raise error if file to delete is not found
        """
        try:
            self.get_conn().remove(path=path, recursive=recursive)
        except FileNotFoundError:
            if ignore_not_found:
                self.log.info("File %s not found", path)
            else:
                raise AirflowException(f"File {path} not found")


class AzureDataLakeStorageV2Hook(BaseHook):
    """
    Interact with a ADLS gen2 storage account.

    It mainly helps to create and manage directories and files in storage
    accounts that have a hierarchical namespace. Using Adls_v2 connection
    details create DataLakeServiceClient object.

    Due to Wasb is marked as legacy and retirement of the (ADLS1), it would
    be nice to implement ADLS gen2 hook for interacting with the storage account.

    .. seealso::
        https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-directory-file-acl-python

    :param adls_conn_id: Reference to the :ref:`adls connection <howto/connection:adls>`.
    :param public_read: Whether an anonymous public read access should be used. default is False
    """

    conn_name_attr = "adls_conn_id"
    default_conn_name = "adls_default"
    conn_type = "adls"
    hook_name = "Azure Date Lake Storage V2"

    @classmethod
    @add_managed_identity_connection_widgets
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import PasswordField, StringField

        return {
            "connection_string": PasswordField(
                lazy_gettext("ADLS Gen2 Connection String (optional)"), widget=BS3PasswordFieldWidget()
            ),
            "tenant_id": StringField(
                lazy_gettext("Tenant ID (Active Directory)"), widget=BS3TextFieldWidget()
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port"],
            "relabeling": {
                "login": "Client ID (Active Directory)",
                "password": "ADLS Gen2 Key / Client Secret (Active Directory)",
                "host": "ADLS Gen2 Account Name",
            },
            "placeholders": {
                "extra": "additional options for use with FileService and AzureFileVolume",
                "login": "client id",
                "password": "key / secret",
                "host": "storage account name",
                "connection_string": "connection string (overrides auth)",
                "tenant_id": "tenant id",
            },
        }

    def __init__(self, adls_conn_id: str, public_read: bool = False) -> None:
        super().__init__()
        self.conn_id = adls_conn_id
        self.public_read = public_read

    def _get_field(self, extra_dict, field_name):
        prefix = "extra__adls__"
        if field_name.startswith("extra__"):
            raise ValueError(
                f"Got prefixed name {field_name}; please remove the '{prefix}' prefix when using this method."
            )
        if field_name in extra_dict:
            return extra_dict[field_name] or None
        return extra_dict.get(f"{prefix}{field_name}") or None

    @cached_property
    def service_client(self) -> DataLakeServiceClient:
        """Return the DataLakeServiceClient object (cached)."""
        return self.get_conn()

    def get_conn(self) -> DataLakeServiceClient:
        """Return the DataLakeServiceClient object."""
        conn = self.get_connection(self.conn_id)
        extra = conn.extra_dejson or {}

        connection_string = self._get_field(extra, "connection_string")
        if connection_string:
            # connection_string auth takes priority
            return DataLakeServiceClient.from_connection_string(connection_string, **extra)

        credential: Credentials
        tenant = self._get_field(extra, "tenant_id")
        if tenant:
            # use Active Directory auth
            app_id = conn.login
            app_secret = conn.password
            proxies = extra.get("proxies", {})
            app_id = cast("str", app_id)
            app_secret = cast("str", app_secret)
            credential = ClientSecretCredential(
                tenant_id=tenant, client_id=app_id, client_secret=app_secret, proxies=proxies
            )
        elif conn.password:
            credential = conn.password  # type: ignore[assignment]
        else:
            managed_identity_client_id = self._get_field(extra, "managed_identity_client_id")
            workload_identity_tenant_id = self._get_field(extra, "workload_identity_tenant_id")
            credential = get_sync_default_azure_credential(
                managed_identity_client_id=managed_identity_client_id,
                workload_identity_tenant_id=workload_identity_tenant_id,
            )

        account_url = extra.pop("account_url", f"https://{conn.host}.dfs.core.windows.net")

        self.log.info("account_url: %s", account_url)

        return DataLakeServiceClient(
            account_url=account_url,
            credential=credential,  # type: ignore[arg-type]
            **extra,
        )

    def create_file_system(self, file_system_name: str) -> None:
        """
        Create a new file system under the specified account.

        A container acts as a file system for your files.

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

    def get_file_system(self, file_system: FileSystemProperties | str) -> FileSystemClient:
        """
        Get a client to interact with the specified file system.

        :param file_system: This can either be the name of the file system
            or an instance of FileSystemProperties.
        """
        try:
            file_system_client = self.service_client.get_file_system_client(file_system=file_system)
            return file_system_client
        except ResourceNotFoundError:
            self.log.info("file system %r doesn't exists.", file_system)
            raise
        except Exception as e:
            self.log.info("Error while attempting to get file system %r: %s", file_system, e)
            raise

    def create_directory(
        self, file_system_name: FileSystemProperties | str, directory_name: str, **kwargs
    ) -> DataLakeDirectoryClient:
        """
        Create a directory under the specified file system.

        :param file_system_name: Name of the file system or instance of FileSystemProperties.
        :param directory_name: Name of the directory which needs to be created in the file system.
        """
        result = self.get_file_system(file_system_name).create_directory(directory_name, **kwargs)
        return result

    def get_directory_client(
        self,
        file_system_name: FileSystemProperties | str,
        directory_name: DirectoryProperties | str,
    ) -> DataLakeDirectoryClient:
        """
        Get the specific directory under the specified file system.

        :param file_system_name: Name of the file system or instance of FileSystemProperties.
        :param directory_name: Name of the directory or instance of DirectoryProperties which needs to be
            retrieved from the file system.
        """
        try:
            directory_client = self.get_file_system(file_system_name).get_directory_client(directory_name)
            return directory_client
        except ResourceNotFoundError:
            self.log.info(
                "Directory %s doesn't exists in the file system %s", directory_name, file_system_name
            )
            raise
        except Exception as e:
            self.log.info(e)
            raise

    def create_file(self, file_system_name: FileSystemProperties | str, file_name: str) -> DataLakeFileClient:
        """
        Create a file under the file system.

        :param file_system_name: Name of the file system or instance of FileSystemProperties.
        :param file_name: Name of the file which needs to be created in the file system.
        """
        file_client = self.get_file_system(file_system_name).create_file(file_name)
        return file_client

    def upload_file(
        self,
        file_system_name: FileSystemProperties | str,
        file_name: str,
        file_path: str,
        overwrite: bool = False,
        **kwargs: Any,
    ) -> None:
        """
        Create a file with data in the file system.

        :param file_system_name: Name of the file system or instance of FileSystemProperties.
        :param file_name: Name of the file to be created with name.
        :param file_path: Path to the file to load.
        :param overwrite: Boolean flag to overwrite an existing file or not.
        """
        file_client = self.create_file(file_system_name, file_name)
        with open(file_path, "rb") as data:
            file_client.upload_data(data, overwrite=overwrite, **kwargs)

    def upload_file_to_directory(
        self,
        file_system_name: str,
        directory_name: str,
        file_name: str,
        file_path: str,
        overwrite: bool = False,
        **kwargs: Any,
    ) -> None:
        """
        Upload data to a file.

        :param file_system_name: Name of the file system or instance of FileSystemProperties.
        :param directory_name: Name of the directory.
        :param file_name: Name of the file to be created with name.
        :param file_path: Path to the file to load.
        :param overwrite: Boolean flag to overwrite an existing file or not.
        """
        directory_client = self.get_directory_client(file_system_name, directory_name=directory_name)
        file_client = directory_client.create_file(file_name, **kwargs)
        with open(file_path, "rb") as data:
            file_client.upload_data(data, overwrite=overwrite, **kwargs)

    def list_files_directory(
        self, file_system_name: FileSystemProperties | str, directory_name: str
    ) -> list[str]:
        """
        List files or directories under the specified file system.

        :param file_system_name: Name of the file system or instance of FileSystemProperties.
        :param directory_name: Name of the directory.
        """
        paths = self.get_file_system(file_system=file_system_name).get_paths(directory_name)
        directory_lists = []
        for path in paths:
            directory_lists.append(path.name)
        return directory_lists

    def list_file_system(
        self, prefix: str | None = None, include_metadata: bool = False, **kwargs: Any
    ) -> list[str]:
        """
        List file systems under the specified account.

        :param prefix:
            Filters the results to return only file systems whose names
            begin with the specified prefix.
        :param include_metadata: Specifies that file system metadata be returned in the response.
            The default value is `False`.
        """
        file_system = self.service_client.list_file_systems(
            name_starts_with=prefix, include_metadata=include_metadata
        )
        file_system_list = []
        for fs in file_system:
            file_system_list.append(fs.name)
        return file_system_list

    def delete_file_system(self, file_system_name: FileSystemProperties | str) -> None:
        """
        Delete the file system.

        :param file_system_name: Name of the file system or instance of FileSystemProperties.
        """
        try:
            self.service_client.delete_file_system(file_system_name)
            self.log.info("Deleted file system: %s", file_system_name)
        except ResourceNotFoundError:
            self.log.info("file system %r doesn't exists.", file_system_name)
        except Exception as e:
            self.log.info("Error while attempting to deleting file system %r: %s", file_system_name, e)
            raise

    def delete_directory(self, file_system_name: FileSystemProperties | str, directory_name: str) -> None:
        """
        Delete the specified directory in a file system.

        :param file_system_name: Name of the file system or instance of FileSystemProperties.
        :param directory_name: Name of the directory.
        """
        directory_client = self.get_directory_client(file_system_name, directory_name)
        directory_client.delete_directory()

    def test_connection(self):
        """Test ADLS Gen2 Storage connection."""
        try:
            # Attempts to list file systems in ADLS Gen2 Storage and retrieves the first
            # file_system from the returned iterator. The Azure DataLake Storage allows creation
            # of DataLakeServiceClient even if the credentials are incorrect but will fail properly
            # if we try to fetch the file_system. We need to _actually_ try to retrieve a
            # file_system to properly test the connection
            next(self.get_conn().list_file_systems(), None)
            return True, "Successfully connected to ADLS Gen2 Storage."
        except Exception as e:
            return False, str(e)

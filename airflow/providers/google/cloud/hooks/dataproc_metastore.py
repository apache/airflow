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
"""This module contains a Google Cloud Dataproc Metastore hook."""
from __future__ import annotations

from typing import Any, Sequence

from google.api_core.client_options import ClientOptions
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.operation import Operation
from google.api_core.retry import Retry
from google.cloud.metastore_v1 import DataprocMetastoreClient
from google.cloud.metastore_v1.types import Backup, MetadataImport, Service
from google.cloud.metastore_v1.types.metastore import DatabaseDumpSpec, Restore
from google.protobuf.field_mask_pb2 import FieldMask

from airflow.exceptions import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class DataprocMetastoreHook(GoogleBaseHook):
    """Hook for Google Cloud Dataproc Metastore APIs."""

    def get_dataproc_metastore_client(self) -> DataprocMetastoreClient:
        """Returns DataprocMetastoreClient."""
        client_options = ClientOptions(api_endpoint="metastore.googleapis.com:443")

        return DataprocMetastoreClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

    def wait_for_operation(self, timeout: float | None, operation: Operation):
        """Waits for long-lasting operation to complete."""
        try:
            return operation.result(timeout=timeout)
        except Exception:
            error = operation.exception(timeout=timeout)
            raise AirflowException(error)

    @GoogleBaseHook.fallback_to_default_project_id
    def create_backup(
        self,
        project_id: str,
        region: str,
        service_id: str,
        backup: dict[Any, Any] | Backup,
        backup_id: str,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Creates a new backup in a given project and location.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param service_id:  Required. The ID of the metastore service, which is used as the final component of
            the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
            with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
            hyphens.

            This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
            provided, this should not be set.
        :param backup:  Required. The backup to create. The ``name`` field is ignored. The ID of the created
            backup must be provided in the request's ``backup_id`` field.

            This corresponds to the ``backup`` field on the ``request`` instance; if ``request`` is provided,
            this should not be set.
        :param backup_id:  Required. The ID of the backup, which is used as the final component of the
            backup's name. This value must be between 1 and 64 characters long, begin with a letter, end with
            a letter or number, and consist of alphanumeric ASCII characters or hyphens.

            This corresponds to the ``backup_id`` field on the ``request`` instance; if ``request`` is
            provided, this should not be set.
        :param request_id: Optional. A unique id used to identify the request.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        parent = f"projects/{project_id}/locations/{region}/services/{service_id}"

        client = self.get_dataproc_metastore_client()
        result = client.create_backup(
            request={
                "parent": parent,
                "backup": backup,
                "backup_id": backup_id,
                "request_id": request_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def create_metadata_import(
        self,
        project_id: str,
        region: str,
        service_id: str,
        metadata_import: dict | MetadataImport,
        metadata_import_id: str,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Creates a new MetadataImport in a given project and location.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param service_id:  Required. The ID of the metastore service, which is used as the final component of
            the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
            with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
            hyphens.

            This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
            provided, this should not be set.
        :param metadata_import:  Required. The metadata import to create. The ``name`` field is ignored. The
            ID of the created metadata import must be provided in the request's ``metadata_import_id`` field.

            This corresponds to the ``metadata_import`` field on the ``request`` instance; if ``request`` is
            provided, this should not be set.
        :param metadata_import_id:  Required. The ID of the metadata import, which is used as the final
            component of the metadata import's name. This value must be between 1 and 64 characters long,
            begin with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
            hyphens.

            This corresponds to the ``metadata_import_id`` field on the ``request`` instance; if ``request``
            is provided, this should not be set.
        :param request_id: Optional. A unique id used to identify the request.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        parent = f"projects/{project_id}/locations/{region}/services/{service_id}"

        client = self.get_dataproc_metastore_client()
        result = client.create_metadata_import(
            request={
                "parent": parent,
                "metadata_import": metadata_import,
                "metadata_import_id": metadata_import_id,
                "request_id": request_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def create_service(
        self,
        region: str,
        project_id: str,
        service: dict | Service,
        service_id: str,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Creates a metastore service in a project and location.

        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param service:  Required. The Metastore service to create. The ``name`` field is ignored. The ID of
            the created metastore service must be provided in the request's ``service_id`` field.

            This corresponds to the ``service`` field on the ``request`` instance; if ``request`` is provided,
            this should not be set.
        :param service_id:  Required. The ID of the metastore service, which is used as the final component of
            the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
            with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
            hyphens.

            This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
            provided, this should not be set.
        :param request_id: Optional. A unique id used to identify the request.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        parent = f"projects/{project_id}/locations/{region}"

        client = self.get_dataproc_metastore_client()
        result = client.create_service(
            request={
                "parent": parent,
                "service_id": service_id,
                "service": service if service else {},
                "request_id": request_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_backup(
        self,
        project_id: str,
        region: str,
        service_id: str,
        backup_id: str,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Deletes a single backup.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param service_id:  Required. The ID of the metastore service, which is used as the final component of
            the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
            with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
            hyphens.

            This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
            provided, this should not be set.
        :param backup_id:  Required. The ID of the backup, which is used as the final component of the
            backup's name. This value must be between 1 and 64 characters long, begin with a letter, end with
            a letter or number, and consist of alphanumeric ASCII characters or hyphens.

            This corresponds to the ``backup_id`` field on the ``request`` instance; if ``request`` is
            provided, this should not be set.
        :param request_id: Optional. A unique id used to identify the request.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        name = f"projects/{project_id}/locations/{region}/services/{service_id}/backups/{backup_id}"

        client = self.get_dataproc_metastore_client()
        result = client.delete_backup(
            request={
                "name": name,
                "request_id": request_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_service(
        self,
        project_id: str,
        region: str,
        service_id: str,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Deletes a single service.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param service_id:  Required. The ID of the metastore service, which is used as the final component of
            the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
            with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
            hyphens.

            This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
            provided, this should not be set.
        :param request_id: Optional. A unique id used to identify the request.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        name = f"projects/{project_id}/locations/{region}/services/{service_id}"

        client = self.get_dataproc_metastore_client()
        result = client.delete_service(
            request={
                "name": name,
                "request_id": request_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def export_metadata(
        self,
        destination_gcs_folder: str,
        project_id: str,
        region: str,
        service_id: str,
        request_id: str | None = None,
        database_dump_type: DatabaseDumpSpec | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Exports metadata from a service.

        :param destination_gcs_folder: A Cloud Storage URI of a folder, in the format
            ``gs://<bucket_name>/<path_inside_bucket>``. A sub-folder
            ``<export_folder>`` containing exported files will be
            created below it.
        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param service_id:  Required. The ID of the metastore service, which is used as the final component of
            the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
            with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
            hyphens.

            This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
            provided, this should not be set.
        :param request_id: Optional. A unique id used to identify the request.
        :param database_dump_type: Optional. The type of the database dump. If unspecified,
            defaults to ``MYSQL``.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        service = f"projects/{project_id}/locations/{region}/services/{service_id}"

        client = self.get_dataproc_metastore_client()
        result = client.export_metadata(
            request={
                "destination_gcs_folder": destination_gcs_folder,
                "service": service,
                "request_id": request_id,
                "database_dump_type": database_dump_type,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_service(
        self,
        project_id: str,
        region: str,
        service_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Gets the details of a single service.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param service_id:  Required. The ID of the metastore service, which is used as the final component of
            the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
            with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
            hyphens.

            This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
            provided, this should not be set.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        name = f"projects/{project_id}/locations/{region}/services/{service_id}"

        client = self.get_dataproc_metastore_client()
        result = client.get_service(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_backup(
        self,
        project_id: str,
        region: str,
        service_id: str,
        backup_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Backup:
        """
        Get backup from a service.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param service_id:  Required. The ID of the metastore service, which is used as the final component of
            the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
            with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
            hyphens.

            This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
            provided, this should not be set.
        :param backup_id:  Required. The ID of the metastore service backup to restore from
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        backup = f"projects/{project_id}/locations/{region}/services/{service_id}/backups/{backup_id}"
        client = self.get_dataproc_metastore_client()
        result = client.get_backup(
            request={
                "name": backup,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_backups(
        self,
        project_id: str,
        region: str,
        service_id: str,
        page_size: int | None = None,
        page_token: str | None = None,
        filter: str | None = None,
        order_by: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Lists backups in a service.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param service_id:  Required. The ID of the metastore service, which is used as the final component of
            the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
            with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
            hyphens.

            This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
            provided, this should not be set.
        :param page_size: Optional. The maximum number of backups to
            return. The response may contain less than the
            maximum number. If unspecified, no more than 500
            backups are returned. The maximum value is 1000;
            values above 1000 are changed to 1000.
        :param page_token: Optional. A page token, received from a previous
            [DataprocMetastore.ListBackups][google.cloud.metastore.v1.DataprocMetastore.ListBackups]
            call. Provide this token to retrieve the subsequent page.
            To retrieve the first page, supply an empty page token.
            When paginating, other parameters provided to
            [DataprocMetastore.ListBackups][google.cloud.metastore.v1.DataprocMetastore.ListBackups]
            must match the call that provided the page token.
        :param filter: Optional. The filter to apply to list
            results.
        :param order_by: Optional. Specify the ordering of results as described in
            `Sorting
            Order <https://cloud.google.com/apis/design/design_patterns#sorting_order>`__.
            If not specified, the results will be sorted in the default
            order.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        parent = f"projects/{project_id}/locations/{region}/services/{service_id}/backups"

        client = self.get_dataproc_metastore_client()
        result = client.list_backups(
            request={
                "parent": parent,
                "page_size": page_size,
                "page_token": page_token,
                "filter": filter,
                "order_by": order_by,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def restore_service(
        self,
        project_id: str,
        region: str,
        service_id: str,
        backup_project_id: str,
        backup_region: str,
        backup_service_id: str,
        backup_id: str,
        restore_type: Restore | None = None,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Restores a service from a backup.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param service_id:  Required. The ID of the metastore service, which is used as the final component of
            the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
            with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
            hyphens.

            This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
            provided, this should not be set.
        :param backup_project_id: Required. The ID of the Google Cloud project that the metastore service
            backup to restore from.
        :param backup_region: Required. The ID of the Google Cloud region that the metastore
            service backup to restore from.
        :param backup_service_id:  Required. The ID of the metastore service backup to restore from,
            which is used as the final component of the metastore service's name. This value must be
            between 2 and 63 characters long inclusive, begin with a letter, end with a letter or number,
            and consist of alphanumeric ASCII characters or hyphens.
        :param backup_id:  Required. The ID of the metastore service backup to restore from
        :param restore_type: Optional. The type of restore. If unspecified, defaults to
            ``METADATA_ONLY``
        :param request_id: Optional. A unique id used to identify the request.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        service = f"projects/{project_id}/locations/{region}/services/{service_id}"
        backup = (
            f"projects/{backup_project_id}/locations/{backup_region}/services/"
            f"{backup_service_id}/backups/{backup_id}"
        )

        client = self.get_dataproc_metastore_client()
        result = client.restore_service(
            request={
                "service": service,
                "backup": backup,
                "restore_type": restore_type,
                "request_id": request_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def update_service(
        self,
        project_id: str,
        region: str,
        service_id: str,
        service: dict | Service,
        update_mask: FieldMask,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Updates the parameters of a single service.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param service_id:  Required. The ID of the metastore service, which is used as the final component of
            the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
            with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
            hyphens.

            This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
            provided, this should not be set.
        :param service:  Required. The metastore service to update. The server only merges fields in the
            service if they are specified in ``update_mask``.

            The metastore service's ``name`` field is used to identify the metastore service to be updated.

            This corresponds to the ``service`` field on the ``request`` instance; if ``request`` is provided,
            this should not be set.
        :param update_mask:  Required. A field mask used to specify the fields to be overwritten in the
            metastore service resource by the update. Fields specified in the ``update_mask`` are relative to
            the resource (not to the full request). A field is overwritten if it is in the mask.

            This corresponds to the ``update_mask`` field on the ``request`` instance; if ``request`` is
            provided, this should not be set.
        :param request_id: Optional. A unique id used to identify the request.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataproc_metastore_client()

        service_name = f"projects/{project_id}/locations/{region}/services/{service_id}"

        service["name"] = service_name

        result = client.update_service(
            request={
                "service": service,
                "update_mask": update_mask,
                "request_id": request_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

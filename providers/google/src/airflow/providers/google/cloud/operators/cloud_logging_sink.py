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

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import google.cloud.exceptions
from google.api_core.exceptions import AlreadyExists
from google.cloud.logging_v2.types import LogSink

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.cloud_logging import CloudLoggingHook
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

if TYPE_CHECKING:
    from google.protobuf.field_mask_pb2 import FieldMask

    from airflow.providers.common.compat.sdk import Context


def _validate_inputs(obj, required_fields: list[str]) -> None:
    """Validate that all required fields are present on self."""
    missing = [field for field in required_fields if not getattr(obj, field, None)]
    if missing:
        raise AirflowException(
            f"Required parameters are missing: {missing}. These must be passed as keyword parameters."
        )


def _get_field(obj, field_name):
    """Supports both dict and protobuf-like objects."""
    if isinstance(obj, dict):
        return obj.get(field_name)
    return getattr(obj, field_name, None)


class CloudLoggingCreateSinkOperator(GoogleCloudBaseOperator):
    """
    Creates a Cloud Logging export sink in a GCP project.

    This operator creates a sink that exports log entries from Cloud Logging
    to destinations like Cloud Storage, BigQuery, or Pub/Sub.

    :param project_id: Required. ID of the Google Cloud project where the sink will be created.
    :param sink_config: Required. The full sink configuration as a dictionary or a LogSink object.
        See: https://cloud.google.com/logging/docs/reference/v2/rest/v2/projects.sinks
    :param unique_writer_identity: If True, creates a unique service account for the sink.
        If False, uses the default Google-managed service account.
    :param gcp_conn_id: Optional. The connection ID used to connect to Google Cloud. Defaults to "google_cloud_default".
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "project_id",
        "sink_config",
        "gcp_conn_id",
        "impersonation_chain",
        "unique_writer_identity",
    )

    def __init__(
        self,
        project_id: str,
        sink_config: dict | LogSink,
        unique_writer_identity: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.sink_config = sink_config
        self.unique_writer_identity = unique_writer_identity
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict[str, Any]:
        """Execute the operator."""
        _validate_inputs(self, required_fields=["project_id", "sink_config"])
        hook = CloudLoggingHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)

        try:
            self.log.info(
                "Creating log sink '%s' in project '%s'",
                _get_field(self.sink_config, "name"),
                self.project_id,
            )
            self.log.info("Destination: %s", _get_field(self.sink_config, "destination"))

            response = hook.create_sink(
                sink=self.sink_config,
                unique_writer_identity=self.unique_writer_identity,
                project_id=self.project_id,
            )

            self.log.info("Log sink created successfully: %s", response.name)

            if self.unique_writer_identity and hasattr(response, "writer_identity"):
                self.log.info("Writer identity: %s", response.writer_identity)
                self.log.info("Remember to grant appropriate permissions to the writer identity")

            return LogSink.to_dict(response)

        except AlreadyExists:
            self.log.info(
                "Already existed log sink, sink_name=%s, project_id=%s",
                _get_field(self.sink_config, "name"),
                self.project_id,
            )
            existing_sink = hook.get_sink(
                sink_name=_get_field(self.sink_config, "name"), project_id=self.project_id
            )
            return LogSink.to_dict(existing_sink)

        except google.cloud.exceptions.GoogleCloudError as e:
            self.log.error("An error occurred. Exiting.")
            raise e


class CloudLoggingDeleteSinkOperator(GoogleCloudBaseOperator):
    """
    Deletes a Cloud Logging export sink from a GCP project.

    :param sink_name: Required. Name of the sink to delete.
    :param project_id: Required. The ID of the Google Cloud project.
    :param gcp_conn_id: Optional. The connection ID to use for connecting to Google Cloud.
        Defaults to "google_cloud_default".
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = ("sink_name", "project_id", "gcp_conn_id", "impersonation_chain")

    def __init__(
        self,
        sink_name: str,
        project_id: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sink_name = sink_name
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        """Execute the operator."""
        _validate_inputs(self, ["sink_name", "project_id"])
        hook = CloudLoggingHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)

        try:
            self.log.info("Deleting log sink '%s' from project '%s'", self.sink_name, self.project_id)
            hook.delete_sink(sink_name=self.sink_name, project_id=self.project_id)
            self.log.info("Log sink '%s' deleted successfully", self.sink_name)

        except google.cloud.exceptions.NotFound as e:
            self.log.error("An error occurred. Not Found.")
            raise e
        except google.cloud.exceptions.GoogleCloudError as e:
            self.log.error("An error occurred. Exiting.")
            raise e


class CloudLoggingUpdateSinkOperator(GoogleCloudBaseOperator):
    """
    Updates an existing Cloud Logging export sink.

    :param project_id: Required. The ID of the Google Cloud project that contains the sink.
    :param sink_name: Required. The name of the sink to update.
    :param sink_config: Required. The updated sink configuration. Can be a dictionary or a
        `google.cloud.logging_v2.types.LogSink` object. Refer to:
        https://cloud.google.com/logging/docs/reference/v2/rest/v2/projects.sinks
    :param update_mask: Required. A FieldMask or dictionary specifying which fields of the sink
        should be updated. For example, to update the destination and filter, use:
        `{"paths": ["destination", "filter"]}`.
    :param unique_writer_identity: Optional. When set to True, a new unique service account
        will be created for the sink. Defaults to False.
    :param gcp_conn_id: Optional. The connection ID used to connect to Google Cloud.
        Defaults to "google_cloud_default".
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "sink_name",
        "project_id",
        "update_mask",
        "sink_config",
        "unique_writer_identity",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        project_id: str,
        sink_name: str,
        sink_config: dict | LogSink,
        update_mask: FieldMask | dict,
        unique_writer_identity: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.sink_name = sink_name
        self.sink_config = sink_config
        self.update_mask = update_mask
        self.unique_writer_identity = unique_writer_identity
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict[str, Any]:
        """Execute the operator."""
        _validate_inputs(self, ["sink_name", "project_id", "sink_config", "update_mask"])
        hook = CloudLoggingHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)

        try:
            current_sink = hook.get_sink(sink_name=self.sink_name, project_id=self.project_id)
            self.log.info("Current log sink configuration: '%s'.", LogSink.to_dict(current_sink))

            self.log.info("Updating log sink '%s' in project '%s'", self.sink_name, self.project_id)
            if isinstance(self.update_mask, dict) and "paths" in self.update_mask:
                paths = self.update_mask["paths"]
            elif hasattr(self.update_mask, "paths"):
                paths = self.update_mask.paths

            self.log.info("Updating fields: %s", ", ".join(paths))

            response = hook.update_sink(
                sink_name=self.sink_name,
                sink=self.sink_config,
                unique_writer_identity=self.unique_writer_identity,
                project_id=self.project_id,
                update_mask=self.update_mask,
            )
            self.log.info("Log sink updated successfully: %s", response.name)
            return LogSink.to_dict(response)

        except google.cloud.exceptions.NotFound as e:
            self.log.error("An error occurred. Not Found.")
            raise e
        except google.cloud.exceptions.GoogleCloudError as e:
            self.log.error("An error occurred. Exiting.")
            raise e


class CloudLoggingListSinksOperator(GoogleCloudBaseOperator):
    """
    Lists Cloud Logging export sinks in a Google Cloud project.

    :param project_id: Required. The ID of the Google Cloud project to list sinks from.
    :param page_size: Optional. The maximum number of sinks to return per page. Must be greater than 0.
        If None, the server will use a default value.
    :param gcp_conn_id: Optional. The connection ID used to connect to Google Cloud.
        Defaults to "google_cloud_default".
    :param impersonation_chain: Optional. Service account or chained list of accounts to impersonate.
        If a string, the service account must grant the originating account the
        'Service Account Token Creator' IAM role.

        If a sequence, each account in the chain must grant this role to the next.
        The first account must grant it to the originating account (templated).
    """

    template_fields: Sequence[str] = ("project_id", "gcp_conn_id", "impersonation_chain", "page_size")

    def __init__(
        self,
        project_id: str,
        page_size: int | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.page_size = page_size
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> list[dict[str, Any]]:
        """Execute the operator."""
        _validate_inputs(self, ["project_id"])

        if self.page_size is not None and self.page_size < 1:
            raise AirflowException("The page_size for the list sinks request must be greater than zero")

        hook = CloudLoggingHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)

        try:
            self.log.info("Listing log sinks in project '%s'", self.project_id)

            sinks = hook.list_sinks(project_id=self.project_id, page_size=self.page_size)

            result = [LogSink.to_dict(sink) for sink in sinks]
            self.log.info("Found %d log sinks", len(result))

            return result

        except google.cloud.exceptions.GoogleCloudError as e:
            self.log.error("An error occurred. Exiting.")
            raise e

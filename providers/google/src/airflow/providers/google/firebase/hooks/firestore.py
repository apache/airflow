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
"""Hook for Google Cloud Firestore service."""

from __future__ import annotations

import time
from collections.abc import Sequence
from typing import Any

from asgiref.sync import sync_to_async
from googleapiclient.discovery import build, build_from_document
from googleapiclient.errors import HttpError

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.common.hooks.base_google import (
    PROVIDE_PROJECT_ID,
    GoogleBaseAsyncHook,
    GoogleBaseHook,
)

# Time to sleep between active checks of the operation results
TIME_TO_SLEEP_IN_SECONDS = 5


class CloudFirestoreHook(GoogleBaseHook):
    """
    Hook for the Google Firestore APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.

    :param api_version: API version used (for example v1 or v1beta1).
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    """

    _conn: build | None = None

    def __init__(
        self,
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
        )
        self.api_version = api_version

    def get_conn(self):
        """
        Retrieve the connection to Cloud Firestore.

        :return: Google Cloud Firestore services object.
        """
        if not self._conn:
            http_authorized = self._authorize()
            # We cannot use an Authorized Client to retrieve discovery document due to an error in the API.
            # When the authorized customer will send a request to the address below
            # https://www.googleapis.com/discovery/v1/apis/firestore/v1/rest
            # then it will get the message below:
            # > Request contains an invalid argument.
            # At the same time, the Non-Authorized Client has no problems.
            non_authorized_conn = build(
                "firestore", self.api_version, cache_discovery=False, client_options=self.get_client_options()
            )
            self._conn = build_from_document(non_authorized_conn._rootDesc, http=http_authorized)
        return self._conn

    def get_operation(self, operation_name: str) -> dict[str, Any]:
        """
        Retrieve the current state of a long-running Firestore operation.

        :param operation_name: The resource name of the operation.
        """
        service = self.get_conn()
        return (
            service.projects()
            .databases()
            .operations()
            .get(name=operation_name)
            .execute(num_retries=self.num_retries)
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def list_operations(
        self, database_id: str = "(default)", project_id: str = PROVIDE_PROJECT_ID
    ) -> list[dict[str, Any]]:
        """
        List long-running operations on the specified Firestore database.

        :param database_id: The Database ID.
        :param project_id: Optional, Google Cloud Project project_id where the database belongs.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        """
        service = self.get_conn()
        name = f"projects/{project_id}/databases/{database_id}"
        operations_resource = service.projects().databases().operations()
        all_operations: list[dict[str, Any]] = []
        page_token: str | None = None

        while True:
            list_kwargs: dict[str, Any] = {"name": name}
            if page_token:
                list_kwargs["pageToken"] = page_token
            response = operations_resource.list(**list_kwargs).execute(num_retries=self.num_retries)
            if not isinstance(response, dict):
                break
            operations = response.get("operations", [])
            if isinstance(operations, list):
                all_operations.extend(operations)
            next_page_token = response.get("nextPageToken")
            if not isinstance(next_page_token, str) or not next_page_token:
                break
            page_token = next_page_token

        return all_operations

    @GoogleBaseHook.fallback_to_default_project_id
    def find_matching_export_operation(
        self,
        body: dict[str, Any],
        database_id: str = "(default)",
        project_id: str = PROVIDE_PROJECT_ID,
        include_completed: bool = True,
    ) -> dict[str, Any] | None:
        """
        Find an existing running or completed export operation matching the export request body.

        :param body: The export request body.
        :param database_id: The Database ID.
        :param project_id: Optional, Google Cloud Project project_id where the database belongs.
        :param include_completed: Whether to match already completed operations in addition to
            in-progress operations.
        """
        target_uri = body.get("outputUriPrefix")
        if not target_uri or not isinstance(target_uri, str):
            return None
        target_collections = sorted(body.get("collectionIds") or [])
        target_namespaces = sorted(body.get("namespaceIds") or [])

        completed_match: dict[str, Any] | None = None
        for op in self.list_operations(database_id=database_id, project_id=project_id):
            if not isinstance(op, dict):
                continue
            metadata = op.get("metadata")
            if not isinstance(metadata, dict):
                metadata = {}

            meta_type = metadata.get("@type", "")
            if meta_type and not meta_type.endswith("ExportDocumentsMetadata"):
                continue

            op_state = metadata.get("operationState", "")
            if op.get("error") or op_state in {"FAILED", "CANCELLED", "CANCELLING"}:
                continue

            op_collections = sorted(metadata.get("collectionIds") or [])
            if target_collections != op_collections:
                continue

            op_namespaces = sorted(metadata.get("namespaceIds") or [])
            if target_namespaces != op_namespaces:
                continue

            response_dict = op.get("response")
            op_uri = metadata.get("outputUriPrefix") or (
                response_dict.get("outputUriPrefix") if isinstance(response_dict, dict) else None
            )
            if not isinstance(op_uri, str) or op_uri.rstrip("/") != target_uri.rstrip("/"):
                continue

            is_done = bool(op.get("done"))
            if not is_done:
                return op
            if include_completed and completed_match is None:
                completed_match = op

        return completed_match

    @GoogleBaseHook.fallback_to_default_project_id
    def start_export_documents(
        self,
        body: dict[str, Any],
        database_id: str = "(default)",
        project_id: str = PROVIDE_PROJECT_ID,
        reattach_on_restart: bool = True,
    ) -> dict[str, Any]:
        """
        Start a Firestore export or return an existing running/completed operation when reattaching.

        :param body: The request body.
        :param database_id: The Database ID.
        :param project_id: Optional, Google Cloud Project project_id where the database belongs.
        :param reattach_on_restart: If True, check for an existing in-progress export operation
            matching ``body`` before submitting a new export request, and recover if the export
            already completed on a prior attempt.
        """
        if reattach_on_restart:
            existing_op = self.find_matching_export_operation(
                body=body,
                database_id=database_id,
                project_id=project_id,
                include_completed=False,
            )
            if existing_op is not None:
                self.log.info(
                    "Reattaching to in-progress Firestore export operation %s.",
                    existing_op.get("name"),
                )
                return existing_op

        service = self.get_conn()
        name = f"projects/{project_id}/databases/{database_id}"
        try:
            return (
                service.projects()
                .databases()
                .exportDocuments(name=name, body=body)
                .execute(num_retries=self.num_retries)
            )
        except HttpError as err:
            if reattach_on_restart and err.resp.status in {400, 409}:
                existing_op = self.find_matching_export_operation(
                    body=body,
                    database_id=database_id,
                    project_id=project_id,
                    include_completed=True,
                )
                if existing_op is not None:
                    if existing_op.get("done"):
                        self.log.info(
                            "Found already completed Firestore export operation %s; skipping new export.",
                            existing_op.get("name"),
                        )
                    else:
                        self.log.info(
                            "Export request returned HTTP %s, found existing operation %s.",
                            err.resp.status,
                            existing_op.get("name"),
                        )
                    return existing_op
            raise

    @GoogleBaseHook.fallback_to_default_project_id
    def export_documents(
        self,
        body: dict,
        database_id: str = "(default)",
        project_id: str = PROVIDE_PROJECT_ID,
        reattach_on_restart: bool = True,
    ) -> None:
        """
        Start a export with the specified configuration.

        :param database_id: The Database ID.
        :param body: The request body.
            See:
            https://firebase.google.com/docs/firestore/reference/rest/v1beta1/projects.databases/exportDocuments
        :param project_id: Optional, Google Cloud Project project_id where the database belongs.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param reattach_on_restart: If True, check for an existing running or completed export
            operation matching ``body`` before submitting a new export request.
        """
        operation = self.start_export_documents(
            body=body,
            database_id=database_id,
            project_id=project_id,
            reattach_on_restart=reattach_on_restart,
        )
        if operation.get("done"):
            if error := operation.get("error"):
                raise RuntimeError(str(error))
            return

        self._wait_for_operation_to_complete(operation["name"])

    def _wait_for_operation_to_complete(self, operation_name: str) -> Any:
        """
        Wait for the named operation to complete - checks status of the asynchronous call.

        :param operation_name: The name of the operation.
        :return: The response returned by the operation.
        :exception: AirflowException in case error is returned.
        """
        while True:
            operation_response = self.get_operation(operation_name)
            if operation_response.get("done"):
                response = operation_response.get("response")
                error = operation_response.get("error")
                # Note, according to documentation always either response or error is
                # set when "done" == True
                if error:
                    raise AirflowException(str(error))
                return response
            time.sleep(TIME_TO_SLEEP_IN_SECONDS)


class CloudFirestoreAsyncHook(GoogleBaseAsyncHook):
    """
    Asynchronous hook for the Google Firestore APIs.

    :param api_version: API version used (for example v1 or v1beta1).
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
    """

    sync_hook_class = CloudFirestoreHook

    def __init__(
        self,
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        super().__init__(
            api_version=api_version,
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
        )
        self.api_version = api_version

    async def get_operation(self, operation_name: str) -> dict[str, Any]:
        """
        Retrieve the current state of a long-running Firestore operation asynchronously.

        :param operation_name: The resource name of the operation.
        """
        sync_hook = await self.get_sync_hook()
        return await sync_to_async(sync_hook.get_operation)(operation_name=operation_name)

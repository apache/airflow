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
"""This module contains Google Datastore hook."""
from __future__ import annotations

import time
import warnings
from typing import Any, Sequence

from googleapiclient.discovery import Resource, build

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class DatastoreHook(GoogleBaseHook):
    """
    Interact with Google Cloud Datastore. This hook uses the Google Cloud connection.

    This object is not threads safe. If you want to make multiple requests
    simultaneously, you will need to create a hook per thread.

    :param api_version: The version of the API it is going to connect to.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        api_version: str = "v1",
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self.connection = None
        self.api_version = api_version

    def get_conn(self) -> Resource:
        """
        Establishes a connection to the Google API.

        :return: a Google Cloud Datastore service object.
        """
        if not self.connection:
            http_authorized = self._authorize()
            self.connection = build(
                "datastore", self.api_version, http=http_authorized, cache_discovery=False
            )

        return self.connection

    @GoogleBaseHook.fallback_to_default_project_id
    def allocate_ids(self, partial_keys: list, project_id: str) -> list:
        """
        Allocate IDs for incomplete keys.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/rest/v1/projects/allocateIds

        :param partial_keys: a list of partial keys.
        :param project_id: Google Cloud project ID against which to make the request.
        :return: a list of full keys.
        """
        conn = self.get_conn()

        resp = (
            conn.projects()
            .allocateIds(projectId=project_id, body={"keys": partial_keys})
            .execute(num_retries=self.num_retries)
        )

        return resp["keys"]

    @GoogleBaseHook.fallback_to_default_project_id
    def begin_transaction(self, project_id: str, transaction_options: dict[str, Any]) -> str:
        """
        Begins a new transaction.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/rest/v1/projects/beginTransaction

        :param project_id: Google Cloud project ID against which to make the request.
        :param transaction_options: Options for a new transaction.
        :return: a transaction handle.
        """
        conn = self.get_conn()

        resp = (
            conn.projects()
            .beginTransaction(projectId=project_id, body={"transactionOptions": transaction_options})
            .execute(num_retries=self.num_retries)
        )

        return resp["transaction"]

    @GoogleBaseHook.fallback_to_default_project_id
    def commit(self, body: dict, project_id: str) -> dict:
        """
        Commit a transaction, optionally creating, deleting or modifying some entities.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/rest/v1/projects/commit

        :param body: the body of the commit request.
        :param project_id: Google Cloud project ID against which to make the request.
        :return: the response body of the commit request.
        """
        conn = self.get_conn()

        resp = conn.projects().commit(projectId=project_id, body=body).execute(num_retries=self.num_retries)

        return resp

    @GoogleBaseHook.fallback_to_default_project_id
    def lookup(
        self,
        keys: list,
        project_id: str,
        read_consistency: str | None = None,
        transaction: str | None = None,
    ) -> dict:
        """
        Lookup some entities by key.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/rest/v1/projects/lookup

        :param keys: the keys to lookup.
        :param read_consistency: the read consistency to use. default, strong or eventual.
                                 Cannot be used with a transaction.
        :param transaction: the transaction to use, if any.
        :param project_id: Google Cloud project ID against which to make the request.
        :return: the response body of the lookup request.
        """
        conn = self.get_conn()

        body: dict[str, Any] = {"keys": keys}
        if read_consistency:
            body["readConsistency"] = read_consistency
        if transaction:
            body["transaction"] = transaction
        resp = conn.projects().lookup(projectId=project_id, body=body).execute(num_retries=self.num_retries)

        return resp

    @GoogleBaseHook.fallback_to_default_project_id
    def rollback(self, transaction: str, project_id: str) -> None:
        """
        Roll back a transaction.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/rest/v1/projects/rollback

        :param transaction: the transaction to roll back.
        :param project_id: Google Cloud project ID against which to make the request.
        """
        conn: Any = self.get_conn()

        conn.projects().rollback(projectId=project_id, body={"transaction": transaction}).execute(
            num_retries=self.num_retries
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def run_query(self, body: dict, project_id: str) -> dict:
        """
        Run a query for entities.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/rest/v1/projects/runQuery

        :param body: the body of the query request.
        :param project_id: Google Cloud project ID against which to make the request.
        :return: the batch of query results.
        """
        conn = self.get_conn()

        resp = conn.projects().runQuery(projectId=project_id, body=body).execute(num_retries=self.num_retries)

        return resp["batch"]

    def get_operation(self, name: str) -> dict:
        """
        Gets the latest state of a long-running operation.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects.operations/get

        :param name: the name of the operation resource.
        :return: a resource operation instance.
        """
        conn: Any = self.get_conn()

        resp = conn.projects().operations().get(name=name).execute(num_retries=self.num_retries)

        return resp

    def delete_operation(self, name: str) -> dict:
        """
        Deletes the long-running operation.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects.operations/delete

        :param name: the name of the operation resource.
        :return: none if successful.
        """
        conn = self.get_conn()

        resp = conn.projects().operations().delete(name=name).execute(num_retries=self.num_retries)

        return resp

    def poll_operation_until_done(self, name: str, polling_interval_in_seconds: float) -> dict:
        """
        Poll backup operation state until it's completed.

        :param name: the name of the operation resource
        :param polling_interval_in_seconds: The number of seconds to wait before calling another request.
        :return: a resource operation instance.
        """
        while True:
            result: dict = self.get_operation(name)

            state: str = result["metadata"]["common"]["state"]
            if state == "PROCESSING":
                self.log.info(
                    "Operation is processing. Re-polling state in %s seconds", polling_interval_in_seconds
                )
                time.sleep(polling_interval_in_seconds)
            else:
                return result

    @GoogleBaseHook.fallback_to_default_project_id
    def export_to_storage_bucket(
        self,
        bucket: str,
        project_id: str,
        namespace: str | None = None,
        entity_filter: dict | None = None,
        labels: dict[str, str] | None = None,
    ) -> dict:
        """
        Export entities from Cloud Datastore to Cloud Storage for backup.

        .. note::
            Keep in mind that this requests the Admin API not the Data API.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/admin/rest/v1/projects/export

        :param bucket: The name of the Cloud Storage bucket.
        :param namespace: The Cloud Storage namespace path.
        :param entity_filter: Description of what data from the project is included in the export.
        :param labels: Client-assigned labels.
        :param project_id: Google Cloud project ID against which to make the request.
        :return: a resource operation instance.
        """
        admin_conn = self.get_conn()

        output_url_prefix = f"gs://{'/'.join(filter(None, [bucket, namespace]))}"
        if not entity_filter:
            entity_filter = {}
        if not labels:
            labels = {}
        body = {
            "outputUrlPrefix": output_url_prefix,
            "entityFilter": entity_filter,
            "labels": labels,
        }
        resp = (
            admin_conn.projects()
            .export(projectId=project_id, body=body)
            .execute(num_retries=self.num_retries)
        )

        return resp

    @GoogleBaseHook.fallback_to_default_project_id
    def import_from_storage_bucket(
        self,
        bucket: str,
        file: str,
        project_id: str,
        namespace: str | None = None,
        entity_filter: dict | None = None,
        labels: dict | str | None = None,
    ) -> dict:
        """
        Import a backup from Cloud Storage to Cloud Datastore.

        .. note::
            Keep in mind that this requests the Admin API not the Data API.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/admin/rest/v1/projects/import

        :param bucket: The name of the Cloud Storage bucket.
        :param file: the metadata file written by the projects.export operation.
        :param namespace: The Cloud Storage namespace path.
        :param entity_filter: specify which kinds/namespaces are to be imported.
        :param labels: Client-assigned labels.
        :param project_id: Google Cloud project ID against which to make the request.
        :return: a resource operation instance.
        """
        admin_conn = self.get_conn()

        input_url = f"gs://{'/'.join(filter(None, [bucket, namespace, file]))}"
        if not entity_filter:
            entity_filter = {}
        if not labels:
            labels = {}
        body = {
            "inputUrl": input_url,
            "entityFilter": entity_filter,
            "labels": labels,
        }
        resp = (
            admin_conn.projects()
            .import_(projectId=project_id, body=body)
            .execute(num_retries=self.num_retries)
        )

        return resp

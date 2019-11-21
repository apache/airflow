# -*- coding: utf-8 -*-
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
#
"""
This module contains Google Datastore hook.
"""

import time
import warnings
from typing import Any, Dict, List, Optional, Union

from googleapiclient.discovery import build

from airflow.gcp.hooks.base import GoogleCloudBaseHook


class DatastoreHook(GoogleCloudBaseHook):
    """
    Interact with Google Cloud Datastore. This hook uses the Google Cloud Platform connection.

    This object is not threads safe. If you want to make multiple requests
    simultaneously, you will need to create a hook per thread.

    :param api_version: The version of the API it is going to connect to.
    :type api_version: str
    """

    def __init__(
        self,
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        api_version: str = 'v1',
        datastore_conn_id: Optional[str] = None
    ) -> None:
        if datastore_conn_id:
            warnings.warn(
                "The datastore_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.", DeprecationWarning, stacklevel=2)
            gcp_conn_id = datastore_conn_id
        super().__init__(gcp_conn_id=gcp_conn_id, delegate_to=delegate_to)
        self.connection = None
        self.api_version = api_version

    def get_conn(self) -> Any:
        """
        Establishes a connection to the Google API.

        :return: a Google Cloud Datastore service object.
        :rtype: Resource
        """
        if not self.connection:
            http_authorized = self._authorize()
            self.connection = build('datastore', self.api_version, http=http_authorized,
                                    cache_discovery=False)

        return self.connection

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def allocate_ids(self, partial_keys: List, project_id: Optional[str] = None) -> List:
        """
        Allocate IDs for incomplete keys.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/rest/v1/projects/allocateIds

        :param partial_keys: a list of partial keys.
        :type partial_keys: list
        :param project_id: Google Cloud Platform project ID against which to make the request.
        :type project_id: str
        :return: a list of full keys.
        :rtype: list
        """
        conn = self.get_conn()  # type: Any

        resp = (conn  # pylint:disable=no-member
                .projects()
                .allocateIds(projectId=project_id, body={'keys': partial_keys})
                .execute(num_retries=self.num_retries))

        return resp['keys']

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def begin_transaction(self, project_id: Optional[str] = None) -> str:
        """
        Begins a new transaction.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/rest/v1/projects/beginTransaction

        :param project_id: Google Cloud Platform project ID against which to make the request.
        :type project_id: str
        :return: a transaction handle.
        :rtype: str
        """
        conn = self.get_conn()  # type: Any

        resp = (conn  # pylint:disable=no-member
                .projects()
                .beginTransaction(projectId=project_id, body={})
                .execute(num_retries=self.num_retries))

        return resp['transaction']

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def commit(self, body: Dict, project_id: Optional[str] = None) -> Dict:
        """
        Commit a transaction, optionally creating, deleting or modifying some entities.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/rest/v1/projects/commit

        :param body: the body of the commit request.
        :type body: dict
        :param project_id: Google Cloud Platform project ID against which to make the request.
        :type project_id: str
        :return: the response body of the commit request.
        :rtype: dict
        """
        conn = self.get_conn()  # type: Any

        resp = (conn  # pylint:disable=no-member
                .projects()
                .commit(projectId=project_id, body=body)
                .execute(num_retries=self.num_retries))

        return resp

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def lookup(self,
               keys: List,
               read_consistency: Optional[str] = None,
               transaction: Optional[str] = None,
               project_id: Optional[str] = None) -> Dict:
        """
        Lookup some entities by key.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/rest/v1/projects/lookup

        :param keys: the keys to lookup.
        :type keys: list
        :param read_consistency: the read consistency to use. default, strong or eventual.
                                 Cannot be used with a transaction.
        :type read_consistency: str
        :param transaction: the transaction to use, if any.
        :type transaction: str
        :param project_id: Google Cloud Platform project ID against which to make the request.
        :type project_id: str
        :return: the response body of the lookup request.
        :rtype: dict
        """
        conn = self.get_conn()  # type: Any

        body = {'keys': keys}  # type: Dict[str, Any]
        if read_consistency:
            body['readConsistency'] = read_consistency
        if transaction:
            body['transaction'] = transaction
        resp = (conn  # pylint:disable=no-member
                .projects()
                .lookup(projectId=project_id, body=body)
                .execute(num_retries=self.num_retries))

        return resp

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def rollback(self, transaction: str, project_id: Optional[str] = None) -> Any:
        """
        Roll back a transaction.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/rest/v1/projects/rollback

        :param transaction: the transaction to roll back.
        :type transaction: str
        :param project_id: Google Cloud Platform project ID against which to make the request.
        :type project_id: str
        """
        conn = self.get_conn()  # type: Any

        conn.projects().rollback(  # pylint:disable=no-member
            projectId=project_id, body={'transaction': transaction}
        ).execute(num_retries=self.num_retries)

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def run_query(self, body: Dict, project_id: Optional[str] = None) -> Dict:
        """
        Run a query for entities.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/rest/v1/projects/runQuery

        :param body: the body of the query request.
        :type body: dict
        :param project_id: Google Cloud Platform project ID against which to make the request.
        :type project_id: str
        :return: the batch of query results.
        :rtype: dict
        """
        conn = self.get_conn()  # type: Any

        resp = (conn  # pylint:disable=no-member
                .projects()
                .runQuery(projectId=project_id, body=body)
                .execute(num_retries=self.num_retries))

        return resp['batch']

    def get_operation(self, name: str) -> Dict:
        """
        Gets the latest state of a long-running operation.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects.operations/get

        :param name: the name of the operation resource.
        :type name: str
        :return: a resource operation instance.
        :rtype: dict
        """
        conn = self.get_conn()  # type: Any

        resp = (conn  # pylint:disable=no-member
                .projects()
                .operations()
                .get(name=name)
                .execute(num_retries=self.num_retries))

        return resp

    def delete_operation(self, name: str) -> Dict:
        """
        Deletes the long-running operation.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects.operations/delete

        :param name: the name of the operation resource.
        :type name: str
        :return: none if successful.
        :rtype: dict
        """
        conn = self.get_conn()  # type: Any

        resp = (conn  # pylint:disable=no-member
                .projects()
                .operations()
                .delete(name=name)
                .execute(num_retries=self.num_retries))

        return resp

    def poll_operation_until_done(self, name: str, polling_interval_in_seconds: int) -> Dict:
        """
        Poll backup operation state until it's completed.

        :param name: the name of the operation resource
        :type name: str
        :param polling_interval_in_seconds: The number of seconds to wait before calling another request.
        :type polling_interval_in_seconds: int
        :return: a resource operation instance.
        :rtype: dict
        """
        while True:
            result = self.get_operation(name)  # type: Dict

            state = result['metadata']['common']['state']  # type: str
            if state == 'PROCESSING':
                self.log.info('Operation is processing. Re-polling state in {} seconds'
                              .format(polling_interval_in_seconds))
                time.sleep(polling_interval_in_seconds)
            else:
                return result

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def export_to_storage_bucket(self,
                                 bucket: str,
                                 namespace: Optional[str] = None,
                                 entity_filter: Optional[Dict] = None,
                                 labels: Optional[Dict[str, str]] = None,
                                 project_id: Optional[str] = None) -> Dict:
        """
        Export entities from Cloud Datastore to Cloud Storage for backup.

        .. note::
            Keep in mind that this requests the Admin API not the Data API.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/admin/rest/v1/projects/export

        :param bucket: The name of the Cloud Storage bucket.
        :type bucket: str
        :param namespace: The Cloud Storage namespace path.
        :type namespace: str
        :param entity_filter: Description of what data from the project is included in the export.
        :type entity_filter: dict
        :param labels: Client-assigned labels.
        :type labels: dict of str
        :param project_id: Google Cloud Platform project ID against which to make the request.
        :type project_id: str
        :return: a resource operation instance.
        :rtype: dict
        """
        admin_conn = self.get_conn()  # type: Any

        output_uri_prefix = 'gs://' + '/'.join(filter(None, [bucket, namespace]))  # type: str
        if not entity_filter:
            entity_filter = {}
        if not labels:
            labels = {}
        body = {
            'outputUrlPrefix': output_uri_prefix,
            'entityFilter': entity_filter,
            'labels': labels,
        }  # type: Dict
        resp = (admin_conn  # pylint:disable=no-member
                .projects()
                .export(projectId=project_id, body=body)
                .execute(num_retries=self.num_retries))

        return resp

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def import_from_storage_bucket(self,
                                   bucket: str,
                                   file: str,
                                   namespace: Optional[str] = None,
                                   entity_filter: Optional[Dict] = None,
                                   labels: Optional[Union[Dict, str]] = None,
                                   project_id: Optional[str] = None) -> Dict:
        """
        Import a backup from Cloud Storage to Cloud Datastore.

        .. note::
            Keep in mind that this requests the Admin API not the Data API.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/admin/rest/v1/projects/import

        :param bucket: The name of the Cloud Storage bucket.
        :type bucket: str
        :param file: the metadata file written by the projects.export operation.
        :type file: str
        :param namespace: The Cloud Storage namespace path.
        :type namespace: str
        :param entity_filter: specify which kinds/namespaces are to be imported.
        :type entity_filter: dict
        :param labels: Client-assigned labels.
        :type labels: dict of str
        :param project_id: Google Cloud Platform project ID against which to make the request.
        :type project_id: str
        :return: a resource operation instance.
        :rtype: dict
        """
        admin_conn = self.get_conn()  # type: Any

        input_url = 'gs://' + '/'.join(filter(None, [bucket, namespace, file]))  # type: str
        if not entity_filter:
            entity_filter = {}
        if not labels:
            labels = {}
        body = {
            'inputUrl': input_url,
            'entityFilter': entity_filter,
            'labels': labels,
        }  # type: Dict
        resp = (admin_conn  # pylint:disable=no-member
                .projects()
                .import_(projectId=project_id, body=body)
                .execute(num_retries=self.num_retries))

        return resp

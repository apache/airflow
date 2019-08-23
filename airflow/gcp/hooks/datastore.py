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

from googleapiclient.discovery import build

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


class DatastoreHook(GoogleCloudBaseHook):
    """
    Interact with Google Cloud Datastore. This hook uses the Google Cloud Platform connection.

    This object is not threads safe. If you want to make multiple requests
    simultaneously, you will need to create a hook per thread.

    :param api_version: The version of the API it is going to connect to.
    :type api_version: str
    """

    def __init__(self,
                 datastore_conn_id='google_cloud_default',
                 delegate_to=None,
                 api_version='v1'):
        super().__init__(datastore_conn_id, delegate_to)
        self.connection = None
        self.api_version = api_version
        self.num_retries = self._get_field('num_retries', 5)

    def get_conn(self):
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

    def allocate_ids(self, partial_keys):
        """
        Allocate IDs for incomplete keys.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/rest/v1/projects/allocateIds

        :param partial_keys: a list of partial keys.
        :type partial_keys: list
        :return: a list of full keys.
        :rtype: list
        """
        conn = self.get_conn()

        resp = (conn  # pylint:disable=no-member
                .projects()
                .allocateIds(projectId=self.project_id, body={'keys': partial_keys})
                .execute(num_retries=self.num_retries))

        return resp['keys']

    def begin_transaction(self):
        """
        Begins a new transaction.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/rest/v1/projects/beginTransaction

        :return: a transaction handle.
        :rtype: str
        """
        conn = self.get_conn()

        resp = (conn  # pylint:disable=no-member
                .projects()
                .beginTransaction(projectId=self.project_id, body={})
                .execute(num_retries=self.num_retries))

        return resp['transaction']

    def commit(self, body):
        """
        Commit a transaction, optionally creating, deleting or modifying some entities.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/rest/v1/projects/commit

        :param body: the body of the commit request.
        :type body: dict
        :return: the response body of the commit request.
        :rtype: dict
        """
        conn = self.get_conn()

        resp = (conn  # pylint:disable=no-member
                .projects()
                .commit(projectId=self.project_id, body=body)
                .execute(num_retries=self.num_retries))

        return resp

    def lookup(self, keys, read_consistency=None, transaction=None):
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
        :return: the response body of the lookup request.
        :rtype: dict
        """
        conn = self.get_conn()

        body = {'keys': keys}
        if read_consistency:
            body['readConsistency'] = read_consistency
        if transaction:
            body['transaction'] = transaction
        resp = (conn  # pylint:disable=no-member
                .projects()
                .lookup(projectId=self.project_id, body=body)
                .execute(num_retries=self.num_retries))

        return resp

    def rollback(self, transaction):
        """
        Roll back a transaction.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/rest/v1/projects/rollback

        :param transaction: the transaction to roll back.
        :type transaction: str
        """
        conn = self.get_conn()

        conn.projects().rollback(  # pylint:disable=no-member
            projectId=self.project_id, body={'transaction': transaction}
        ).execute(num_retries=self.num_retries)

    def run_query(self, body):
        """
        Run a query for entities.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/rest/v1/projects/runQuery

        :param body: the body of the query request.
        :type body: dict
        :return: the batch of query results.
        :rtype: dict
        """
        conn = self.get_conn()

        resp = (conn  # pylint:disable=no-member
                .projects()
                .runQuery(projectId=self.project_id, body=body)
                .execute(num_retries=self.num_retries))

        return resp['batch']

    def get_operation(self, name):
        """
        Gets the latest state of a long-running operation.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects.operations/get

        :param name: the name of the operation resource.
        :type name: str
        :return: a resource operation instance.
        :rtype: dict
        """
        conn = self.get_conn()

        resp = (conn  # pylint:disable=no-member
                .projects()
                .operations()
                .get(name=name)
                .execute(num_retries=self.num_retries))

        return resp

    def delete_operation(self, name):
        """
        Deletes the long-running operation.

        .. seealso::
            https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects.operations/delete

        :param name: the name of the operation resource.
        :type name: str
        :return: none if successful.
        :rtype: dict
        """
        conn = self.get_conn()

        resp = (conn  # pylint:disable=no-member
                .projects()
                .operations()
                .delete(name=name)
                .execute(num_retries=self.num_retries))

        return resp

    def poll_operation_until_done(self, name, polling_interval_in_seconds):
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
            result = self.get_operation(name)

            state = result['metadata']['common']['state']
            if state == 'PROCESSING':
                self.log.info('Operation is processing. Re-polling state in {} seconds'
                              .format(polling_interval_in_seconds))
                time.sleep(polling_interval_in_seconds)
            else:
                return result

    def export_to_storage_bucket(self, bucket, namespace=None, entity_filter=None, labels=None):
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
        :return: a resource operation instance.
        :rtype: dict
        """
        admin_conn = self.get_conn()

        output_uri_prefix = 'gs://' + '/'.join(filter(None, [bucket, namespace]))
        if not entity_filter:
            entity_filter = {}
        if not labels:
            labels = {}
        body = {
            'outputUrlPrefix': output_uri_prefix,
            'entityFilter': entity_filter,
            'labels': labels,
        }
        resp = (admin_conn  # pylint:disable=no-member
                .projects()
                .export(projectId=self.project_id, body=body)
                .execute(num_retries=self.num_retries))

        return resp

    def import_from_storage_bucket(self, bucket, file, namespace=None, entity_filter=None, labels=None):
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
        :return: a resource operation instance.
        :rtype: dict
        """
        admin_conn = self.get_conn()

        input_url = 'gs://' + '/'.join(filter(None, [bucket, namespace, file]))
        if not entity_filter:
            entity_filter = {}
        if not labels:
            labels = {}
        body = {
            'inputUrl': input_url,
            'entityFilter': entity_filter,
            'labels': labels,
        }
        resp = (admin_conn  # pylint:disable=no-member
                .projects()
                .import_(projectId=self.project_id, body=body)
                .execute(num_retries=self.num_retries))

        return resp

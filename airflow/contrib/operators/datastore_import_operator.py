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
from airflow.contrib.hooks.datastore_hook import DatastoreHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DatastoreImportOperator(BaseOperator):
    """
    Import entities from Cloud Storage to Google Cloud Datastore

    :param bucket: container in Cloud Storage to store data
    :type bucket: str
    :param file: path of the backup metadata file in the specified Cloud Storage bucket.
        It should have the extension .overall_export_metadata
    :type file: str
    :param namespace: optional namespace of the backup metadata file in
        the specified Cloud Storage bucket.
    :type namespace: str
    :param entity_filter: description of what data from the project is included in
        the export, refer to
        https://cloud.google.com/datastore/docs/reference/rest/Shared.Types/EntityFilter
    :type entity_filter: dict
    :param labels: client-assigned labels for cloud storage
    :type labels: dict
    :param datastore_conn_id: the name of the connection id to use
    :type datastore_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    :param polling_interval_in_seconds: number of seconds to wait before polling for
        execution status again
    :type polling_interval_in_seconds: int
    """

    @apply_defaults
    def __init__(self,
                 bucket,
                 file,
                 namespace=None,
                 entity_filter=None,
                 labels=None,
                 datastore_conn_id='google_cloud_default',
                 delegate_to=None,
                 polling_interval_in_seconds=10,
                 *args,
                 **kwargs):
        super(DatastoreImportOperator, self).__init__(*args, **kwargs)
        self.datastore_conn_id = datastore_conn_id
        self.delegate_to = delegate_to
        self.bucket = bucket
        self.file = file
        self.namespace = namespace
        self.entity_filter = entity_filter
        self.labels = labels
        self.polling_interval_in_seconds = polling_interval_in_seconds
        if kwargs.get('xcom_push') is not None:
            raise AirflowException("'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

    def execute(self, context):
        self.log.info('Importing data from Cloud Storage bucket %s', self.bucket)
        ds_hook = DatastoreHook(self.datastore_conn_id, self.delegate_to)
        result = ds_hook.import_from_storage_bucket(bucket=self.bucket,
                                                    file=self.file,
                                                    namespace=self.namespace,
                                                    entity_filter=self.entity_filter,
                                                    labels=self.labels)
        operation_name = result['name']
        result = ds_hook.poll_operation_until_done(operation_name,
                                                   self.polling_interval_in_seconds)

        state = result['metadata']['common']['state']
        if state != 'SUCCESSFUL':
            raise AirflowException('Operation failed: result={}'.format(result))

        return result

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
"""
This module contains a Google Cloud Storage list operator.
"""

from typing import Iterable

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GoogleCloudStorageListOperator(BaseOperator):
    """
    List all objects from the bucket with the give string prefix and delimiter in name.

    This operator returns a python list with the name of objects which can be used by
     `xcom` in the downstream task.

    :param bucket: The Google cloud storage bucket to find the objects. (templated)
    :type bucket: str
    :param prefix: Prefix string which filters objects whose name begin with
           this prefix. (templated)
    :type prefix: str
    :param delimiter: The delimiter by which you want to filter the objects. (templated)
        For e.g to lists the CSV files from in a directory in GCS you would use
        delimiter='.csv'.
    :type delimiter: str
    :param google_cloud_storage_conn_id: The connection ID to use when
        connecting to Google cloud storage.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str

    **Example**:
        The following Operator would list all the Avro files from ``sales/sales-2017``
        folder in ``data`` bucket. ::

            GCS_Files = GoogleCloudStorageListOperator(
                task_id='GCS_Files',
                bucket='data',
                prefix='sales/sales-2017/',
                delimiter='.avro',
                google_cloud_storage_conn_id=google_cloud_conn_id
            )
    """
    template_fields = ('bucket', 'prefix', 'delimiter')  # type: Iterable[str]

    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 bucket,
                 prefix=None,
                 delimiter=None,
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.delimiter = delimiter
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):

        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to
        )

        self.log.info('Getting list of the files. Bucket: %s; Delimiter: %s; Prefix: %s',
                      self.bucket, self.delimiter, self.prefix)

        return hook.list(bucket_name=self.bucket,
                         prefix=self.prefix,
                         delimiter=self.delimiter)

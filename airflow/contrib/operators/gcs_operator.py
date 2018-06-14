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

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.version import version


class GoogleCloudStorageCreateBucketOperator(BaseOperator):
    """
    Creates a new bucket. Google Cloud Storage uses a flat namespace,
    so you can't create a bucket with a name that is already in use.

        .. seealso::
            For more information, see Bucket Naming Guidelines:
            https://cloud.google.com/storage/docs/bucketnaming.html#requirements

    :param bucket_name: The name of the bucket. (templated)
    :type bucket_name: string
    :param storage_class: This defines how objects in the bucket are stored
            and determines the SLA and the cost of storage (templated). Values include

            - ``MULTI_REGIONAL``
            - ``REGIONAL``
            - ``STANDARD``
            - ``NEARLINE``
            - ``COLDLINE``.
            If this value is not specified when the bucket is
            created, it will default to STANDARD.
    :type storage_class: string
    :param location: The location of the bucket. (templated)
        Object data for objects in the bucket resides in physical storage
        within this region. Defaults to US.

        .. seealso::
            https://developers.google.com/storage/docs/bucket-locations

    :type location: string
    :param project_id: The ID of the GCP Project. (templated)
    :type project_id: string
    :param labels: User-provided labels, in key/value pairs.
    :type labels: dict
    :param google_cloud_storage_conn_id: The connection ID to use when
        connecting to Google cloud storage.
    :type google_cloud_storage_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must
        have domain-wide delegation enabled.
    :type delegate_to: string

    **Example**:
        The following Operator would create a new bucket ``test-bucket``
        with ``MULTI_REGIONAL`` storage class in ``EU`` region ::

            CreateBucket = GoogleCloudStorageCreateBucketOperator(
                task_id='CreateNewBucket',
                bucket_name='test-bucket',
                storage_class='MULTI_REGIONAL',
                location='EU',
                labels={'env': 'dev', 'team': 'airflow'},
                google_cloud_storage_conn_id='airflow-service-account'
            )
    """

    template_fields = ('bucket_name', 'storage_class',
                       'location', 'project_id')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 bucket_name,
                 storage_class='MULTI_REGIONAL',
                 location='US',
                 project_id=None,
                 labels=None,
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(GoogleCloudStorageCreateBucketOperator, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.storage_class = storage_class
        self.location = location
        self.project_id = project_id
        self.labels = labels

        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        if self.labels is not None:
            self.labels.update(
                {'airflow-version': 'v' + version.replace('.', '-').replace('+', '-')}
            )

        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to
        )

        hook.create_bucket(bucket_name=self.bucket_name,
                           storage_class=self.storage_class,
                           location=self.location,
                           project_id=self.project_id,
                           labels=self.labels)

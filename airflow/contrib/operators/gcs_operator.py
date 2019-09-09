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
This module contains a Google Cloud Storage Bucket operator.
"""
import warnings
from typing import Dict, Optional

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GoogleCloudStorageCreateBucketOperator(BaseOperator):
    """
    Creates a new bucket. Google Cloud Storage uses a flat namespace,
    so you can't create a bucket with a name that is already in use.

        .. seealso::
            For more information, see Bucket Naming Guidelines:
            https://cloud.google.com/storage/docs/bucketnaming.html#requirements

    :param bucket_name: The name of the bucket. (templated)
    :type bucket_name: str
    :param resource: An optional dict with parameters for creating the bucket.
            For information on available parameters, see Cloud Storage API doc:
            https://cloud.google.com/storage/docs/json_api/v1/buckets/insert
    :type resource: dict
    :param storage_class: This defines how objects in the bucket are stored
            and determines the SLA and the cost of storage (templated). Values include

            - ``MULTI_REGIONAL``
            - ``REGIONAL``
            - ``STANDARD``
            - ``NEARLINE``
            - ``COLDLINE``.

            If this value is not specified when the bucket is
            created, it will default to STANDARD.
    :type storage_class: str
    :param location: The location of the bucket. (templated)
        Object data for objects in the bucket resides in physical storage
        within this region. Defaults to US.

        .. seealso:: https://developers.google.com/storage/docs/bucket-locations

    :type location: str
    :param project_id: The ID of the GCP Project. (templated)
    :type project_id: str
    :param labels: User-provided labels, in key/value pairs.
    :type labels: dict
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud
        Platform. This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must
        have domain-wide delegation enabled.
    :type delegate_to: str

    The following Operator would create a new bucket ``test-bucket``
    with ``MULTI_REGIONAL`` storage class in ``EU`` region

    .. code-block:: python

        CreateBucket = GoogleCloudStorageCreateBucketOperator(
            task_id='CreateNewBucket',
            bucket_name='test-bucket',
            storage_class='MULTI_REGIONAL',
            location='EU',
            labels={'env': 'dev', 'team': 'airflow'},
            gcp_conn_id='airflow-conn-id'
        )

    """
    template_fields = ('bucket_name', 'storage_class',
                       'location', 'project_id')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 bucket_name: str,
                 resource: Optional[Dict] = None,
                 storage_class: str = 'MULTI_REGIONAL',
                 location: str = 'US',
                 project_id: Optional[str] = None,
                 labels: Optional[Dict] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 google_cloud_storage_conn_id: Optional[str] = None,
                 delegate_to: Optional[str] = None,
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if google_cloud_storage_conn_id:
            warnings.warn(
                "The google_cloud_storage_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.", DeprecationWarning, stacklevel=3)
            gcp_conn_id = google_cloud_storage_conn_id

        self.bucket_name = bucket_name
        self.resource = resource
        self.storage_class = storage_class
        self.location = location
        self.project_id = project_id
        self.labels = labels
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to
        )

        hook.create_bucket(bucket_name=self.bucket_name,
                           resource=self.resource,
                           storage_class=self.storage_class,
                           location=self.location,
                           project_id=self.project_id,
                           labels=self.labels)

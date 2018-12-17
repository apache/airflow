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

from airflow.models import BaseOperator
from airflow.contrib.hooks.gcp_transfer_hook import GCPTransferServiceHook
from airflow.utils.decorators import apply_defaults


class GoogleCloudStorageToGoogleCloudStorageTransferOperator(BaseOperator):
    """
    Copies objects from a bucket to another using the GCP Storage Transfer
    Service.

    :param source_bucket: The source Google cloud storage bucket where the
         object is. (templated)
    :type source_bucket: str
    :param destination_bucket: The destination Google cloud storage bucket
        where the object should be. (templated)
    :type destination_bucket: str
    :param project_id: The ID of the Google Cloud Platform Console project that
        owns the job
    :type project_id: str
    :param gcp_conn_id: Optional connection ID to use when connecting to Google Cloud
        Storage.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param description: Optional transfer service job description
    :type description: str
    :param schedule: Optional transfer service schedule; see
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs.
        If not set, run transfer job once as soon as the operator runs
    :type schedule: dict
    :param object_conditions: Optional transfer service object conditions; see
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/TransferSpec#ObjectConditions
    :type object_conditions: dict
    :param transfer_options: Optional transfer service transfer options; see
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/TransferSpec#TransferOptions
    :type transfer_options: dict
    :param wait: Wait for transfer to finish; defaults to `True`
    :type wait: bool

    **Example**:

    .. code-block:: python

       gcs_to_gcs_transfer_op = GoogleCloudStorageToGoogleCloudStorageTransferOperator(
            task_id='gcs_to_gcs_transfer_example',
            source_bucket='my-source-bucket',
            destination_bucket='my-destination-bucket',
            project_id='my-gcp-project',
            dag=my_dag)
    """

    template_fields = ('source_bucket', 'destination_bucket', 'description', 'object_conditions')
    ui_color = '#e09411'

    @apply_defaults
    def __init__(self,
                 source_bucket,
                 destination_bucket,
                 project_id=None,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 description=None,
                 schedule=None,
                 object_conditions=None,
                 transfer_options=None,
                 wait=True,
                 *args,
                 **kwargs):

        super(GoogleCloudStorageToGoogleCloudStorageTransferOperator, self).__init__(
            *args,
            **kwargs)
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.description = description
        self.schedule = schedule
        self.object_conditions = object_conditions or {}
        self.transfer_options = transfer_options or {}
        self.wait = wait

    def execute(self, context):
        transfer_hook = GCPTransferServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to)

        job = transfer_hook.create_transfer_job(
            project_id=self.project_id,
            description=self.description,
            schedule=self.schedule,
            transfer_spec={
                'gcsDataSource': {
                    'bucketName': self.source_bucket,
                },
                'gcsDataSink': {
                    'bucketName': self.destination_bucket,
                },
                'objectConditions': self.object_conditions,
                'transferOptions': self.transfer_options,
            }
        )

        if self.wait:
            transfer_hook.wait_for_transfer_job(job)

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
This module contains a operator
to create a Google Cloud DLP job and wait for it to finish.
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcp_data_loss_prevention_hook import CloudDataLossPreventionHook


class RunDlpJobOperator(BaseOperator):
    """
    Create a dlp job and wait for it to finish.

    :param parent: The parent resource name, for example projects/my-project-id. (templated)
    :type parent: str
    :param body: Destination path within the specified bucket. (templated)
    :type body: dict
    :param num_retries: The number of retries for Google service discovery API call.
    :type num_retries: number
    :parem polling_interval: The time interval of polling job status in seconds.
    :type polling_interval: number
    :param gcp_conn_id: The Airflow connection ID to upload with
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any
    :type delegate_to: str
    """

    template_fields = ('parent', 'body')

    @apply_defaults
    def __init__(self,
                 parent,
                 body,
                 num_retries=5,
                 polling_interval=100,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(RunDlpJobOperator, self).__init__(*args, **kwargs)
        self.parent = parent
        self.body = body
        self.num_retries = num_retries
        self.polling_interval = polling_interval
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        hook = CloudDataLossPreventionHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            num_retries=self.num_retries,
            polling_interval=self.polling_interval)

        hook.run_dlp_job(parent=self.parent, body=self.body)

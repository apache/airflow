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

from airflow.contrib.hooks.sagemaker_hook import SageMakerHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from airflow.exceptions import AirflowException


class SageMakerCreateTrainingJobOperator(BaseOperator):

    template_fields = ['training_job_config']
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(self,
                 sagemaker_conn_id=None,
                 job_name=None,
                 training_job_config=None,
                 *args, **kwargs):
        super(SageMakerCreateTrainingJobOperator, self).__init__(*args, **kwargs)

        self.sagemaker_conn_id = sagemaker_conn_id
        self.job_name = job_name
        self.training_job_config = training_job_config

    def execute(self, context):
        sagemaker = SageMakerHook(
            sagemaker_conn_id=self.sagemaker_conn_id, job_name=self.job_name)

        self.log.info(
            "Creating SageMaker Training Job %s." % self.job_name
        )
        response = sagemaker.create_training_job(self.training_job_config)
        if not response['ResponseMetadata']['HTTPStatusCode'] \
           == 200:
            raise AirflowException(
                'Sagemaker Training Job creation failed: %s' % response)
        else:
            return response

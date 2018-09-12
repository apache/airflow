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
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


class SageMakerCreateTrainingJobOperator(BaseOperator):

    """
       Initiate a SageMaker training

       This operator returns The ARN of the model created in Amazon SageMaker

       :param training_job_config:
       The configuration necessary to start a training job (templated)
       :type training_job_config: dict
       :param region_name: The AWS region_name
       :type region_name: str
       :param sagemaker_conn_id: The SageMaker connection ID to use.
       :type sagemaker_conn_id: str
       :param use_db_config: Whether or not to use db config
       associated with sagemaker_conn_id.
       If set to true, will automatically update the training config
       with what's in db, so the db config doesn't need to
       included everything, but what's there does replace the ones
       in the training_job_config, so be careful
       :type use_db_config: bool
       :param aws_conn_id: The AWS connection ID to use.
       :type aws_conn_id: str
       :param wait_for_completion: if the operator should block
       until training job finishes
       :type wait_for_completion: bool
       :param check_interval: if wait is set to be true, this is the time interval
       which the operator will check the status of the training job
       :type check_interval: int
       :param max_ingestion_time: if wait is set to be true, the operator will fail
       if the training job hasn't finish within the max_ingestion_time
       (Caution: be careful to set this parameters because training can take very long)
       :type max_ingestion_time: int

       **Example**:
           The following operator would start a training job when executed

            sagemaker_training =
               SageMakerCreateTrainingJobOperator(
                   task_id='sagemaker_training',
                   training_job_config=config,
                   region_name='us-west-2'
                   sagemaker_conn_id='sagemaker_customers_conn',
                   use_db_config=True,
                   aws_conn_id='aws_customers_conn'
               )
    """

    template_fields = ['training_job_config']
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(self,
                 training_job_config=None,
                 region_name=None,
                 sagemaker_conn_id=None,
                 use_db_config=False,
                 wait_for_completion=True,
                 check_interval=5,
                 max_ingestion_time=None,
                 *args, **kwargs):
        super(SageMakerCreateTrainingJobOperator, self).__init__(*args, **kwargs)

        self.sagemaker_conn_id = sagemaker_conn_id
        self.training_job_config = training_job_config
        self.use_db_config = use_db_config
        self.region_name = region_name
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval
        self.max_ingestion_time = max_ingestion_time

    def execute(self, context):
        sagemaker = SageMakerHook(
            sagemaker_conn_id=self.sagemaker_conn_id,
            use_db_config=self.use_db_config,
            region_name=self.region_name,
            check_interval=self.check_interval,
            max_ingestion_time=self.max_ingestion_time
        )

        self.log.info(
            "Creating SageMaker Training Job %s."
            % self.training_job_config['TrainingJobName']
        )
        response = sagemaker.create_training_job(
            self.training_job_config,
            wait_for_completion=self.wait_for_completion)
        if not response['ResponseMetadata']['HTTPStatusCode'] \
           == 200:
            raise AirflowException(
                'Sagemaker Training Job creation failed: %s' % response)
        else:
            return response

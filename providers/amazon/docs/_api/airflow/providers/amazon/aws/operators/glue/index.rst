 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

:py:mod:`airflow.providers.amazon.aws.operators.glue`
=====================================================

.. py:module:: airflow.providers.amazon.aws.operators.glue


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.glue.GlueJobOperator




.. py:class:: GlueJobOperator(*, job_name = 'aws_glue_default_job', job_desc = 'AWS Glue Job with Airflow', script_location = None, concurrent_run_limit = None, script_args = None, retry_limit = 0, num_of_dpus = None, aws_conn_id = 'aws_default', region_name = None, s3_bucket = None, iam_role_name = None, iam_role_arn = None, create_job_kwargs = None, run_job_kwargs = None, wait_for_completion = True, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), verbose = False, update_config = False, job_poll_interval = 6, stop_job_run_on_kill = False, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Create an AWS Glue Job.

   AWS Glue is a serverless Spark ETL service for running Spark Jobs on the AWS
   cloud. Language support: Python and Scala.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GlueJobOperator`

   :param job_name: unique job name per AWS Account
   :param script_location: location of ETL script. Must be a local or S3 path
   :param job_desc: job description details
   :param concurrent_run_limit: The maximum number of concurrent runs allowed for a job
   :param script_args: etl script arguments and AWS Glue arguments (templated)
   :param retry_limit: The maximum number of times to retry this job if it fails
   :param num_of_dpus: Number of AWS Glue DPUs to allocate to this Job.
   :param region_name: aws region name (example: us-east-1)
   :param s3_bucket: S3 bucket where logs and local etl script will be uploaded
   :param iam_role_name: AWS IAM Role for Glue Job Execution. If set `iam_role_arn` must equal None.
   :param iam_role_arn: AWS IAM ARN for Glue Job Execution. If set `iam_role_name` must equal None.
   :param create_job_kwargs: Extra arguments for Glue Job Creation
   :param run_job_kwargs: Extra arguments for Glue Job Run
   :param wait_for_completion: Whether to wait for job run completion. (default: True)
   :param deferrable: If True, the operator will wait asynchronously for the job to complete.
       This implies waiting for completion. This mode requires aiobotocore module to be installed.
       (default: False)
   :param verbose: If True, Glue Job Run logs show in the Airflow Task Logs.  (default: False)
   :param update_config: If True, Operator will update job configuration.  (default: False)
   :param stop_job_run_on_kill: If True, Operator will stop the job run when task is killed.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('job_name', 'script_location', 'script_args', 'create_job_kwargs', 's3_bucket',...



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:attribute:: template_fields_renderers



   .. py:attribute:: ui_color
      :value: '#ededed'



   .. py:attribute:: operator_extra_links
      :value: ()



   .. py:method:: glue_job_hook()


   .. py:method:: execute(context)

      Execute AWS Glue Job from Airflow.

      :return: the current Glue job ID.


   .. py:method:: execute_complete(context, event=None)


   .. py:method:: on_kill()

      Cancel the running AWS Glue Job.

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

:py:mod:`airflow.providers.amazon.aws.hooks.glue`
=================================================

.. py:module:: airflow.providers.amazon.aws.hooks.glue


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.glue.GlueJobHook




Attributes
~~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.glue.DEFAULT_LOG_SUFFIX
   airflow.providers.amazon.aws.hooks.glue.ERROR_LOG_SUFFIX


.. py:data:: DEFAULT_LOG_SUFFIX
   :value: 'output'



.. py:data:: ERROR_LOG_SUFFIX
   :value: 'error'



.. py:class:: GlueJobHook(s3_bucket = None, job_name = None, desc = None, concurrent_run_limit = 1, script_location = None, retry_limit = 0, num_of_dpus = None, iam_role_name = None, iam_role_arn = None, create_job_kwargs = None, update_config = False, job_poll_interval = 6, *args, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with AWS Glue.

   Provide thick wrapper around :external+boto3:py:class:`boto3.client("glue") <Glue.Client>`.

   :param s3_bucket: S3 bucket where logs and local etl script will be uploaded
   :param job_name: unique job name per AWS account
   :param desc: job description
   :param concurrent_run_limit: The maximum number of concurrent runs allowed for a job
   :param script_location: path to etl script on s3
   :param retry_limit: Maximum number of times to retry this job if it fails
   :param num_of_dpus: Number of AWS Glue DPUs to allocate to this Job
   :param region_name: aws region name (example: us-east-1)
   :param iam_role_name: AWS IAM Role for Glue Job Execution. If set `iam_role_arn` must equal None.
   :param iam_role_arn: AWS IAM Role ARN for Glue Job Execution, If set `iam_role_name` must equal None.
   :param create_job_kwargs: Extra arguments for Glue Job Creation
   :param update_config: Update job configuration on Glue (default: False)

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   .. py:class:: LogContinuationTokens


      Used to hold the continuation tokens when reading logs from both streams Glue Jobs write to.


   .. py:method:: create_glue_job_config()


   .. py:method:: list_jobs()

      Get list of Jobs.

      .. seealso::
          - :external+boto3:py:meth:`Glue.Client.get_jobs`


   .. py:method:: get_iam_execution_role()


   .. py:method:: initialize_job(script_arguments = None, run_kwargs = None)

      Initialize connection with AWS Glue to run job.

      .. seealso::
          - :external+boto3:py:meth:`Glue.Client.start_job_run`


   .. py:method:: get_job_state(job_name, run_id)

      Get state of the Glue job; the job state can be running, finished, failed, stopped or timeout.

      .. seealso::
          - :external+boto3:py:meth:`Glue.Client.get_job_run`

      :param job_name: unique job name per AWS account
      :param run_id: The job-run ID of the predecessor job run
      :return: State of the Glue job


   .. py:method:: async_get_job_state(job_name, run_id)
      :async:

      Get state of the Glue job; the job state can be running, finished, failed, stopped or timeout.

      The async version of get_job_state.


   .. py:method:: print_job_logs(job_name, run_id, continuation_tokens)

      Print the latest job logs to the Airflow task log and updates the continuation tokens.

      :param continuation_tokens: the tokens where to resume from when reading logs.
          The object gets updated with the new tokens by this method.


   .. py:method:: job_completion(job_name, run_id, verbose = False)

      Wait until Glue job with job_name finishes; return final state if finished or raises AirflowException.

      :param job_name: unique job name per AWS account
      :param run_id: The job-run ID of the predecessor job run
      :param verbose: If True, more Glue Job Run logs show in the Airflow Task Logs.  (default: False)
      :return: Dict of JobRunState and JobRunId


   .. py:method:: async_job_completion(job_name, run_id, verbose = False)
      :async:

      Wait until Glue job with job_name finishes; return final state if finished or raises AirflowException.

      :param job_name: unique job name per AWS account
      :param run_id: The job-run ID of the predecessor job run
      :param verbose: If True, more Glue Job Run logs show in the Airflow Task Logs.  (default: False)
      :return: Dict of JobRunState and JobRunId


   .. py:method:: has_job(job_name)

      Check if the job already exists.

      .. seealso::
          - :external+boto3:py:meth:`Glue.Client.get_job`

      :param job_name: unique job name per AWS account
      :return: Returns True if the job already exists and False if not.


   .. py:method:: update_job(**job_kwargs)

      Update job configurations.

      .. seealso::
          - :external+boto3:py:meth:`Glue.Client.update_job`

      :param job_kwargs: Keyword args that define the configurations used for the job
      :return: True if job was updated and false otherwise


   .. py:method:: get_or_create_glue_job()

      Get (or creates) and returns the Job name.

      .. seealso::
          - :external+boto3:py:meth:`Glue.Client.create_job`

      :return:Name of the Job


   .. py:method:: create_or_update_glue_job()

      Create (or update) and return the Job name.

      .. seealso::
          - :external+boto3:py:meth:`Glue.Client.update_job`
          - :external+boto3:py:meth:`Glue.Client.create_job`

      :return:Name of the Job

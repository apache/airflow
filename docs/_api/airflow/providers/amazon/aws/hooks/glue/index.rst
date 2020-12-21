:mod:`airflow.providers.amazon.aws.hooks.glue`
==============================================

.. py:module:: airflow.providers.amazon.aws.hooks.glue


Module Contents
---------------

.. py:class:: AwsGlueJobHook(s3_bucket: Optional[str] = None, job_name: Optional[str] = None, desc: Optional[str] = None, concurrent_run_limit: int = 1, script_location: Optional[str] = None, retry_limit: int = 0, num_of_dpus: int = 10, region_name: Optional[str] = None, iam_role_name: Optional[str] = None, *args, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with AWS Glue - create job, trigger, crawler

   :param s3_bucket: S3 bucket where logs and local etl script will be uploaded
   :type s3_bucket: Optional[str]
   :param job_name: unique job name per AWS account
   :type job_name: Optional[str]
   :param desc: job description
   :type desc: Optional[str]
   :param concurrent_run_limit: The maximum number of concurrent runs allowed for a job
   :type concurrent_run_limit: int
   :param script_location: path to etl script on s3
   :type script_location: Optional[str]
   :param retry_limit: Maximum number of times to retry this job if it fails
   :type retry_limit: int
   :param num_of_dpus: Number of AWS Glue DPUs to allocate to this Job
   :type num_of_dpus: int
   :param region_name: aws region name (example: us-east-1)
   :type region_name: Optional[str]
   :param iam_role_name: AWS IAM Role for Glue Job
   :type iam_role_name: Optional[str]

   .. attribute:: JOB_POLL_INTERVAL
      :annotation: = 6

      

   
   .. method:: list_jobs(self)

      :return: Lists of Jobs



   
   .. method:: get_iam_execution_role(self)

      :return: iam role for job execution



   
   .. method:: initialize_job(self, script_arguments: Optional[dict] = None)

      Initializes connection with AWS Glue
      to run job
      :return:



   
   .. method:: get_job_state(self, job_name: str, run_id: str)

      Get state of the Glue job. The job state can be
      running, finished, failed, stopped or timeout.
      :param job_name: unique job name per AWS account
      :type job_name: str
      :param run_id: The job-run ID of the predecessor job run
      :type run_id: str
      :return: State of the Glue job



   
   .. method:: job_completion(self, job_name: str, run_id: str)

      Waits until Glue job with job_name completes or
      fails and return final state if finished.
      Raises AirflowException when the job failed
      :param job_name: unique job name per AWS account
      :type job_name: str
      :param run_id: The job-run ID of the predecessor job run
      :type run_id: str
      :return: Dict of JobRunState and JobRunId



   
   .. method:: get_or_create_glue_job(self)

      Creates(or just returns) and returns the Job name
      :return:Name of the Job





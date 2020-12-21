:mod:`airflow.providers.amazon.aws.operators.glue`
==================================================

.. py:module:: airflow.providers.amazon.aws.operators.glue


Module Contents
---------------

.. py:class:: AwsGlueJobOperator(*, job_name: str = 'aws_glue_default_job', job_desc: str = 'AWS Glue Job with Airflow', script_location: Optional[str] = None, concurrent_run_limit: Optional[int] = None, script_args: Optional[dict] = None, retry_limit: Optional[int] = None, num_of_dpus: int = 6, aws_conn_id: str = 'aws_default', region_name: Optional[str] = None, s3_bucket: Optional[str] = None, iam_role_name: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates an AWS Glue Job. AWS Glue is a serverless Spark
   ETL service for running Spark Jobs on the AWS cloud.
   Language support: Python and Scala

   :param job_name: unique job name per AWS Account
   :type job_name: Optional[str]
   :param script_location: location of ETL script. Must be a local or S3 path
   :type script_location: Optional[str]
   :param job_desc: job description details
   :type job_desc: Optional[str]
   :param concurrent_run_limit: The maximum number of concurrent runs allowed for a job
   :type concurrent_run_limit: Optional[int]
   :param script_args: etl script arguments and AWS Glue arguments
   :type script_args: dict
   :param retry_limit: The maximum number of times to retry this job if it fails
   :type retry_limit: Optional[int]
   :param num_of_dpus: Number of AWS Glue DPUs to allocate to this Job.
   :type num_of_dpus: int
   :param region_name: aws region name (example: us-east-1)
   :type region_name: str
   :param s3_bucket: S3 bucket where logs and local etl script will be uploaded
   :type s3_bucket: Optional[str]
   :param iam_role_name: AWS IAM Role for Glue Job Execution
   :type iam_role_name: Optional[str]

   .. attribute:: template_fields
      :annotation: = []

      

   .. attribute:: template_ext
      :annotation: = []

      

   .. attribute:: ui_color
      :annotation: = #ededed

      

   
   .. method:: execute(self, context)

      Executes AWS Glue Job from Airflow

      :return: the id of the current glue job.





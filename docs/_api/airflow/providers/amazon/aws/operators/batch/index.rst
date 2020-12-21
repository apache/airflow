:mod:`airflow.providers.amazon.aws.operators.batch`
===================================================

.. py:module:: airflow.providers.amazon.aws.operators.batch

.. autoapi-nested-parse::

   An Airflow operator for AWS Batch services

   .. seealso::

       - http://boto3.readthedocs.io/en/latest/guide/configuration.html
       - http://boto3.readthedocs.io/en/latest/reference/services/batch.html
       - https://docs.aws.amazon.com/batch/latest/APIReference/Welcome.html



Module Contents
---------------

.. py:class:: AwsBatchOperator(*, job_name: str, job_definition: str, job_queue: str, overrides: dict, array_properties: Optional[dict] = None, parameters: Optional[dict] = None, job_id: Optional[str] = None, waiters: Optional[Any] = None, max_retries: Optional[int] = None, status_retries: Optional[int] = None, aws_conn_id: Optional[str] = None, region_name: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Execute a job on AWS Batch

   :param job_name: the name for the job that will run on AWS Batch (templated)
   :type job_name: str

   :param job_definition: the job definition name on AWS Batch
   :type job_definition: str

   :param job_queue: the queue name on AWS Batch
   :type job_queue: str

   :param overrides: the `containerOverrides` parameter for boto3 (templated)
   :type overrides: Optional[dict]

   :param array_properties: the `arrayProperties` parameter for boto3
   :type array_properties: Optional[dict]

   :param parameters: the `parameters` for boto3 (templated)
   :type parameters: Optional[dict]

   :param job_id: the job ID, usually unknown (None) until the
       submit_job operation gets the jobId defined by AWS Batch
   :type job_id: Optional[str]

   :param waiters: an :py:class:`.AwsBatchWaiters` object (see note below);
       if None, polling is used with max_retries and status_retries.
   :type waiters: Optional[AwsBatchWaiters]

   :param max_retries: exponential back-off retries, 4200 = 48 hours;
       polling is only used when waiters is None
   :type max_retries: int

   :param status_retries: number of HTTP retries to get job status, 10;
       polling is only used when waiters is None
   :type status_retries: int

   :param aws_conn_id: connection id of AWS credentials / region name. If None,
       credential boto3 strategy will be used.
   :type aws_conn_id: str

   :param region_name: region name to use in AWS Hook.
       Override the region_name in connection (if provided)
   :type region_name: str

   .. note::
       Any custom waiters must return a waiter for these calls:
       .. code-block:: python

           waiter = waiters.get_waiter("JobExists")
           waiter = waiters.get_waiter("JobRunning")
           waiter = waiters.get_waiter("JobComplete")

   .. attribute:: ui_color
      :annotation: = #c3dae0

      

   .. attribute:: arn
      :annotation: :Optional[str]

      

   .. attribute:: template_fields
      :annotation: = ['job_name', 'overrides', 'parameters']

      

   
   .. method:: execute(self, context: Dict)

      Submit and monitor an AWS Batch job

      :raises: AirflowException



   
   .. method:: on_kill(self)



   
   .. method:: submit_job(self, context: Dict)

      Submit an AWS Batch job

      :raises: AirflowException



   
   .. method:: monitor_job(self, context: Dict)

      Monitor an AWS Batch job

      :raises: AirflowException





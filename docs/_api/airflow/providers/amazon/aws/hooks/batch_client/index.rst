:mod:`airflow.providers.amazon.aws.hooks.batch_client`
======================================================

.. py:module:: airflow.providers.amazon.aws.hooks.batch_client

.. autoapi-nested-parse::

   A client for AWS batch services

   .. seealso::

       - http://boto3.readthedocs.io/en/latest/guide/configuration.html
       - http://boto3.readthedocs.io/en/latest/reference/services/batch.html
       - https://docs.aws.amazon.com/batch/latest/APIReference/Welcome.html



Module Contents
---------------

.. py:class:: AwsBatchProtocol

   Bases: :class:`airflow.typing_compat.Protocol`

   A structured Protocol for ``boto3.client('batch') -> botocore.client.Batch``.
   This is used for type hints on :py:meth:`.AwsBatchClient.client`; it covers
   only the subset of client methods required.

   .. seealso::

       - https://mypy.readthedocs.io/en/latest/protocols.html
       - http://boto3.readthedocs.io/en/latest/reference/services/batch.html

   
   .. method:: describe_jobs(self, jobs: List[str])

      Get job descriptions from AWS batch

      :param jobs: a list of JobId to describe
      :type jobs: List[str]

      :return: an API response to describe jobs
      :rtype: Dict



   
   .. method:: get_waiter(self, waiterName: str)

      Get an AWS Batch service waiter

      :param waiterName: The name of the waiter.  The name should match
          the name (including the casing) of the key name in the waiter
          model file (typically this is CamelCasing).
      :type waiterName: str

      :return: a waiter object for the named AWS batch service
      :rtype: botocore.waiter.Waiter

      .. note::
          AWS batch might not have any waiters (until botocore PR-1307 is released).

          .. code-block:: python

              import boto3
              boto3.client('batch').waiter_names == []

      .. seealso::

          - https://boto3.amazonaws.com/v1/documentation/api/latest/guide/clients.html#waiters
          - https://github.com/boto/botocore/pull/1307



   
   .. method:: submit_job(self, jobName: str, jobQueue: str, jobDefinition: str, arrayProperties: Dict, parameters: Dict, containerOverrides: Dict)

      Submit a batch job

      :param jobName: the name for the AWS batch job
      :type jobName: str

      :param jobQueue: the queue name on AWS Batch
      :type jobQueue: str

      :param jobDefinition: the job definition name on AWS Batch
      :type jobDefinition: str

      :param arrayProperties: the same parameter that boto3 will receive
      :type arrayProperties: Dict

      :param parameters: the same parameter that boto3 will receive
      :type parameters: Dict

      :param containerOverrides: the same parameter that boto3 will receive
      :type containerOverrides: Dict

      :return: an API response
      :rtype: Dict



   
   .. method:: terminate_job(self, jobId: str, reason: str)

      Terminate a batch job

      :param jobId: a job ID to terminate
      :type jobId: str

      :param reason: a reason to terminate job ID
      :type reason: str

      :return: an API response
      :rtype: Dict




.. py:class:: AwsBatchClientHook(*args, max_retries: Optional[int] = None, status_retries: Optional[int] = None, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   A client for AWS batch services.

   :param max_retries: exponential back-off retries, 4200 = 48 hours;
       polling is only used when waiters is None
   :type max_retries: Optional[int]

   :param status_retries: number of HTTP retries to get job status, 10;
       polling is only used when waiters is None
   :type status_retries: Optional[int]

   .. note::
       Several methods use a default random delay to check or poll for job status, i.e.
       ``random.uniform(DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX)``
       Using a random interval helps to avoid AWS API throttle limits
       when many concurrent tasks request job-descriptions.

       To modify the global defaults for the range of jitter allowed when a
       random delay is used to check batch job status, modify these defaults, e.g.:
       .. code-block::

           AwsBatchClient.DEFAULT_DELAY_MIN = 0
           AwsBatchClient.DEFAULT_DELAY_MAX = 5

       When explict delay values are used, a 1 second random jitter is applied to the
       delay (e.g. a delay of 0 sec will be a ``random.uniform(0, 1)`` delay.  It is
       generally recommended that random jitter is added to API requests.  A
       convenience method is provided for this, e.g. to get a random delay of
       10 sec +/- 5 sec: ``delay = AwsBatchClient.add_jitter(10, width=5, minima=0)``

   .. seealso::
       - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch.html
       - https://docs.aws.amazon.com/general/latest/gr/api-retries.html
       - https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/

   .. attribute:: MAX_RETRIES
      :annotation: = 4200

      

   .. attribute:: STATUS_RETRIES
      :annotation: = 10

      

   .. attribute:: DEFAULT_DELAY_MIN
      :annotation: = 1

      

   .. attribute:: DEFAULT_DELAY_MAX
      :annotation: = 10

      

   .. attribute:: client
      

      An AWS API client for batch services, like ``boto3.client('batch')``

      :return: a boto3 'batch' client for the ``.region_name``
      :rtype: Union[AwsBatchProtocol, botocore.client.BaseClient]


   
   .. method:: terminate_job(self, job_id: str, reason: str)

      Terminate a batch job

      :param job_id: a job ID to terminate
      :type job_id: str

      :param reason: a reason to terminate job ID
      :type reason: str

      :return: an API response
      :rtype: Dict



   
   .. method:: check_job_success(self, job_id: str)

      Check the final status of the batch job; return True if the job
      'SUCCEEDED', else raise an AirflowException

      :param job_id: a batch job ID
      :type job_id: str

      :rtype: bool

      :raises: AirflowException



   
   .. method:: wait_for_job(self, job_id: str, delay: Union[int, float, None] = None)

      Wait for batch job to complete

      :param job_id: a batch job ID
      :type job_id: str

      :param delay: a delay before polling for job status
      :type delay: Optional[Union[int, float]]

      :raises: AirflowException



   
   .. method:: poll_for_job_running(self, job_id: str, delay: Union[int, float, None] = None)

      Poll for job running. The status that indicates a job is running or
      already complete are: 'RUNNING'|'SUCCEEDED'|'FAILED'.

      So the status options that this will wait for are the transitions from:
      'SUBMITTED'>'PENDING'>'RUNNABLE'>'STARTING'>'RUNNING'|'SUCCEEDED'|'FAILED'

      The completed status options are included for cases where the status
      changes too quickly for polling to detect a RUNNING status that moves
      quickly from STARTING to RUNNING to completed (often a failure).

      :param job_id: a batch job ID
      :type job_id: str

      :param delay: a delay before polling for job status
      :type delay: Optional[Union[int, float]]

      :raises: AirflowException



   
   .. method:: poll_for_job_complete(self, job_id: str, delay: Union[int, float, None] = None)

      Poll for job completion. The status that indicates job completion
      are: 'SUCCEEDED'|'FAILED'.

      So the status options that this will wait for are the transitions from:
      'SUBMITTED'>'PENDING'>'RUNNABLE'>'STARTING'>'RUNNING'>'SUCCEEDED'|'FAILED'

      :param job_id: a batch job ID
      :type job_id: str

      :param delay: a delay before polling for job status
      :type delay: Optional[Union[int, float]]

      :raises: AirflowException



   
   .. method:: poll_job_status(self, job_id: str, match_status: List[str])

      Poll for job status using an exponential back-off strategy (with max_retries).

      :param job_id: a batch job ID
      :type job_id: str

      :param match_status: a list of job status to match; the batch job status are:
          'SUBMITTED'|'PENDING'|'RUNNABLE'|'STARTING'|'RUNNING'|'SUCCEEDED'|'FAILED'
      :type match_status: List[str]

      :rtype: bool

      :raises: AirflowException



   
   .. method:: get_job_description(self, job_id: str)

      Get job description (using status_retries).

      :param job_id: a batch job ID
      :type job_id: str

      :return: an API response for describe jobs
      :rtype: Dict

      :raises: AirflowException



   
   .. staticmethod:: parse_job_description(job_id: str, response: Dict)

      Parse job description to extract description for job_id

      :param job_id: a batch job ID
      :type job_id: str

      :param response: an API response for describe jobs
      :type response: Dict

      :return: an API response to describe job_id
      :rtype: Dict

      :raises: AirflowException



   
   .. staticmethod:: add_jitter(delay: Union[int, float], width: Union[int, float] = 1, minima: Union[int, float] = 0)

      Use delay +/- width for random jitter

      Adding jitter to status polling can help to avoid
      AWS batch API limits for monitoring batch jobs with
      a high concurrency in Airflow tasks.

      :param delay: number of seconds to pause;
          delay is assumed to be a positive number
      :type delay: Union[int, float]

      :param width: delay +/- width for random jitter;
          width is assumed to be a positive number
      :type width: Union[int, float]

      :param minima: minimum delay allowed;
          minima is assumed to be a non-negative number
      :type minima: Union[int, float]

      :return: uniform(delay - width, delay + width) jitter
          and it is a non-negative number
      :rtype: float



   
   .. staticmethod:: delay(delay: Union[int, float, None] = None)

      Pause execution for ``delay`` seconds.

      :param delay: a delay to pause execution using ``time.sleep(delay)``;
          a small 1 second jitter is applied to the delay.
      :type delay: Optional[Union[int, float]]

      .. note::
          This method uses a default random delay, i.e.
          ``random.uniform(DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX)``;
          using a random interval helps to avoid AWS API throttle limits
          when many concurrent tasks request job-descriptions.



   
   .. staticmethod:: exponential_delay(tries: int)

      An exponential back-off delay, with random jitter.  There is a maximum
      interval of 10 minutes (with random jitter between 3 and 10 minutes).
      This is used in the :py:meth:`.poll_for_job_status` method.

      :param tries: Number of tries
      :type tries: int

      :rtype: float

      Examples of behavior:

      .. code-block:: python

          def exp(tries):
              max_interval = 600.0  # 10 minutes in seconds
              delay = 1 + pow(tries * 0.6, 2)
              delay = min(max_interval, delay)
              print(delay / 3, delay)

          for tries in range(10):
              exp(tries)

          #  0.33  1.0
          #  0.45  1.35
          #  0.81  2.44
          #  1.41  4.23
          #  2.25  6.76
          #  3.33 10.00
          #  4.65 13.95
          #  6.21 18.64
          #  8.01 24.04
          # 10.05 30.15

      .. seealso::

          - https://docs.aws.amazon.com/general/latest/gr/api-retries.html
          - https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/





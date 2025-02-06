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

:py:mod:`airflow.providers.amazon.aws.hooks.batch_client`
=========================================================

.. py:module:: airflow.providers.amazon.aws.hooks.batch_client

.. autoapi-nested-parse::

   A client for AWS Batch services.

   .. seealso::

       - https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html
       - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch.html
       - https://docs.aws.amazon.com/batch/latest/APIReference/Welcome.html



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.batch_client.BatchProtocol
   airflow.providers.amazon.aws.hooks.batch_client.BatchClientHook




.. py:class:: BatchProtocol


   Bases: :py:obj:`airflow.typing_compat.Protocol`

   A structured Protocol for ``boto3.client('batch') -> botocore.client.Batch``.

   This is used for type hints on :py:meth:`.BatchClient.client`; it covers
   only the subset of client methods required.

   .. seealso::

       - https://mypy.readthedocs.io/en/latest/protocols.html
       - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch.html

   .. py:method:: describe_jobs(jobs)

      Get job descriptions from AWS Batch.

      :param jobs: a list of JobId to describe

      :return: an API response to describe jobs


   .. py:method:: get_waiter(waiterName)

      Get an AWS Batch service waiter.

      :param waiterName: The name of the waiter.  The name should match
          the name (including the casing) of the key name in the waiter
          model file (typically this is CamelCasing).

      :return: a waiter object for the named AWS Batch service

      .. note::
          AWS Batch might not have any waiters (until botocore PR-1307 is released).

          .. code-block:: python

              import boto3

              boto3.client("batch").waiter_names == []

      .. seealso::

          - https://boto3.amazonaws.com/v1/documentation/api/latest/guide/clients.html#waiters
          - https://github.com/boto/botocore/pull/1307


   .. py:method:: submit_job(jobName, jobQueue, jobDefinition, arrayProperties, parameters, containerOverrides, tags)

      Submit a Batch job.

      :param jobName: the name for the AWS Batch job

      :param jobQueue: the queue name on AWS Batch

      :param jobDefinition: the job definition name on AWS Batch

      :param arrayProperties: the same parameter that boto3 will receive

      :param parameters: the same parameter that boto3 will receive

      :param containerOverrides: the same parameter that boto3 will receive

      :param tags: the same parameter that boto3 will receive

      :return: an API response


   .. py:method:: terminate_job(jobId, reason)

      Terminate a Batch job.

      :param jobId: a job ID to terminate

      :param reason: a reason to terminate job ID

      :return: an API response



.. py:class:: BatchClientHook(*args, max_retries = None, status_retries = None, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with AWS Batch.

   Provide thick wrapper around :external+boto3:py:class:`boto3.client("batch") <Batch.Client>`.

   :param max_retries: exponential back-off retries, 4200 = 48 hours;
       polling is only used when waiters is None
   :param status_retries: number of HTTP retries to get job status, 10;
       polling is only used when waiters is None

   .. note::
       Several methods use a default random delay to check or poll for job status, i.e.
       ``random.uniform(DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX)``
       Using a random interval helps to avoid AWS API throttle limits
       when many concurrent tasks request job-descriptions.

       To modify the global defaults for the range of jitter allowed when a
       random delay is used to check Batch job status, modify these defaults, e.g.:
       .. code-block::

           BatchClient.DEFAULT_DELAY_MIN = 0
           BatchClient.DEFAULT_DELAY_MAX = 5

       When explicit delay values are used, a 1 second random jitter is applied to the
       delay (e.g. a delay of 0 sec will be a ``random.uniform(0, 1)`` delay.  It is
       generally recommended that random jitter is added to API requests.  A
       convenience method is provided for this, e.g. to get a random delay of
       10 sec +/- 5 sec: ``delay = BatchClient.add_jitter(10, width=5, minima=0)``

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
       - https://docs.aws.amazon.com/general/latest/gr/api-retries.html
       - https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/

   .. py:property:: client
      :type: BatchProtocol | botocore.client.BaseClient

      An AWS API client for Batch services.

      :return: a boto3 'batch' client for the ``.region_name``


   .. py:attribute:: MAX_RETRIES
      :value: 4200



   .. py:attribute:: STATUS_RETRIES
      :value: 10



   .. py:attribute:: DEFAULT_DELAY_MIN
      :value: 1



   .. py:attribute:: DEFAULT_DELAY_MAX
      :value: 10



   .. py:attribute:: FAILURE_STATE
      :value: 'FAILED'



   .. py:attribute:: SUCCESS_STATE
      :value: 'SUCCEEDED'



   .. py:attribute:: RUNNING_STATE
      :value: 'RUNNING'



   .. py:attribute:: INTERMEDIATE_STATES
      :value: ('SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING')



   .. py:attribute:: COMPUTE_ENVIRONMENT_TERMINAL_STATUS
      :value: ('VALID', 'DELETED')



   .. py:attribute:: COMPUTE_ENVIRONMENT_INTERMEDIATE_STATUS
      :value: ('CREATING', 'UPDATING', 'DELETING')



   .. py:attribute:: JOB_QUEUE_TERMINAL_STATUS
      :value: ('VALID', 'DELETED')



   .. py:attribute:: JOB_QUEUE_INTERMEDIATE_STATUS
      :value: ('CREATING', 'UPDATING', 'DELETING')



   .. py:method:: terminate_job(job_id, reason)

      Terminate a Batch job.

      :param job_id: a job ID to terminate

      :param reason: a reason to terminate job ID

      :return: an API response


   .. py:method:: check_job_success(job_id)

      Check the final status of the Batch job.

      Return True if the job 'SUCCEEDED', else raise an AirflowException.

      :param job_id: a Batch job ID

      :raises: AirflowException


   .. py:method:: wait_for_job(job_id, delay = None, get_batch_log_fetcher = None)

      Wait for Batch job to complete.

      :param job_id: a Batch job ID

      :param delay: a delay before polling for job status

      :param get_batch_log_fetcher : a method that returns batch_log_fetcher

      :raises: AirflowException


   .. py:method:: poll_for_job_running(job_id, delay = None)

      Poll for job running.

      The status that indicates a job is running or already complete are: 'RUNNING'|'SUCCEEDED'|'FAILED'.

      So the status options that this will wait for are the transitions from:
      'SUBMITTED'>'PENDING'>'RUNNABLE'>'STARTING'>'RUNNING'|'SUCCEEDED'|'FAILED'

      The completed status options are included for cases where the status
      changes too quickly for polling to detect a RUNNING status that moves
      quickly from STARTING to RUNNING to completed (often a failure).

      :param job_id: a Batch job ID

      :param delay: a delay before polling for job status

      :raises: AirflowException


   .. py:method:: poll_for_job_complete(job_id, delay = None)

      Poll for job completion.

      The status that indicates job completion are: 'SUCCEEDED'|'FAILED'.

      So the status options that this will wait for are the transitions from:
      'SUBMITTED'>'PENDING'>'RUNNABLE'>'STARTING'>'RUNNING'>'SUCCEEDED'|'FAILED'

      :param job_id: a Batch job ID

      :param delay: a delay before polling for job status

      :raises: AirflowException


   .. py:method:: poll_job_status(job_id, match_status)

      Poll for job status using an exponential back-off strategy (with max_retries).

      :param job_id: a Batch job ID

      :param match_status: a list of job status to match; the Batch job status are:
          'SUBMITTED'|'PENDING'|'RUNNABLE'|'STARTING'|'RUNNING'|'SUCCEEDED'|'FAILED'


      :raises: AirflowException


   .. py:method:: get_job_description(job_id)

      Get job description (using status_retries).

      :param job_id: a Batch job ID

      :return: an API response for describe jobs

      :raises: AirflowException


   .. py:method:: parse_job_description(job_id, response)
      :staticmethod:

      Parse job description to extract description for job_id.

      :param job_id: a Batch job ID

      :param response: an API response for describe jobs

      :return: an API response to describe job_id

      :raises: AirflowException


   .. py:method:: get_job_awslogs_info(job_id)


   .. py:method:: get_job_all_awslogs_info(job_id)

      Parse job description to extract AWS CloudWatch information.

      :param job_id: AWS Batch Job ID


   .. py:method:: add_jitter(delay, width = 1, minima = 0)
      :staticmethod:

      Use delay +/- width for random jitter.

      Adding jitter to status polling can help to avoid
      AWS Batch API limits for monitoring Batch jobs with
      a high concurrency in Airflow tasks.

      :param delay: number of seconds to pause;
          delay is assumed to be a positive number

      :param width: delay +/- width for random jitter;
          width is assumed to be a positive number

      :param minima: minimum delay allowed;
          minima is assumed to be a non-negative number

      :return: uniform(delay - width, delay + width) jitter
          and it is a non-negative number


   .. py:method:: delay(delay = None)
      :staticmethod:

      Pause execution for ``delay`` seconds.

      :param delay: a delay to pause execution using ``time.sleep(delay)``;
          a small 1 second jitter is applied to the delay.

      .. note::
          This method uses a default random delay, i.e.
          ``random.uniform(DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX)``;
          using a random interval helps to avoid AWS API throttle limits
          when many concurrent tasks request job-descriptions.


   .. py:method:: exponential_delay(tries)
      :staticmethod:

      Apply an exponential back-off delay, with random jitter.

      There is a maximum interval of 10 minutes (with random jitter between 3 and 10 minutes).
      This is used in the :py:meth:`.poll_for_job_status` method.

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

      :param tries: Number of tries

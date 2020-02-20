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

"""
A client for AWS batch services

.. seealso::

    - http://boto3.readthedocs.io/en/latest/guide/configuration.html
    - http://boto3.readthedocs.io/en/latest/reference/services/batch.html
    - https://docs.aws.amazon.com/batch/latest/APIReference/Welcome.html
"""

from random import uniform
from time import sleep
from typing import Dict, List, Optional, Union

import botocore.client
import botocore.exceptions
import botocore.waiter

from airflow import AirflowException, LoggingMixin
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.typing_compat import Protocol, runtime_checkable

# Add exceptions to pylint for the boto3 protocol only; ideally the boto3 library could provide
# protocols for all their dynamically generated classes (try to migrate this to a PR on botocore).
# Note that the use of invalid-name parameters should be restricted to the boto3 mappings only;
# all the Airflow wrappers of boto3 clients should not adopt invalid-names to match boto3.
# pylint: disable=invalid-name, unused-argument


@runtime_checkable
class AwsBatchProtocol(Protocol):
    """
    A structured Protocol for ``boto3.client('batch') -> botocore.client.Batch``.
    This is used for type hints on :py:meth:`.AwsBatchClient.client`; it covers
    only the subset of client methods required.

    .. seealso::

        - https://mypy.readthedocs.io/en/latest/protocols.html
        - http://boto3.readthedocs.io/en/latest/reference/services/batch.html
    """

    def describe_jobs(self, jobs: List[str]) -> Dict:
        """
        Get job descriptions from AWS batch

        :param jobs: a list of JobId to describe
        :type jobs: List[str]

        :return: an API response to describe jobs
        :rtype: Dict
        """
        ...

    def get_waiter(self, waiterName: str) -> botocore.waiter.Waiter:
        """
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
        """
        ...

    def submit_job(
        self,
        jobName: str,
        jobQueue: str,
        jobDefinition: str,
        arrayProperties: Dict,
        parameters: Dict,
        containerOverrides: Dict,
    ) -> Dict:
        """
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
        """
        ...

    def terminate_job(self, jobId: str, reason: str) -> Dict:
        """
        Terminate a batch job

        :param jobId: a job ID to terminate
        :type jobId: str

        :param reason: a reason to terminate job ID
        :type reason: str

        :return: an API response
        :rtype: Dict
        """
        ...


# Note that the use of invalid-name parameters should be restricted to the boto3 mappings only;
# all the Airflow wrappers of boto3 clients should not adopt invalid-names to match boto3.
# pylint: enable=invalid-name, unused-argument


class AwsBatchClient(LoggingMixin):
    """
    A client for AWS batch services.

    :param max_retries: exponential back-off retries, 4200 = 48 hours;
        polling is only used when waiters is None
    :type max_retries: Optional[int]

    :param status_retries: number of HTTP retries to get job status, 10;
        polling is only used when waiters is None
    :type status_retries: Optional[int]

    :param aws_conn_id: connection id of AWS credentials / region name. If None,
        credential boto3 strategy will be used
        (http://boto3.readthedocs.io/en/latest/guide/configuration.html).
    :type aws_conn_id: Optional[str]

    :param region_name: region name to use in AWS client.
        Override the region_name in connection (if provided)
    :type region_name: Optional[str]

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
    """

    MAX_RETRIES = 4200
    STATUS_RETRIES = 10

    # delays are in seconds
    DEFAULT_DELAY_MIN = 1
    DEFAULT_DELAY_MAX = 10

    def __init__(
        self,
        max_retries: Optional[int] = None,
        status_retries: Optional[int] = None,
        aws_conn_id: Optional[str] = None,
        region_name: Optional[str] = None,
    ):
        super().__init__()
        self.max_retries = max_retries or self.MAX_RETRIES
        self.status_retries = status_retries or self.STATUS_RETRIES
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self._hook = None  # type: Union[AwsBaseHook, None]
        self._client = None  # type: Union[AwsBatchProtocol, botocore.client.BaseClient, None]

    @property
    def hook(self) -> AwsBaseHook:
        """
        An AWS API connection manager (wraps boto3)

        :return: the connected hook to AWS
        :rtype: AwsBaseHook
        """
        if self._hook is None:
            self._hook = AwsBaseHook(aws_conn_id=self.aws_conn_id)
        return self._hook

    @property
    def client(self) -> Union[AwsBatchProtocol, botocore.client.BaseClient]:
        """
        An AWS API client for batch services, like ``boto3.client('batch')``

        :return: a boto3 'batch' client for the ``.region_name``
        :rtype: Union[AwsBatchProtocol, botocore.client.BaseClient]
        """
        if self._client is None:
            self._client = self.hook.get_client_type("batch", region_name=self.region_name)
        return self._client

    def terminate_job(self, job_id: str, reason: str) -> Dict:
        """
        Terminate a batch job

        :param job_id: a job ID to terminate
        :type job_id: str

        :param reason: a reason to terminate job ID
        :type reason: str

        :return: an API response
        :rtype: Dict
        """
        response = self.client.terminate_job(jobId=job_id, reason=reason)
        self.log.info(response)
        return response

    def check_job_success(self, job_id: str) -> bool:
        """
        Check the final status of the batch job; return True if the job
        'SUCCEEDED', else raise an AirflowException

        :param job_id: a batch job ID
        :type job_id: str

        :rtype: bool

        :raises: AirflowException
        """
        job = self.get_job_description(job_id)
        job_status = job.get("status")

        if job_status == "SUCCEEDED":
            self.log.info("AWS batch job (%s) succeeded: %s", job_id, job)
            return True

        if job_status == "FAILED":
            raise AirflowException("AWS Batch job ({}) failed: {}".format(job_id, job))

        if job_status in ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"]:
            raise AirflowException("AWS Batch job ({}) is not complete: {}".format(job_id, job))

        raise AirflowException("AWS Batch job ({}) has unknown status: {}".format(job_id, job))

    def wait_for_job(self, job_id: str, delay: Union[int, float, None] = None):
        """
        Wait for batch job to complete

        :param job_id: a batch job ID
        :type job_id: str

        :param delay: a delay before polling for job status
        :type delay: Optional[Union[int, float]]

        :raises: AirflowException
        """
        self.delay(delay)
        self.poll_for_job_running(job_id, delay)
        self.poll_for_job_complete(job_id, delay)
        self.log.info("AWS Batch job (%s) has completed", job_id)

    def poll_for_job_running(self, job_id: str, delay: Union[int, float, None] = None):
        """
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
        """
        self.delay(delay)
        running_status = ["RUNNING", "SUCCEEDED", "FAILED"]
        self.poll_job_status(job_id, running_status)

    def poll_for_job_complete(self, job_id: str, delay: Union[int, float, None] = None):
        """
        Poll for job completion. The status that indicates job completion
        are: 'SUCCEEDED'|'FAILED'.

        So the status options that this will wait for are the transitions from:
        'SUBMITTED'>'PENDING'>'RUNNABLE'>'STARTING'>'RUNNING'>'SUCCEEDED'|'FAILED'

        :param job_id: a batch job ID
        :type job_id: str

        :param delay: a delay before polling for job status
        :type delay: Optional[Union[int, float]]

        :raises: AirflowException
        """
        self.delay(delay)
        complete_status = ["SUCCEEDED", "FAILED"]
        self.poll_job_status(job_id, complete_status)

    def poll_job_status(self, job_id: str, match_status: List[str]) -> bool:
        """
        Poll for job status using an exponential back-off strategy (with max_retries).

        :param job_id: a batch job ID
        :type job_id: str

        :param match_status: a list of job status to match; the batch job status are:
            'SUBMITTED'|'PENDING'|'RUNNABLE'|'STARTING'|'RUNNING'|'SUCCEEDED'|'FAILED'
        :type match_status: List[str]

        :rtype: bool

        :raises: AirflowException
        """
        retries = 0
        while True:

            job = self.get_job_description(job_id)
            job_status = job.get("status")
            self.log.info(
                "AWS Batch job (%s) check status (%s) in %s", job_id, job_status, match_status,
            )

            if job_status in match_status:
                return True

            if retries >= self.max_retries:
                raise AirflowException(
                    "AWS Batch job ({}) status checks exceed max_retries".format(job_id)
                )

            retries += 1
            pause = self.exponential_delay(retries)
            self.log.info(
                "AWS Batch job (%s) status check (%d of %d) in the next %.2f seconds",
                job_id,
                retries,
                self.max_retries,
                pause,
            )
            self.delay(pause)

    def get_job_description(self, job_id: str) -> Dict:
        """
        Get job description (using status_retries).

        :param job_id: a batch job ID
        :type job_id: str

        :return: an API response for describe jobs
        :rtype: Dict

        :raises: AirflowException
        """
        retries = 0
        while True:
            try:
                response = self.client.describe_jobs(jobs=[job_id])
                return self.parse_job_description(job_id, response)

            except botocore.exceptions.ClientError as err:
                error = err.response.get("Error", {})
                if error.get("Code") == "TooManyRequestsException":
                    pass  # allow it to retry, if possible
                else:
                    raise AirflowException(
                        "AWS Batch job ({}) description error: {}".format(job_id, err)
                    )

            retries += 1
            if retries >= self.status_retries:
                raise AirflowException(
                    "AWS Batch job ({}) description error: exceeded "
                    "status_retries ({})".format(job_id, self.status_retries)
                )

            pause = self.exponential_delay(retries)
            self.log.info(
                "AWS Batch job (%s) description retry (%d of %d) in the next %.2f seconds",
                job_id,
                retries,
                self.status_retries,
                pause,
            )
            self.delay(pause)

    @staticmethod
    def parse_job_description(job_id: str, response: Dict) -> Dict:
        """
        Parse job description to extract description for job_id

        :param job_id: a batch job ID
        :type job_id: str

        :param response: an API response for describe jobs
        :type response: Dict

        :return: an API response to describe job_id
        :rtype: Dict

        :raises: AirflowException
        """
        jobs = response.get("jobs", [])
        matching_jobs = [job for job in jobs if job.get("jobId") == job_id]
        if len(matching_jobs) != 1:
            raise AirflowException(
                "AWS Batch job ({}) description error: response: {}".format(job_id, response)
            )

        return matching_jobs[0]

    @staticmethod
    def add_jitter(
        delay: Union[int, float], width: Union[int, float] = 1, minima: Union[int, float] = 0
    ) -> float:
        """
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
        """
        delay = abs(delay)
        width = abs(width)
        minima = abs(minima)
        lower = max(minima, delay - width)
        upper = delay + width
        return uniform(lower, upper)

    @staticmethod
    def delay(delay: Union[int, float, None] = None):
        """
        Pause execution for ``delay`` seconds.

        :param delay: a delay to pause execution using ``time.sleep(delay)``;
            a small 1 second jitter is applied to the delay.
        :type delay: Optional[Union[int, float]]

        .. note::
            This method uses a default random delay, i.e.
            ``random.uniform(DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX)``;
            using a random interval helps to avoid AWS API throttle limits
            when many concurrent tasks request job-descriptions.
        """
        if delay is None:
            delay = uniform(AwsBatchClient.DEFAULT_DELAY_MIN, AwsBatchClient.DEFAULT_DELAY_MAX)
        else:
            delay = AwsBatchClient.add_jitter(delay)
        sleep(delay)

    @staticmethod
    def exponential_delay(tries: int) -> float:
        """
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
        """
        max_interval = 600.0  # results in 3 to 10 minute delay
        delay = 1 + pow(tries * 0.6, 2)
        delay = min(max_interval, delay)
        return uniform(delay / 3, delay)

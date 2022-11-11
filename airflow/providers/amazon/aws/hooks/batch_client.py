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
A client for AWS Batch services

.. seealso::

    - https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html
    - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch.html
    - https://docs.aws.amazon.com/batch/latest/APIReference/Welcome.html
"""
from __future__ import annotations

from random import uniform
from time import sleep

import botocore.client
import botocore.exceptions
import botocore.waiter

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.typing_compat import Protocol, runtime_checkable


@runtime_checkable
class BatchProtocol(Protocol):
    """
    A structured Protocol for ``boto3.client('batch') -> botocore.client.Batch``.
    This is used for type hints on :py:meth:`.BatchClient.client`; it covers
    only the subset of client methods required.

    .. seealso::

        - https://mypy.readthedocs.io/en/latest/protocols.html
        - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch.html
    """

    def describe_jobs(self, jobs: list[str]) -> dict:
        """
        Get job descriptions from AWS Batch

        :param jobs: a list of JobId to describe

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

        :return: a waiter object for the named AWS Batch service
        :rtype: botocore.waiter.Waiter

        .. note::
            AWS Batch might not have any waiters (until botocore PR-1307 is released).

            .. code-block:: python

                import boto3

                boto3.client("batch").waiter_names == []

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
        arrayProperties: dict,
        parameters: dict,
        containerOverrides: dict,
        tags: dict,
    ) -> dict:
        """
        Submit a Batch job

        :param jobName: the name for the AWS Batch job

        :param jobQueue: the queue name on AWS Batch

        :param jobDefinition: the job definition name on AWS Batch

        :param arrayProperties: the same parameter that boto3 will receive

        :param parameters: the same parameter that boto3 will receive

        :param containerOverrides: the same parameter that boto3 will receive

        :param tags: the same parameter that boto3 will receive

        :return: an API response
        :rtype: Dict
        """
        ...

    def terminate_job(self, jobId: str, reason: str) -> dict:
        """
        Terminate a Batch job

        :param jobId: a job ID to terminate

        :param reason: a reason to terminate job ID

        :return: an API response
        :rtype: Dict
        """
        ...


# Note that the use of invalid-name parameters should be restricted to the boto3 mappings only;
# all the Airflow wrappers of boto3 clients should not adopt invalid-names to match boto3.


class BatchClientHook(AwsBaseHook):
    """
    A client for AWS Batch services.

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

    FAILURE_STATE = "FAILED"
    SUCCESS_STATE = "SUCCEEDED"
    RUNNING_STATE = "RUNNING"
    INTERMEDIATE_STATES = (
        "SUBMITTED",
        "PENDING",
        "RUNNABLE",
        "STARTING",
        RUNNING_STATE,
    )

    COMPUTE_ENVIRONMENT_TERMINAL_STATUS = ("VALID", "DELETED")
    COMPUTE_ENVIRONMENT_INTERMEDIATE_STATUS = ("CREATING", "UPDATING", "DELETING")

    JOB_QUEUE_TERMINAL_STATUS = ("VALID", "DELETED")
    JOB_QUEUE_INTERMEDIATE_STATUS = ("CREATING", "UPDATING", "DELETING")

    def __init__(
        self, *args, max_retries: int | None = None, status_retries: int | None = None, **kwargs
    ) -> None:
        # https://github.com/python/mypy/issues/6799 hence type: ignore
        super().__init__(client_type="batch", *args, **kwargs)  # type: ignore
        self.max_retries = max_retries or self.MAX_RETRIES
        self.status_retries = status_retries or self.STATUS_RETRIES

    @property
    def client(self) -> BatchProtocol | botocore.client.BaseClient:
        """
        An AWS API client for Batch services.

        :return: a boto3 'batch' client for the ``.region_name``
        :rtype: Union[BatchProtocol, botocore.client.BaseClient]
        """
        return self.conn

    def terminate_job(self, job_id: str, reason: str) -> dict:
        """
        Terminate a Batch job

        :param job_id: a job ID to terminate

        :param reason: a reason to terminate job ID

        :return: an API response
        :rtype: Dict
        """
        response = self.get_conn().terminate_job(jobId=job_id, reason=reason)
        self.log.info(response)
        return response

    def check_job_success(self, job_id: str) -> bool:
        """
        Check the final status of the Batch job; return True if the job
        'SUCCEEDED', else raise an AirflowException

        :param job_id: a Batch job ID

        :rtype: bool

        :raises: AirflowException
        """
        job = self.get_job_description(job_id)
        job_status = job.get("status")

        if job_status == self.SUCCESS_STATE:
            self.log.info("AWS Batch job (%s) succeeded: %s", job_id, job)
            return True

        if job_status == self.FAILURE_STATE:
            raise AirflowException(f"AWS Batch job ({job_id}) failed: {job}")

        if job_status in self.INTERMEDIATE_STATES:
            raise AirflowException(f"AWS Batch job ({job_id}) is not complete: {job}")

        raise AirflowException(f"AWS Batch job ({job_id}) has unknown status: {job}")

    def wait_for_job(self, job_id: str, delay: int | float | None = None) -> None:
        """
        Wait for Batch job to complete

        :param job_id: a Batch job ID

        :param delay: a delay before polling for job status

        :raises: AirflowException
        """
        self.delay(delay)
        self.poll_for_job_running(job_id, delay)
        self.poll_for_job_complete(job_id, delay)
        self.log.info("AWS Batch job (%s) has completed", job_id)

    def poll_for_job_running(self, job_id: str, delay: int | float | None = None) -> None:
        """
        Poll for job running. The status that indicates a job is running or
        already complete are: 'RUNNING'|'SUCCEEDED'|'FAILED'.

        So the status options that this will wait for are the transitions from:
        'SUBMITTED'>'PENDING'>'RUNNABLE'>'STARTING'>'RUNNING'|'SUCCEEDED'|'FAILED'

        The completed status options are included for cases where the status
        changes too quickly for polling to detect a RUNNING status that moves
        quickly from STARTING to RUNNING to completed (often a failure).

        :param job_id: a Batch job ID

        :param delay: a delay before polling for job status

        :raises: AirflowException
        """
        self.delay(delay)
        running_status = [self.RUNNING_STATE, self.SUCCESS_STATE, self.FAILURE_STATE]
        self.poll_job_status(job_id, running_status)

    def poll_for_job_complete(self, job_id: str, delay: int | float | None = None) -> None:
        """
        Poll for job completion. The status that indicates job completion
        are: 'SUCCEEDED'|'FAILED'.

        So the status options that this will wait for are the transitions from:
        'SUBMITTED'>'PENDING'>'RUNNABLE'>'STARTING'>'RUNNING'>'SUCCEEDED'|'FAILED'

        :param job_id: a Batch job ID

        :param delay: a delay before polling for job status

        :raises: AirflowException
        """
        self.delay(delay)
        complete_status = [self.SUCCESS_STATE, self.FAILURE_STATE]
        self.poll_job_status(job_id, complete_status)

    def poll_job_status(self, job_id: str, match_status: list[str]) -> bool:
        """
        Poll for job status using an exponential back-off strategy (with max_retries).

        :param job_id: a Batch job ID

        :param match_status: a list of job status to match; the Batch job status are:
            'SUBMITTED'|'PENDING'|'RUNNABLE'|'STARTING'|'RUNNING'|'SUCCEEDED'|'FAILED'

        :rtype: bool

        :raises: AirflowException
        """
        retries = 0
        while True:

            job = self.get_job_description(job_id)
            job_status = job.get("status")
            self.log.info(
                "AWS Batch job (%s) check status (%s) in %s",
                job_id,
                job_status,
                match_status,
            )

            if job_status in match_status:
                return True

            if retries >= self.max_retries:
                raise AirflowException(f"AWS Batch job ({job_id}) status checks exceed max_retries")

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

    def get_job_description(self, job_id: str) -> dict:
        """
        Get job description (using status_retries).

        :param job_id: a Batch job ID

        :return: an API response for describe jobs
        :rtype: Dict

        :raises: AirflowException
        """
        retries = 0
        while True:
            try:
                response = self.get_conn().describe_jobs(jobs=[job_id])
                return self.parse_job_description(job_id, response)

            except botocore.exceptions.ClientError as err:
                error = err.response.get("Error", {})
                if error.get("Code") == "TooManyRequestsException":
                    pass  # allow it to retry, if possible
                else:
                    raise AirflowException(f"AWS Batch job ({job_id}) description error: {err}")

            retries += 1
            if retries >= self.status_retries:
                raise AirflowException(
                    f"AWS Batch job ({job_id}) description error: exceeded status_retries "
                    f"({self.status_retries})"
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
    def parse_job_description(job_id: str, response: dict) -> dict:
        """
        Parse job description to extract description for job_id

        :param job_id: a Batch job ID

        :param response: an API response for describe jobs

        :return: an API response to describe job_id
        :rtype: Dict

        :raises: AirflowException
        """
        jobs = response.get("jobs", [])
        matching_jobs = [job for job in jobs if job.get("jobId") == job_id]
        if len(matching_jobs) != 1:
            raise AirflowException(f"AWS Batch job ({job_id}) description error: response: {response}")

        return matching_jobs[0]

    def get_job_awslogs_info(self, job_id: str) -> dict[str, str] | None:
        """
        Parse job description to extract AWS CloudWatch information.

        :param job_id: AWS Batch Job ID
        """
        job_container_desc = self.get_job_description(job_id=job_id).get("container", {})
        log_configuration = job_container_desc.get("logConfiguration", {})

        # In case if user select other "logDriver" rather than "awslogs"
        # than CloudWatch logging should be disabled.
        # If user not specify anything than expected that "awslogs" will use
        # with default settings:
        #   awslogs-group = /aws/batch/job
        #   awslogs-region = `same as AWS Batch Job region`
        log_driver = log_configuration.get("logDriver", "awslogs")
        if log_driver != "awslogs":
            self.log.warning(
                "AWS Batch job (%s) uses logDriver (%s). AWS CloudWatch logging disabled.", job_id, log_driver
            )
            return None

        awslogs_stream_name = job_container_desc.get("logStreamName")
        if not awslogs_stream_name:
            # In case of call this method on very early stage of running AWS Batch
            # there is possibility than AWS CloudWatch Stream Name not exists yet.
            # AWS CloudWatch Stream Name also not created in case of misconfiguration.
            self.log.warning("AWS Batch job (%s) doesn't create AWS CloudWatch Stream.", job_id)
            return None

        # Try to get user-defined log configuration options
        log_options = log_configuration.get("options", {})

        return {
            "awslogs_stream_name": awslogs_stream_name,
            "awslogs_group": log_options.get("awslogs-group", "/aws/batch/job"),
            "awslogs_region": log_options.get("awslogs-region", self.conn_region_name),
        }

    @staticmethod
    def add_jitter(delay: int | float, width: int | float = 1, minima: int | float = 0) -> float:
        """
        Use delay +/- width for random jitter

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
        :rtype: float
        """
        delay = abs(delay)
        width = abs(width)
        minima = abs(minima)
        lower = max(minima, delay - width)
        upper = delay + width
        return uniform(lower, upper)

    @staticmethod
    def delay(delay: int | float | None = None) -> None:
        """
        Pause execution for ``delay`` seconds.

        :param delay: a delay to pause execution using ``time.sleep(delay)``;
            a small 1 second jitter is applied to the delay.

        .. note::
            This method uses a default random delay, i.e.
            ``random.uniform(DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX)``;
            using a random interval helps to avoid AWS API throttle limits
            when many concurrent tasks request job-descriptions.
        """
        if delay is None:
            delay = uniform(BatchClientHook.DEFAULT_DELAY_MIN, BatchClientHook.DEFAULT_DELAY_MAX)
        else:
            delay = BatchClientHook.add_jitter(delay)
        sleep(delay)

    @staticmethod
    def exponential_delay(tries: int) -> float:
        """
        An exponential back-off delay, with random jitter.  There is a maximum
        interval of 10 minutes (with random jitter between 3 and 10 minutes).
        This is used in the :py:meth:`.poll_for_job_status` method.

        :param tries: Number of tries

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

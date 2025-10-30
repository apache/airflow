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
A client for AWS Batch services.

.. seealso::

    - https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html
    - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch.html
    - https://docs.aws.amazon.com/batch/latest/APIReference/Welcome.html
"""

from __future__ import annotations

import itertools
import random
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Protocol, runtime_checkable

import botocore.client
import botocore.exceptions
import botocore.waiter

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.utils.task_log_fetcher import AwsTaskLogFetcher


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
        Get job descriptions from AWS Batch.

        :param jobs: a list of JobId to describe

        :return: an API response to describe jobs
        """
        ...

    def get_waiter(self, waiterName: str) -> botocore.waiter.Waiter:
        """
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
        ecsPropertiesOverride: dict,
        eksPropertiesOverride: dict,
        tags: dict,
    ) -> dict:
        """
        Submit a Batch job.

        :param jobName: the name for the AWS Batch job

        :param jobQueue: the queue name on AWS Batch

        :param jobDefinition: the job definition name on AWS Batch

        :param arrayProperties: the same parameter that boto3 will receive

        :param parameters: the same parameter that boto3 will receive

        :param containerOverrides: the same parameter that boto3 will receive

        :param ecsPropertiesOverride: the same parameter that boto3 will receive

        :param eksPropertiesOverride: the same parameter that boto3 will receive

        :param tags: the same parameter that boto3 will receive

        :return: an API response
        """
        ...

    def terminate_job(self, jobId: str, reason: str) -> dict:
        """
        Terminate a Batch job.

        :param jobId: a job ID to terminate

        :param reason: a reason to terminate job ID

        :return: an API response
        """
        ...

    def create_compute_environment(self, **kwargs) -> dict:
        """
        Create an AWS Batch compute environment.

        :param kwargs: Arguments for boto3 create_compute_environment

        .. seealso::
            - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/create_compute_environment.html
        """
        ...


# Note that the use of invalid-name parameters should be restricted to the boto3 mappings only;
# all the Airflow wrappers of boto3 clients should not adopt invalid-names to match boto3.


class BatchClientHook(AwsBaseHook):
    """
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
        """
        return self.conn

    def terminate_job(self, job_id: str, reason: str) -> dict:
        """
        Terminate a Batch job.

        :param job_id: a job ID to terminate

        :param reason: a reason to terminate job ID

        :return: an API response
        """
        response = self.get_conn().terminate_job(jobId=job_id, reason=reason)
        self.log.info(response)
        return response

    def check_job_success(self, job_id: str) -> bool:
        """
        Check the final status of the Batch job.

        Return True if the job 'SUCCEEDED', else raise an AirflowException.

        :param job_id: a Batch job ID

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

    def wait_for_job(
        self,
        job_id: str,
        delay: int | float | None = None,
        get_batch_log_fetcher: Callable[[str], AwsTaskLogFetcher | None] | None = None,
    ) -> None:
        """
        Wait for Batch job to complete.

        :param job_id: a Batch job ID

        :param delay: a delay before polling for job status

        :param get_batch_log_fetcher : a method that returns batch_log_fetcher

        :raises: AirflowException
        """
        self.delay(delay)
        self.poll_for_job_running(job_id, delay)
        batch_log_fetcher = None
        try:
            if get_batch_log_fetcher:
                batch_log_fetcher = get_batch_log_fetcher(job_id)
                if batch_log_fetcher:
                    batch_log_fetcher.start()
            self.poll_for_job_complete(job_id, delay)
        finally:
            if batch_log_fetcher:
                batch_log_fetcher.stop()
                batch_log_fetcher.join()
        self.log.info("AWS Batch job (%s) has completed", job_id)

    def poll_for_job_running(self, job_id: str, delay: int | float | None = None) -> None:
        """
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
        """
        self.delay(delay)
        running_status = [self.RUNNING_STATE, self.SUCCESS_STATE, self.FAILURE_STATE]
        self.poll_job_status(job_id, running_status)

    def poll_for_job_complete(self, job_id: str, delay: int | float | None = None) -> None:
        """
        Poll for job completion.

        The status that indicates job completion are: 'SUCCEEDED'|'FAILED'.

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


        :raises: AirflowException
        """
        for retries in range(1 + self.max_retries):
            if retries:
                pause = self.exponential_delay(retries)
                self.log.info(
                    "AWS Batch job (%s) status check (%d of %d) in the next %.2f seconds",
                    job_id,
                    retries,
                    self.max_retries,
                    pause,
                )
                self.delay(pause)

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
        raise AirflowException(f"AWS Batch job ({job_id}) status checks exceed max_retries")

    def get_job_description(self, job_id: str) -> dict:
        """
        Get job description (using status_retries).

        :param job_id: a Batch job ID

        :return: an API response for describe jobs

        :raises: AirflowException
        """
        for retries in range(self.status_retries):
            if retries:
                pause = self.exponential_delay(retries)
                self.log.info(
                    "AWS Batch job (%s) description retry (%d of %d) in the next %.2f seconds",
                    job_id,
                    retries,
                    self.status_retries,
                    pause,
                )
                self.delay(pause)

            try:
                response = self.get_conn().describe_jobs(jobs=[job_id])
                return self.parse_job_description(job_id, response)
            except AirflowException as err:
                self.log.warning(err)
            except botocore.exceptions.ClientError as err:
                # Allow it to retry in case of exceeded quota limit of requests to AWS API
                if err.response.get("Error", {}).get("Code") != "TooManyRequestsException":
                    raise
                self.log.warning(
                    "Ignored TooManyRequestsException error, original message: %r. "
                    "Please consider to setup retries mode in boto3, "
                    "check Amazon Provider AWS Connection documentation for more details.",
                    str(err),
                )
        raise AirflowException(
            f"AWS Batch job ({job_id}) description error: exceeded status_retries ({self.status_retries})"
        )

    @staticmethod
    def parse_job_description(job_id: str, response: dict) -> dict:
        """
        Parse job description to extract description for job_id.

        :param job_id: a Batch job ID

        :param response: an API response for describe jobs

        :return: an API response to describe job_id

        :raises: AirflowException
        """
        jobs = response.get("jobs", [])
        matching_jobs = [job for job in jobs if job.get("jobId") == job_id]
        if len(matching_jobs) != 1:
            raise AirflowException(f"AWS Batch job ({job_id}) description error: response: {response}")

        return matching_jobs[0]

    def get_job_awslogs_info(self, job_id: str) -> dict[str, str] | None:
        all_info = self.get_job_all_awslogs_info(job_id)
        if not all_info:
            return None
        if len(all_info) > 1:
            self.log.warning(
                "AWS Batch job (%s) has more than one log stream, only returning the first one.", job_id
            )
        return all_info[0]

    def get_job_all_awslogs_info(self, job_id: str) -> list[dict[str, str]]:
        """
        Parse job description to extract AWS CloudWatch information.

        :param job_id: AWS Batch Job ID
        """
        job_desc = self.get_job_description(job_id=job_id)

        job_node_properties = job_desc.get("nodeProperties", {})
        job_container_desc = job_desc.get("container", {})

        if job_node_properties:
            # one log config per node
            log_configs = [
                p.get("container", {}).get("logConfiguration", {})
                for p in job_node_properties.get("nodeRangeProperties", {})
            ]
            # one stream name per attempt
            stream_names = [a.get("container", {}).get("logStreamName") for a in job_desc.get("attempts", [])]
        elif job_container_desc:
            log_configs = [job_container_desc.get("logConfiguration", {})]
            stream_name = job_container_desc.get("logStreamName")
            stream_names = [stream_name] if stream_name is not None else []
        else:
            raise AirflowException(
                f"AWS Batch job ({job_id}) is not a supported job type. "
                "Supported job types: container, array, multinode."
            )

        # If the user selected another logDriver than "awslogs", then CloudWatch logging is disabled.
        if any(c.get("logDriver", "awslogs") != "awslogs" for c in log_configs):
            self.log.warning(
                "AWS Batch job (%s) uses non-aws log drivers. AWS CloudWatch logging disabled.", job_id
            )
            return []

        if not stream_names:
            # If this method is called very early after starting the AWS Batch job,
            # there is a possibility that the AWS CloudWatch Stream Name would not exist yet.
            # This can also happen in case of misconfiguration.
            self.log.warning("AWS Batch job (%s) doesn't have any AWS CloudWatch Stream.", job_id)
            return []

        # Try to get user-defined log configuration options
        log_options = [c.get("options", {}) for c in log_configs]

        # cross stream names with options (i.e. attempts X nodes) to generate all log infos
        result = []
        for stream, option in itertools.product(stream_names, log_options):
            result.append(
                {
                    "awslogs_stream_name": stream,
                    # If the user did not specify anything, the default settings are:
                    #   awslogs-group = /aws/batch/job
                    #   awslogs-region = `same as AWS Batch Job region`
                    "awslogs_group": option.get("awslogs-group", "/aws/batch/job"),
                    "awslogs_region": option.get("awslogs-region", self.conn_region_name),
                }
            )
        return result

    @staticmethod
    def add_jitter(delay: int | float, width: int | float = 1, minima: int | float = 0) -> float:
        """
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
        """
        delay = abs(delay)
        width = abs(width)
        minima = abs(minima)
        lower = max(minima, delay - width)
        upper = delay + width
        return random.uniform(lower, upper)

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
            delay = random.uniform(BatchClientHook.DEFAULT_DELAY_MIN, BatchClientHook.DEFAULT_DELAY_MAX)
        else:
            delay = BatchClientHook.add_jitter(delay)
        time.sleep(delay)

    @staticmethod
    def exponential_delay(tries: int) -> float:
        """
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
        """
        max_interval = 600.0  # results in 3 to 10 minute delay
        delay = 1 + pow(tries * 0.6, 2)
        delay = min(max_interval, delay)
        return random.uniform(delay / 3, delay)

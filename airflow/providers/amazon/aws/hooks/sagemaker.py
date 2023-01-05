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
from __future__ import annotations

import collections
import os
import re
import tarfile
import tempfile
import time
import warnings
from collections import Counter
from datetime import datetime
from functools import partial
from typing import Any, Callable, Generator, cast

from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils import timezone


class LogState:
    """
    Enum-style class holding all possible states of CloudWatch log streams.
    https://sagemaker.readthedocs.io/en/stable/session.html#sagemaker.session.LogState
    """

    STARTING = 1
    WAIT_IN_PROGRESS = 2
    TAILING = 3
    JOB_COMPLETE = 4
    COMPLETE = 5


# Position is a tuple that includes the last read timestamp and the number of items that were read
# at that time. This is used to figure out which event to start with on the next read.
Position = collections.namedtuple("Position", ["timestamp", "skip"])


def argmin(arr, f: Callable) -> int | None:
    """Return the index, i, in arr that minimizes f(arr[i])"""
    min_value = None
    min_idx = None
    for idx, item in enumerate(arr):
        if item is not None:
            if min_value is None or f(item) < min_value:
                min_value = f(item)
                min_idx = idx
    return min_idx


def secondary_training_status_changed(current_job_description: dict, prev_job_description: dict) -> bool:
    """
    Returns true if training job's secondary status message has changed.

    :param current_job_description: Current job description, returned from DescribeTrainingJob call.
    :param prev_job_description: Previous job description, returned from DescribeTrainingJob call.

    :return: Whether the secondary status message of a training job changed or not.
    """
    current_secondary_status_transitions = current_job_description.get("SecondaryStatusTransitions")
    if current_secondary_status_transitions is None or len(current_secondary_status_transitions) == 0:
        return False

    prev_job_secondary_status_transitions = (
        prev_job_description.get("SecondaryStatusTransitions") if prev_job_description is not None else None
    )

    last_message = (
        prev_job_secondary_status_transitions[-1]["StatusMessage"]
        if prev_job_secondary_status_transitions is not None
        and len(prev_job_secondary_status_transitions) > 0
        else ""
    )

    message = current_job_description["SecondaryStatusTransitions"][-1]["StatusMessage"]

    return message != last_message


def secondary_training_status_message(
    job_description: dict[str, list[Any]], prev_description: dict | None
) -> str:
    """
    Returns a string contains start time and the secondary training job status message.

    :param job_description: Returned response from DescribeTrainingJob call
    :param prev_description: Previous job description from DescribeTrainingJob call

    :return: Job status string to be printed.
    """
    current_transitions = job_description.get("SecondaryStatusTransitions")
    if current_transitions is None or len(current_transitions) == 0:
        return ""

    prev_transitions_num = 0
    if prev_description is not None:
        if prev_description.get("SecondaryStatusTransitions") is not None:
            prev_transitions_num = len(prev_description["SecondaryStatusTransitions"])

    transitions_to_print = (
        current_transitions[-1:]
        if len(current_transitions) == prev_transitions_num
        else current_transitions[prev_transitions_num - len(current_transitions) :]
    )

    status_strs = []
    for transition in transitions_to_print:
        message = transition["StatusMessage"]
        time_str = timezone.convert_to_utc(cast(datetime, job_description["LastModifiedTime"])).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        status_strs.append(f"{time_str} {transition['Status']} - {message}")

    return "\n".join(status_strs)


class SageMakerHook(AwsBaseHook):
    """
    Interact with Amazon SageMaker.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    non_terminal_states = {"InProgress", "Stopping"}
    endpoint_non_terminal_states = {"Creating", "Updating", "SystemUpdating", "RollingBack", "Deleting"}
    pipeline_non_terminal_states = {"Executing", "Stopping"}
    failed_states = {"Failed"}

    def __init__(self, *args, **kwargs):
        super().__init__(client_type="sagemaker", *args, **kwargs)
        self.s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        self.logs_hook = AwsLogsHook(aws_conn_id=self.aws_conn_id)

    def tar_and_s3_upload(self, path: str, key: str, bucket: str) -> None:
        """
        Tar the local file or directory and upload to s3

        :param path: local file or directory
        :param key: s3 key
        :param bucket: s3 bucket
        :return: None
        """
        with tempfile.TemporaryFile() as temp_file:
            if os.path.isdir(path):
                files = [os.path.join(path, name) for name in os.listdir(path)]
            else:
                files = [path]
            with tarfile.open(mode="w:gz", fileobj=temp_file) as tar_file:
                for f in files:
                    tar_file.add(f, arcname=os.path.basename(f))
            temp_file.seek(0)
            self.s3_hook.load_file_obj(temp_file, key, bucket, replace=True)

    def configure_s3_resources(self, config: dict) -> None:
        """
        Extract the S3 operations from the configuration and execute them.

        :param config: config of SageMaker operation
        """
        s3_operations = config.pop("S3Operations", None)

        if s3_operations is not None:
            create_bucket_ops = s3_operations.get("S3CreateBucket", [])
            upload_ops = s3_operations.get("S3Upload", [])
            for op in create_bucket_ops:
                self.s3_hook.create_bucket(bucket_name=op["Bucket"])
            for op in upload_ops:
                if op["Tar"]:
                    self.tar_and_s3_upload(op["Path"], op["Key"], op["Bucket"])
                else:
                    self.s3_hook.load_file(op["Path"], op["Key"], op["Bucket"])

    def check_s3_url(self, s3url: str) -> bool:
        """
        Check if an S3 URL exists

        :param s3url: S3 url
        """
        bucket, key = S3Hook.parse_s3_url(s3url)
        if not self.s3_hook.check_for_bucket(bucket_name=bucket):
            raise AirflowException(f"The input S3 Bucket {bucket} does not exist ")
        if (
            key
            and not self.s3_hook.check_for_key(key=key, bucket_name=bucket)
            and not self.s3_hook.check_for_prefix(prefix=key, bucket_name=bucket, delimiter="/")
        ):
            # check if s3 key exists in the case user provides a single file
            # or if s3 prefix exists in the case user provides multiple files in
            # a prefix
            raise AirflowException(
                f"The input S3 Key or Prefix {s3url} does not exist in the Bucket {bucket}"
            )
        return True

    def check_training_config(self, training_config: dict) -> None:
        """
        Check if a training configuration is valid

        :param training_config: training_config
        :return: None
        """
        if "InputDataConfig" in training_config:
            for channel in training_config["InputDataConfig"]:
                if "S3DataSource" in channel["DataSource"]:
                    self.check_s3_url(channel["DataSource"]["S3DataSource"]["S3Uri"])

    def check_tuning_config(self, tuning_config: dict) -> None:
        """
        Check if a tuning configuration is valid

        :param tuning_config: tuning_config
        :return: None
        """
        for channel in tuning_config["TrainingJobDefinition"]["InputDataConfig"]:
            if "S3DataSource" in channel["DataSource"]:
                self.check_s3_url(channel["DataSource"]["S3DataSource"]["S3Uri"])

    def multi_stream_iter(self, log_group: str, streams: list, positions=None) -> Generator:
        """
        Iterate over the available events coming from a set of log streams in a single log group
        interleaving the events from each stream so they're yielded in timestamp order.

        :param log_group: The name of the log group.
        :param streams: A list of the log stream names. The position of the stream in this list is
            the stream number.
        :param positions: A list of pairs of (timestamp, skip) which represents the last record
            read from each stream.
        :return: A tuple of (stream number, cloudwatch log event).
        """
        positions = positions or {s: Position(timestamp=0, skip=0) for s in streams}
        event_iters = [
            self.logs_hook.get_log_events(log_group, s, positions[s].timestamp, positions[s].skip)
            for s in streams
        ]
        events: list[Any | None] = []
        for event_stream in event_iters:
            if not event_stream:
                events.append(None)
                continue
            try:
                events.append(next(event_stream))
            except StopIteration:
                events.append(None)

        while any(events):
            i = argmin(events, lambda x: x["timestamp"] if x else 9999999999) or 0
            yield i, events[i]
            try:
                events[i] = next(event_iters[i])
            except StopIteration:
                events[i] = None

    def create_training_job(
        self,
        config: dict,
        wait_for_completion: bool = True,
        print_log: bool = True,
        check_interval: int = 30,
        max_ingestion_time: int | None = None,
    ):
        """
        Starts a model training job. After training completes, Amazon SageMaker saves
        the resulting model artifacts to an Amazon S3 location that you specify.

        :param config: the config for training
        :param wait_for_completion: if the program should keep running until job finishes
        :param check_interval: the time interval in seconds which the operator
            will check the status of any SageMaker job
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
            SageMaker jobs that run longer than this will fail. Setting this to
            None implies no timeout for any SageMaker job.
        :return: A response to training job creation
        """
        self.check_training_config(config)

        response = self.get_conn().create_training_job(**config)
        if print_log:
            self.check_training_status_with_log(
                config["TrainingJobName"],
                self.non_terminal_states,
                self.failed_states,
                wait_for_completion,
                check_interval,
                max_ingestion_time,
            )
        elif wait_for_completion:
            describe_response = self.check_status(
                config["TrainingJobName"],
                "TrainingJobStatus",
                self.describe_training_job,
                check_interval,
                max_ingestion_time,
            )

            billable_time = (
                describe_response["TrainingEndTime"] - describe_response["TrainingStartTime"]
            ) * describe_response["ResourceConfig"]["InstanceCount"]
            self.log.info("Billable seconds: %d", int(billable_time.total_seconds()) + 1)

        return response

    def create_tuning_job(
        self,
        config: dict,
        wait_for_completion: bool = True,
        check_interval: int = 30,
        max_ingestion_time: int | None = None,
    ):
        """
        Starts a hyperparameter tuning job. A hyperparameter tuning job finds the
        best version of a model by running many training jobs on your dataset using
        the algorithm you choose and values for hyperparameters within ranges that
        you specify. It then chooses the hyperparameter values that result in a model
        that performs the best, as measured by an objective metric that you choose.

        :param config: the config for tuning
        :param wait_for_completion: if the program should keep running until job finishes
        :param check_interval: the time interval in seconds which the operator
            will check the status of any SageMaker job
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
            SageMaker jobs that run longer than this will fail. Setting this to
            None implies no timeout for any SageMaker job.
        :return: A response to tuning job creation
        """
        self.check_tuning_config(config)

        response = self.get_conn().create_hyper_parameter_tuning_job(**config)
        if wait_for_completion:
            self.check_status(
                config["HyperParameterTuningJobName"],
                "HyperParameterTuningJobStatus",
                self.describe_tuning_job,
                check_interval,
                max_ingestion_time,
            )
        return response

    def create_transform_job(
        self,
        config: dict,
        wait_for_completion: bool = True,
        check_interval: int = 30,
        max_ingestion_time: int | None = None,
    ):
        """
        Starts a transform job. A transform job uses a trained model to get inferences
        on a dataset and saves these results to an Amazon S3 location that you specify.

        :param config: the config for transform job
        :param wait_for_completion: if the program should keep running until job finishes
        :param check_interval: the time interval in seconds which the operator
            will check the status of any SageMaker job
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
            SageMaker jobs that run longer than this will fail. Setting this to
            None implies no timeout for any SageMaker job.
        :return: A response to transform job creation
        """
        if "S3DataSource" in config["TransformInput"]["DataSource"]:
            self.check_s3_url(config["TransformInput"]["DataSource"]["S3DataSource"]["S3Uri"])

        response = self.get_conn().create_transform_job(**config)
        if wait_for_completion:
            self.check_status(
                config["TransformJobName"],
                "TransformJobStatus",
                self.describe_transform_job,
                check_interval,
                max_ingestion_time,
            )
        return response

    def create_processing_job(
        self,
        config: dict,
        wait_for_completion: bool = True,
        check_interval: int = 30,
        max_ingestion_time: int | None = None,
    ):
        """
        Use Amazon SageMaker Processing to analyze data and evaluate machine learning
        models on Amazon SageMaker. With Processing, you can use a simplified, managed
        experience on SageMaker to run your data processing workloads, such as feature
        engineering, data validation, model evaluation, and model interpretation.

        :param config: the config for processing job
        :param wait_for_completion: if the program should keep running until job finishes
        :param check_interval: the time interval in seconds which the operator
            will check the status of any SageMaker job
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
            SageMaker jobs that run longer than this will fail. Setting this to
            None implies no timeout for any SageMaker job.
        :return: A response to transform job creation
        """
        response = self.get_conn().create_processing_job(**config)
        if wait_for_completion:
            self.check_status(
                config["ProcessingJobName"],
                "ProcessingJobStatus",
                self.describe_processing_job,
                check_interval,
                max_ingestion_time,
            )
        return response

    def create_model(self, config: dict):
        """
        Creates a model in Amazon SageMaker. In the request, you name the model and
        describe a primary container. For the primary container, you specify the Docker
        image that contains inference code, artifacts (from prior training), and a custom
        environment map that the inference code uses when you deploy the model for predictions.

        :param config: the config for model
        :return: A response to model creation
        """
        return self.get_conn().create_model(**config)

    def create_endpoint_config(self, config: dict):
        """
        Creates an endpoint configuration that Amazon SageMaker hosting
        services uses to deploy models. In the configuration, you identify
        one or more models, created using the CreateModel API, to deploy and
        the resources that you want Amazon SageMaker to provision.

        .. seealso::
             :class:`~airflow.providers.amazon.aws.hooks.sagemaker.SageMakerHook.create_model`
             :class:`~airflow.providers.amazon.aws.hooks.sagemaker.SageMakerHook.create_endpoint`

        :param config: the config for endpoint-config
        :return: A response to endpoint config creation
        """
        return self.get_conn().create_endpoint_config(**config)

    def create_endpoint(
        self,
        config: dict,
        wait_for_completion: bool = True,
        check_interval: int = 30,
        max_ingestion_time: int | None = None,
    ):
        """
        When you create a serverless endpoint, SageMaker provisions and manages
        the compute resources for you. Then, you can make inference requests to
        the endpoint and receive model predictions in response. SageMaker scales
        the compute resources up and down as needed to handle your request traffic.

        Requires an Endpoint Config.
         .. seealso::
             :class:`~airflow.providers.amazon.aws.hooks.sagemaker.SageMakerHook.create_endpoint_config`


        :param config: the config for endpoint
        :param wait_for_completion: if the program should keep running until job finishes
        :param check_interval: the time interval in seconds which the operator
            will check the status of any SageMaker job
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
            SageMaker jobs that run longer than this will fail. Setting this to
            None implies no timeout for any SageMaker job.
        :return: A response to endpoint creation
        """
        response = self.get_conn().create_endpoint(**config)
        if wait_for_completion:
            self.check_status(
                config["EndpointName"],
                "EndpointStatus",
                self.describe_endpoint,
                check_interval,
                max_ingestion_time,
                non_terminal_states=self.endpoint_non_terminal_states,
            )
        return response

    def update_endpoint(
        self,
        config: dict,
        wait_for_completion: bool = True,
        check_interval: int = 30,
        max_ingestion_time: int | None = None,
    ):
        """
        Deploys the new EndpointConfig specified in the request, switches to using
        newly created endpoint, and then deletes resources provisioned for the
        endpoint using the previous EndpointConfig (there is no availability loss).

        :param config: the config for endpoint
        :param wait_for_completion: if the program should keep running until job finishes
        :param check_interval: the time interval in seconds which the operator
            will check the status of any SageMaker job
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
            SageMaker jobs that run longer than this will fail. Setting this to
            None implies no timeout for any SageMaker job.
        :return: A response to endpoint update
        """
        response = self.get_conn().update_endpoint(**config)
        if wait_for_completion:
            self.check_status(
                config["EndpointName"],
                "EndpointStatus",
                self.describe_endpoint,
                check_interval,
                max_ingestion_time,
                non_terminal_states=self.endpoint_non_terminal_states,
            )
        return response

    def describe_training_job(self, name: str):
        """
        Return the training job info associated with the name

        :param name: the name of the training job
        :return: A dict contains all the training job info
        """
        return self.get_conn().describe_training_job(TrainingJobName=name)

    def describe_training_job_with_log(
        self,
        job_name: str,
        positions,
        stream_names: list,
        instance_count: int,
        state: int,
        last_description: dict,
        last_describe_job_call: float,
    ):
        """Return the training job info associated with job_name and print CloudWatch logs"""
        log_group = "/aws/sagemaker/TrainingJobs"

        if len(stream_names) < instance_count:
            # Log streams are created whenever a container starts writing to stdout/err, so this list
            # may be dynamic until we have a stream for every instance.
            logs_conn = self.logs_hook.get_conn()
            try:
                streams = logs_conn.describe_log_streams(
                    logGroupName=log_group,
                    logStreamNamePrefix=job_name + "/",
                    orderBy="LogStreamName",
                    limit=instance_count,
                )
                stream_names = [s["logStreamName"] for s in streams["logStreams"]]
                positions.update(
                    [(s, Position(timestamp=0, skip=0)) for s in stream_names if s not in positions]
                )
            except logs_conn.exceptions.ResourceNotFoundException:
                # On the very first training job run on an account, there's no log group until
                # the container starts logging, so ignore any errors thrown about that
                pass

        if len(stream_names) > 0:
            for idx, event in self.multi_stream_iter(log_group, stream_names, positions):
                self.log.info(event["message"])
                ts, count = positions[stream_names[idx]]
                if event["timestamp"] == ts:
                    positions[stream_names[idx]] = Position(timestamp=ts, skip=count + 1)
                else:
                    positions[stream_names[idx]] = Position(timestamp=event["timestamp"], skip=1)

        if state == LogState.COMPLETE:
            return state, last_description, last_describe_job_call

        if state == LogState.JOB_COMPLETE:
            state = LogState.COMPLETE
        elif time.monotonic() - last_describe_job_call >= 30:
            description = self.describe_training_job(job_name)
            last_describe_job_call = time.monotonic()

            if secondary_training_status_changed(description, last_description):
                self.log.info(secondary_training_status_message(description, last_description))
                last_description = description

            status = description["TrainingJobStatus"]

            if status not in self.non_terminal_states:
                state = LogState.JOB_COMPLETE
        return state, last_description, last_describe_job_call

    def describe_tuning_job(self, name: str) -> dict:
        """
        Return the tuning job info associated with the name

        :param name: the name of the tuning job
        :return: A dict contains all the tuning job info
        """
        return self.get_conn().describe_hyper_parameter_tuning_job(HyperParameterTuningJobName=name)

    def describe_model(self, name: str) -> dict:
        """
        Return the SageMaker model info associated with the name

        :param name: the name of the SageMaker model
        :return: A dict contains all the model info
        """
        return self.get_conn().describe_model(ModelName=name)

    def describe_transform_job(self, name: str) -> dict:
        """
        Return the transform job info associated with the name

        :param name: the name of the transform job
        :return: A dict contains all the transform job info
        """
        return self.get_conn().describe_transform_job(TransformJobName=name)

    def describe_processing_job(self, name: str) -> dict:
        """
        Return the processing job info associated with the name

        :param name: the name of the processing job
        :return: A dict contains all the processing job info
        """
        return self.get_conn().describe_processing_job(ProcessingJobName=name)

    def describe_endpoint_config(self, name: str) -> dict:
        """
        Return the endpoint config info associated with the name

        :param name: the name of the endpoint config
        :return: A dict contains all the endpoint config info
        """
        return self.get_conn().describe_endpoint_config(EndpointConfigName=name)

    def describe_endpoint(self, name: str) -> dict:
        """
        :param name: the name of the endpoint
        :return: A dict contains all the endpoint info
        """
        return self.get_conn().describe_endpoint(EndpointName=name)

    def check_status(
        self,
        job_name: str,
        key: str,
        describe_function: Callable,
        check_interval: int,
        max_ingestion_time: int | None = None,
        non_terminal_states: set | None = None,
    ) -> dict:
        """
        Check status of a SageMaker resource

        :param job_name: name of the resource to check status, can be a job but also pipeline for instance.
        :param key: the key of the response dict that points to the state
        :param describe_function: the function used to retrieve the status
        :param args: the arguments for the function
        :param check_interval: the time interval in seconds which the operator
            will check the status of any SageMaker resource
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
            SageMaker resources that run longer than this will fail. Setting this to
            None implies no timeout for any SageMaker resource.
        :param non_terminal_states: the set of nonterminal states
        :return: response of describe call after resource is done
        """
        if not non_terminal_states:
            non_terminal_states = self.non_terminal_states

        sec = 0

        while True:
            time.sleep(check_interval)
            sec += check_interval

            try:
                response = describe_function(job_name)
                status = response[key]
                self.log.info("Resource still running for %s seconds... current status is %s", sec, status)
            except KeyError:
                raise AirflowException("Could not get status of the SageMaker resource")
            except ClientError:
                raise AirflowException("AWS request failed, check logs for more info")

            if status in self.failed_states:
                raise AirflowException(f"SageMaker resource failed because {response['FailureReason']}")
            elif status not in non_terminal_states:
                break

            if max_ingestion_time and sec > max_ingestion_time:
                # ensure that the resource gets killed if the max ingestion time is exceeded
                raise AirflowException(f"SageMaker resource took more than {max_ingestion_time} seconds")

        self.log.info("SageMaker resource completed")
        return response

    def check_training_status_with_log(
        self,
        job_name: str,
        non_terminal_states: set,
        failed_states: set,
        wait_for_completion: bool,
        check_interval: int,
        max_ingestion_time: int | None = None,
    ):
        """
        Display the logs for a given training job, optionally tailing them until the
        job is complete.

        :param job_name: name of the training job to check status and display logs for
        :param non_terminal_states: the set of non_terminal states
        :param failed_states: the set of failed states
        :param wait_for_completion: Whether to keep looking for new log entries
            until the job completes
        :param check_interval: The interval in seconds between polling for new log entries and job completion
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
            SageMaker jobs that run longer than this will fail. Setting this to
            None implies no timeout for any SageMaker job.
        :return: None
        """
        sec = 0
        description = self.describe_training_job(job_name)
        self.log.info(secondary_training_status_message(description, None))
        instance_count = description["ResourceConfig"]["InstanceCount"]
        status = description["TrainingJobStatus"]

        stream_names: list = []  # The list of log streams
        positions: dict = {}  # The current position in each stream, map of stream name -> position

        job_already_completed = status not in non_terminal_states

        state = LogState.TAILING if wait_for_completion and not job_already_completed else LogState.COMPLETE

        # The loop below implements a state machine that alternates between checking the job status and
        # reading whatever is available in the logs at this point. Note, that if we were called with
        # wait_for_completion == False, we never check the job status.
        #
        # If wait_for_completion == TRUE and job is not completed, the initial state is TAILING
        # If wait_for_completion == FALSE, the initial state is COMPLETE
        # (doesn't matter if the job really is complete).
        #
        # The state table:
        #
        # STATE               ACTIONS                        CONDITION             NEW STATE
        # ----------------    ----------------               -----------------     ----------------
        # TAILING             Read logs, Pause, Get status   Job complete          JOB_COMPLETE
        #                                                    Else                  TAILING
        # JOB_COMPLETE        Read logs, Pause               Any                   COMPLETE
        # COMPLETE            Read logs, Exit                                      N/A
        #
        # Notes:
        # - The JOB_COMPLETE state forces us to do an extra pause and read any items that
        # got to Cloudwatch after the job was marked complete.
        last_describe_job_call = time.monotonic()
        last_description = description

        while True:
            time.sleep(check_interval)
            sec += check_interval

            state, last_description, last_describe_job_call = self.describe_training_job_with_log(
                job_name,
                positions,
                stream_names,
                instance_count,
                state,
                last_description,
                last_describe_job_call,
            )
            if state == LogState.COMPLETE:
                break

            if max_ingestion_time and sec > max_ingestion_time:
                # ensure that the job gets killed if the max ingestion time is exceeded
                raise AirflowException(f"SageMaker job took more than {max_ingestion_time} seconds")

        if wait_for_completion:
            status = last_description["TrainingJobStatus"]
            if status in failed_states:
                reason = last_description.get("FailureReason", "(No reason provided)")
                raise AirflowException(f"Error training {job_name}: {status} Reason: {reason}")
            billable_time = (
                last_description["TrainingEndTime"] - last_description["TrainingStartTime"]
            ) * instance_count
            self.log.info("Billable seconds: %d", int(billable_time.total_seconds()) + 1)

    def list_training_jobs(
        self, name_contains: str | None = None, max_results: int | None = None, **kwargs
    ) -> list[dict]:
        """
        This method wraps boto3's `list_training_jobs`. The training job name and max results are configurable
        via arguments. Other arguments are not, and should be provided via kwargs. Note boto3 expects these in
        CamelCase format, for example:

        .. code-block:: python

            list_training_jobs(name_contains="myjob", StatusEquals="Failed")

        .. seealso::
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_training_jobs

        :param name_contains: (optional) partial name to match
        :param max_results: (optional) maximum number of results to return. None returns infinite results
        :param kwargs: (optional) kwargs to boto3's list_training_jobs method
        :return: results of the list_training_jobs request
        """
        config, max_results = self._preprocess_list_request_args(name_contains, max_results, **kwargs)
        list_training_jobs_request = partial(self.get_conn().list_training_jobs, **config)
        results = self._list_request(
            list_training_jobs_request, "TrainingJobSummaries", max_results=max_results
        )
        return results

    def list_transform_jobs(
        self, name_contains: str | None = None, max_results: int | None = None, **kwargs
    ) -> list[dict]:
        """
        This method wraps boto3's `list_transform_jobs`.
        The transform job name and max results are configurable via arguments.
        Other arguments are not, and should be provided via kwargs. Note boto3 expects these in
        CamelCase format, for example:

        .. code-block:: python

            list_transform_jobs(name_contains="myjob", StatusEquals="Failed")

        .. seealso::
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_transform_jobs

        :param name_contains: (optional) partial name to match
        :param max_results: (optional) maximum number of results to return. None returns infinite results
        :param kwargs: (optional) kwargs to boto3's list_transform_jobs method
        :return: results of the list_transform_jobs request
        """
        config, max_results = self._preprocess_list_request_args(name_contains, max_results, **kwargs)
        list_transform_jobs_request = partial(self.get_conn().list_transform_jobs, **config)
        results = self._list_request(
            list_transform_jobs_request, "TransformJobSummaries", max_results=max_results
        )
        return results

    def list_processing_jobs(self, **kwargs) -> list[dict]:
        """
        This method wraps boto3's `list_processing_jobs`. All arguments should be provided via kwargs.
        Note boto3 expects these in CamelCase format, for example:

        .. code-block:: python

            list_processing_jobs(NameContains="myjob", StatusEquals="Failed")

        .. seealso::
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_processing_jobs

        :param kwargs: (optional) kwargs to boto3's list_training_jobs method
        :return: results of the list_processing_jobs request
        """
        list_processing_jobs_request = partial(self.get_conn().list_processing_jobs, **kwargs)
        results = self._list_request(
            list_processing_jobs_request, "ProcessingJobSummaries", max_results=kwargs.get("MaxResults")
        )
        return results

    def _preprocess_list_request_args(
        self, name_contains: str | None = None, max_results: int | None = None, **kwargs
    ) -> tuple[dict[str, Any], int | None]:
        """
        This method preprocesses the arguments to the boto3's list_* methods.
        It will turn arguments name_contains and max_results as boto3 compliant CamelCase format.
        This method also makes sure that these two arguments are only set once.

        :param name_contains: boto3 function with arguments
        :param max_results: the result key to iterate over
        :param kwargs: (optional) kwargs to boto3's list_* method
        :return: Tuple with config dict to be passed to boto3's list_* method and max_results parameter
        """
        config = {}

        if name_contains:
            if "NameContains" in kwargs:
                raise AirflowException("Either name_contains or NameContains can be provided, not both.")
            config["NameContains"] = name_contains

        if "MaxResults" in kwargs and kwargs["MaxResults"] is not None:
            if max_results:
                raise AirflowException("Either max_results or MaxResults can be provided, not both.")
            # Unset MaxResults, we'll use the SageMakerHook's internal method for iteratively fetching results
            max_results = kwargs["MaxResults"]
            del kwargs["MaxResults"]

        config.update(kwargs)

        return config, max_results

    def _list_request(
        self, partial_func: Callable, result_key: str, max_results: int | None = None
    ) -> list[dict]:
        """
        All AWS boto3 list_* requests return results in batches (if the key "NextToken" is contained in the
        result, there are more results to fetch). The default AWS batch size is 10, and configurable up to
        100. This function iteratively loads all results (or up to a given maximum).

        Each boto3 list_* function returns the results in a list with a different name. The key of this
        structure must be given to iterate over the results, e.g. "TransformJobSummaries" for
        list_transform_jobs().

        :param partial_func: boto3 function with arguments
        :param result_key: the result key to iterate over
        :param max_results: maximum number of results to return (None = infinite)
        :return: Results of the list_* request
        """
        sagemaker_max_results = 100  # Fixed number set by AWS

        results: list[dict] = []
        next_token = None

        while True:
            kwargs = {}
            if next_token is not None:
                kwargs["NextToken"] = next_token

            if max_results is None:
                kwargs["MaxResults"] = sagemaker_max_results
            else:
                kwargs["MaxResults"] = min(max_results - len(results), sagemaker_max_results)

            response = partial_func(**kwargs)
            self.log.debug("Fetched %s results.", len(response[result_key]))
            results.extend(response[result_key])

            if "NextToken" not in response or (max_results is not None and len(results) == max_results):
                # Return when there are no results left (no NextToken) or when we've reached max_results.
                return results
            else:
                next_token = response["NextToken"]

    def find_processing_job_by_name(self, processing_job_name: str) -> bool:
        """
        Query processing job by name

        This method is deprecated.
        Please use `airflow.providers.amazon.aws.hooks.sagemaker.count_processing_jobs_by_name`.
        """
        warnings.warn(
            "This method is deprecated. "
            "Please use `airflow.providers.amazon.aws.hooks.sagemaker.count_processing_jobs_by_name`.",
            DeprecationWarning,
            stacklevel=2,
        )
        return bool(self.count_processing_jobs_by_name(processing_job_name))

    @staticmethod
    def _name_matches_pattern(
        processing_job_name: str,
        found_name: str,
        job_name_suffix: str | None = None,
    ) -> bool:
        pattern = re.compile(f"^{processing_job_name}({job_name_suffix})?$")
        return pattern.fullmatch(found_name) is not None

    def count_processing_jobs_by_name(
        self,
        processing_job_name: str,
        job_name_suffix: str | None = None,
        throttle_retry_delay: int = 2,
        retries: int = 3,
    ) -> int:
        """
        Returns the number of processing jobs found with the provided name prefix.
        :param processing_job_name: The prefix to look for.
        :param job_name_suffix: The optional suffix which may be appended to deduplicate an existing job name.
        :param throttle_retry_delay: Seconds to wait if a ThrottlingException is hit.
        :param retries: The max number of times to retry.
        :returns: The number of processing jobs that start with the provided prefix.
        """
        try:
            jobs = self.get_conn().list_processing_jobs(NameContains=processing_job_name)
            # We want to make sure the job name starts with the provided name, not just contains it.
            matching_jobs = [
                job["ProcessingJobName"]
                for job in jobs["ProcessingJobSummaries"]
                if self._name_matches_pattern(processing_job_name, job["ProcessingJobName"], job_name_suffix)
            ]
            return len(matching_jobs)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFound":
                # No jobs found with that name.  This is good, return 0.
                return 0
            if e.response["Error"]["Code"] == "ThrottlingException" and retries:
                # If we hit a ThrottlingException, back off a little and try again.
                time.sleep(throttle_retry_delay)
                return self.count_processing_jobs_by_name(
                    processing_job_name, job_name_suffix, throttle_retry_delay * 2, retries - 1
                )
            raise

    def delete_model(self, model_name: str):
        """Delete SageMaker model

        :param model_name: name of the model
        """
        try:
            self.get_conn().delete_model(ModelName=model_name)
        except Exception as general_error:
            self.log.error("Failed to delete model, error: %s", general_error)
            raise

    def describe_pipeline_exec(self, pipeline_exec_arn: str, verbose: bool = False):
        """Get info about a SageMaker pipeline execution

        :param pipeline_exec_arn: arn of the pipeline execution
        :param verbose: Whether to log details about the steps status in the pipeline execution
        """
        if verbose:
            res = self.conn.list_pipeline_execution_steps(PipelineExecutionArn=pipeline_exec_arn)
            count_by_state = Counter(s["StepStatus"] for s in res["PipelineExecutionSteps"])
            running_steps = [
                s["StepName"] for s in res["PipelineExecutionSteps"] if s["StepStatus"] == "Executing"
            ]
            self.log.info("state of the pipeline steps: %s", count_by_state)
            self.log.info("steps currently in progress: %s", running_steps)

        return self.conn.describe_pipeline_execution(PipelineExecutionArn=pipeline_exec_arn)

    def start_pipeline(
        self,
        pipeline_name: str,
        display_name: str = "airflow-triggered-execution",
        pipeline_params: dict | None = None,
        wait_for_completion: bool = False,
        check_interval: int = 30,
        verbose: bool = True,
    ) -> str:
        """
        Start a new execution for a SageMaker pipeline

        :param pipeline_name: Name of the pipeline to start (this is _not_ the ARN).
        :param display_name: The name this pipeline execution will have in the UI. Doesn't need to be unique.
        :param pipeline_params: Optional parameters for the pipeline.
            All parameters supplied need to already be present in the pipeline definition.
        :param wait_for_completion: Will only return once the pipeline is complete if true.
        :param check_interval: How long to wait between checks for pipeline status when waiting for
            completion.
        :param verbose: Whether to print steps details when waiting for completion.
            Defaults to true, consider turning off for pipelines that have thousands of steps.

        :return: the ARN of the pipeline execution launched.
        """
        if pipeline_params is None:
            pipeline_params = {}
        formatted_params = [{"Name": kvp[0], "Value": kvp[1]} for kvp in pipeline_params.items()]

        try:
            res = self.conn.start_pipeline_execution(
                PipelineName=pipeline_name,
                PipelineExecutionDisplayName=display_name,
                PipelineParameters=formatted_params,
            )
        except ClientError as ce:
            self.log.error("Failed to start pipeline execution, error: %s", ce)
            raise

        arn = res["PipelineExecutionArn"]
        if wait_for_completion:
            self.check_status(
                arn,
                "PipelineExecutionStatus",
                lambda p: self.describe_pipeline_exec(p, verbose),
                check_interval,
                non_terminal_states=self.pipeline_non_terminal_states,
            )
        return arn

    def stop_pipeline(
        self,
        pipeline_exec_arn: str,
        wait_for_completion: bool = False,
        check_interval: int = 10,
        verbose: bool = True,
        fail_if_not_running: bool = False,
    ) -> str:
        """Stop SageMaker pipeline execution

        :param pipeline_exec_arn: Amazon Resource Name (ARN) of the pipeline execution.
            It's the ARN of the pipeline itself followed by "/execution/" and an id.
        :param wait_for_completion: Whether to wait for the pipeline to reach a final state.
            (i.e. either 'Stopped' or 'Failed')
        :param check_interval: How long to wait between checks for pipeline status when waiting for
            completion.
        :param verbose: Whether to print steps details when waiting for completion.
            Defaults to true, consider turning off for pipelines that have thousands of steps.
        :param fail_if_not_running: This method will raise an exception if the pipeline we're trying to stop
            is not in an "Executing" state when the call is sent (which would mean that the pipeline is
            already either stopping or stopped).
            Note that setting this to True will raise an error if the pipeline finished successfully before it
            was stopped.
        :return: Status of the pipeline execution after the operation.
            One of 'Executing'|'Stopping'|'Stopped'|'Failed'|'Succeeded'.
        """
        try:
            self.conn.stop_pipeline_execution(PipelineExecutionArn=pipeline_exec_arn)
        except ClientError as ce:
            # we have to rely on the message to catch the right error here, because its type
            # (ValidationException) is shared with other kinds of error (for instance, badly formatted ARN)
            if (
                not fail_if_not_running
                and "Only pipelines with 'Executing' status can be stopped" in ce.response["Error"]["Message"]
            ):
                self.log.warning("Cannot stop pipeline execution, as it was not running: %s", ce)
            else:
                self.log.error(ce)
                raise

        res = self.describe_pipeline_exec(pipeline_exec_arn)

        if wait_for_completion and res["PipelineExecutionStatus"] in self.pipeline_non_terminal_states:
            res = self.check_status(
                pipeline_exec_arn,
                "PipelineExecutionStatus",
                lambda p: self.describe_pipeline_exec(p, verbose),
                check_interval,
                non_terminal_states=self.pipeline_non_terminal_states,
            )

        return res["PipelineExecutionStatus"]

    def create_model_package_group(self, package_group_name: str, package_group_desc: str = "") -> bool:
        """
        Creates a Model Package Group if it does not already exist

        :param package_group_name: Name of the model package group to create if not already present.
        :param package_group_desc: Description of the model package group, if it was to be created (optional).

        :return: True if the model package group was created, False if it already existed.
        """
        try:
            res = self.conn.create_model_package_group(
                ModelPackageGroupName=package_group_name,
                ModelPackageGroupDescription=package_group_desc,
            )
            self.log.info(
                "Created new Model Package Group with name %s (ARN: %s)",
                package_group_name,
                res["ModelPackageGroupArn"],
            )
            return True
        except ClientError as e:
            # ValidationException can also happen if the package group name contains invalid char,
            # so we have to look at the error message too
            if e.response["Error"]["Code"] == "ValidationException" and e.response["Error"][
                "Message"
            ].startswith("Model Package Group already exists"):
                # log msg only so it doesn't look like an error
                self.log.info("%s", e.response["Error"]["Message"])
                return False
            else:
                self.log.error("Error when trying to create Model Package Group: %s", e)
                raise

    def _describe_auto_ml_job(self, job_name: str):
        res = self.conn.describe_auto_ml_job(AutoMLJobName=job_name)
        self.log.info("%s's current step: %s", job_name, res["AutoMLJobSecondaryStatus"])
        return res

    def create_auto_ml_job(
        self,
        job_name: str,
        s3_input: str,
        target_attribute: str,
        s3_output: str,
        role_arn: str,
        compressed_input: bool = False,
        time_limit: int | None = None,
        autodeploy_endpoint_name: str | None = None,
        extras: dict | None = None,
        wait_for_completion: bool = True,
        check_interval: int = 30,
    ) -> dict | None:
        """
        Creates an auto ML job, learning to predict the given column from the data provided through S3.
        The learning output is written to the specified S3 location.

        :param job_name: Name of the job to create, needs to be unique within the account.
        :param s3_input: The S3 location (folder or file) where to fetch the data.
            By default, it expects csv with headers.
        :param target_attribute: The name of the column containing the values to predict.
        :param s3_output: The S3 folder where to write the model artifacts. Must be 128 characters or fewer.
        :param role_arn: The ARN or the IAM role to use when interacting with S3.
            Must have read access to the input, and write access to the output folder.
        :param compressed_input: Set to True if the input is gzipped.
        :param time_limit: The maximum amount of time in seconds to spend training the model(s).
        :param autodeploy_endpoint_name: If specified, the best model will be deployed to an endpoint with
            that name. No deployment made otherwise.
        :param extras: Use this dictionary to set any variable input variable for job creation that is not
            offered through the parameters of this function. The format is described in:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.create_auto_ml_job
        :param wait_for_completion: Whether to wait for the job to finish before returning. Defaults to True.
        :param check_interval: Interval in seconds between 2 status checks when waiting for completion.

        :returns: Only if waiting for completion, a dictionary detailing the best model. The structure is that
            of the "BestCandidate" key in:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_auto_ml_job
        """
        input_data = [
            {
                "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": s3_input}},
                "TargetAttributeName": target_attribute,
            },
        ]
        params_dict = {
            "AutoMLJobName": job_name,
            "InputDataConfig": input_data,
            "OutputDataConfig": {"S3OutputPath": s3_output},
            "RoleArn": role_arn,
        }
        if compressed_input:
            input_data[0]["CompressionType"] = "Gzip"
        if time_limit:
            params_dict.update(
                {"AutoMLJobConfig": {"CompletionCriteria": {"MaxAutoMLJobRuntimeInSeconds": time_limit}}}
            )
        if autodeploy_endpoint_name:
            params_dict.update({"ModelDeployConfig": {"EndpointName": autodeploy_endpoint_name}})
        if extras:
            params_dict.update(extras)

        # returns the job ARN, but we don't need it because we access it by its name
        self.conn.create_auto_ml_job(**params_dict)

        if wait_for_completion:
            res = self.check_status(
                job_name,
                "AutoMLJobStatus",
                # cannot pass the function directly because the parameter needs to be named
                self._describe_auto_ml_job,
                check_interval,
            )
            if "BestCandidate" in res:
                return res["BestCandidate"]
        return None

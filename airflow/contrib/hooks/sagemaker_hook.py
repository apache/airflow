# -*- coding: utf-8 -*-
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
import tarfile
import tempfile
import time
import os
import collections

import botocore.config
from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.S3_hook import S3Hook
from airflow.utils import timezone


class LogState:
    STARTING = 1
    WAIT_IN_PROGRESS = 2
    TAILING = 3
    JOB_COMPLETE = 4
    COMPLETE = 5


# Position is a tuple that includes the last read timestamp and the number of items that were read
# at that time. This is used to figure out which event to start with on the next read.
Position = collections.namedtuple('Position', ['timestamp', 'skip'])


def argmin(arr, f):
    """Return the index, i, in arr that minimizes f(arr[i])"""
    m = None
    i = None
    for idx, item in enumerate(arr):
        if item is not None:
            if m is None or f(item) < m:
                m = f(item)
                i = idx
    return i


def secondary_training_status_changed(current_job_description, prev_job_description):
    """
    Returns true if training job's secondary status message has changed.

    :param current_job_description: Current job description, returned from DescribeTrainingJob call.
    :type current_job_description: dict
    :param prev_job_description: Previous job description, returned from DescribeTrainingJob call.
    :type prev_job_description: dict

    :return: Whether the secondary status message of a training job changed or not.
    """
    current_secondary_status_transitions = current_job_description.get('SecondaryStatusTransitions')
    if current_secondary_status_transitions is None or len(current_secondary_status_transitions) == 0:
        return False

    prev_job_secondary_status_transitions = prev_job_description.get('SecondaryStatusTransitions') \
        if prev_job_description is not None else None

    last_message = prev_job_secondary_status_transitions[-1]['StatusMessage'] \
        if prev_job_secondary_status_transitions is not None \
        and len(prev_job_secondary_status_transitions) > 0 else ''

    message = current_job_description['SecondaryStatusTransitions'][-1]['StatusMessage']

    return message != last_message


def secondary_training_status_message(job_description, prev_description):
    """
    Returns a string contains start time and the secondary training job status message.

    :param job_description: Returned response from DescribeTrainingJob call
    :type job_description: dict
    :param prev_description: Previous job description from DescribeTrainingJob call
    :type prev_description: dict

    :return: Job status string to be printed.
    """

    if job_description is None or job_description.get('SecondaryStatusTransitions') is None\
            or len(job_description.get('SecondaryStatusTransitions')) == 0:
        return ''

    prev_description_secondary_transitions = prev_description.get('SecondaryStatusTransitions')\
        if prev_description is not None else None
    prev_transitions_num = len(prev_description['SecondaryStatusTransitions'])\
        if prev_description_secondary_transitions is not None else 0
    current_transitions = job_description['SecondaryStatusTransitions']

    transitions_to_print = current_transitions[-1:] if len(current_transitions) == prev_transitions_num else \
        current_transitions[prev_transitions_num - len(current_transitions):]

    status_strs = []
    for transition in transitions_to_print:
        message = transition['StatusMessage']
        time_str = timezone.convert_to_utc(job_description['LastModifiedTime']).strftime('%Y-%m-%d %H:%M:%S')
        status_strs.append('{} {} - {}'.format(time_str, transition['Status'], message))

    return '\n'.join(status_strs)


class SageMakerHook(AwsHook):
    """
    Interact with Amazon SageMaker.
    """
    non_terminal_states = {'InProgress', 'Stopping'}
    endpoint_non_terminal_states = {'Creating', 'Updating', 'SystemUpdating',
                                    'RollingBack', 'Deleting'}
    failed_states = {'Failed'}

    def __init__(self,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

    def tar_and_s3_upload(self, path, key, bucket):
        """
        Tar the local file or directory and upload to s3

        :param path: local file or directory
        :type path: str
        :param key: s3 key
        :type key: str
        :param bucket: s3 bucket
        :type bucket: str
        :return: None
        """
        with tempfile.TemporaryFile() as temp_file:
            if os.path.isdir(path):
                files = [os.path.join(path, name) for name in os.listdir(path)]
            else:
                files = [path]
            with tarfile.open(mode='w:gz', fileobj=temp_file) as tar_file:
                for f in files:
                    tar_file.add(f, arcname=os.path.basename(f))
            temp_file.seek(0)
            self.s3_hook.load_file_obj(temp_file, key, bucket, replace=True)

    def configure_s3_resources(self, config):
        """
        Extract the S3 operations from the configuration and execute them.

        :param config: config of SageMaker operation
        :type config: dict
        :rtype: dict
        """
        s3_operations = config.pop('S3Operations', None)

        if s3_operations is not None:
            create_bucket_ops = s3_operations.get('S3CreateBucket', [])
            upload_ops = s3_operations.get('S3Upload', [])
            for op in create_bucket_ops:
                self.s3_hook.create_bucket(bucket_name=op['Bucket'])
            for op in upload_ops:
                if op['Tar']:
                    self.tar_and_s3_upload(op['Path'], op['Key'],
                                           op['Bucket'])
                else:
                    self.s3_hook.load_file(op['Path'], op['Key'],
                                           op['Bucket'])

    def check_s3_url(self, s3url):
        """
        Check if an S3 URL exists

        :param s3url: S3 url
        :type s3url: str
        :rtype: bool
        """
        bucket, key = S3Hook.parse_s3_url(s3url)
        if not self.s3_hook.check_for_bucket(bucket_name=bucket):
            raise AirflowException(
                "The input S3 Bucket {} does not exist ".format(bucket))
        if key and not self.s3_hook.check_for_key(key=key, bucket_name=bucket)\
           and not self.s3_hook.check_for_prefix(
                prefix=key, bucket_name=bucket, delimiter='/'):
            # check if s3 key exists in the case user provides a single file
            # or if s3 prefix exists in the case user provides multiple files in
            # a prefix
            raise AirflowException("The input S3 Key "
                                   "or Prefix {} does not exist in the Bucket {}"
                                   .format(s3url, bucket))
        return True

    def check_training_config(self, training_config):
        """
        Check if a training configuration is valid

        :param training_config: training_config
        :type training_config: dict
        :return: None
        """
        for channel in training_config['InputDataConfig']:
            self.check_s3_url(channel['DataSource']['S3DataSource']['S3Uri'])

    def check_tuning_config(self, tuning_config):
        """
        Check if a tuning configuration is valid

        :param tuning_config: tuning_config
        :type tuning_config: dict
        :return: None
        """
        for channel in tuning_config['TrainingJobDefinition']['InputDataConfig']:
            self.check_s3_url(channel['DataSource']['S3DataSource']['S3Uri'])

    def get_conn(self):
        """
        Establish an AWS connection for SageMaker

        :rtype: :py:class:`SageMaker.Client`
        """
        return self.get_client_type('sagemaker')

    def get_log_conn(self):
        """
        Establish an AWS connection for retrieving logs during training

        :rtype: CloudWatchLogs.Client
        """
        config = botocore.config.Config(retries={'max_attempts': 15})
        return self.get_client_type('logs', config=config)

    def log_stream(self, log_group, stream_name, start_time=0, skip=0):
        """
        A generator for log items in a single stream. This will yield all the
        items that are available at the current moment.

        :param log_group: The name of the log group.
        :type log_group: str
        :param stream_name: The name of the specific stream.
        :type stream_name: str
        :param start_time: The time stamp value to start reading the logs from (default: 0).
        :type start_time: int
        :param skip: The number of log entries to skip at the start (default: 0).
            This is for when there are multiple entries at the same timestamp.
        :type skip: int
        :rtype: dict
        :return: | A CloudWatch log event with the following key-value pairs:
                 |   'timestamp' (int): The time in milliseconds of the event.
                 |   'message' (str): The log event data.
                 |   'ingestionTime' (int): The time in milliseconds the event was ingested.
        """

        next_token = None

        event_count = 1
        while event_count > 0:
            if next_token is not None:
                token_arg = {'nextToken': next_token}
            else:
                token_arg = {}

            response = self.get_log_conn().get_log_events(logGroupName=log_group,
                                                          logStreamName=stream_name,
                                                          startTime=start_time,
                                                          startFromHead=True,
                                                          **token_arg)
            next_token = response['nextForwardToken']
            events = response['events']
            event_count = len(events)
            if event_count > skip:
                events = events[skip:]
                skip = 0
            else:
                skip = skip - event_count
                events = []
            yield from events

    def multi_stream_iter(self, log_group, streams, positions=None):
        """
        Iterate over the available events coming from a set of log streams in a single log group
        interleaving the events from each stream so they're yielded in timestamp order.

        :param log_group: The name of the log group.
        :type log_group: str
        :param streams: A list of the log stream names. The position of the stream in this list is
            the stream number.
        :type streams: list
        :param positions: A list of pairs of (timestamp, skip) which represents the last record
            read from each stream.
        :type positions: list
        :return: A tuple of (stream number, cloudwatch log event).
        """
        positions = positions or {s: Position(timestamp=0, skip=0) for s in streams}
        event_iters = [self.log_stream(log_group, s, positions[s].timestamp, positions[s].skip)
                       for s in streams]
        events = []
        for s in event_iters:
            if not s:
                events.append(None)
                continue
            try:
                events.append(next(s))
            except StopIteration:
                events.append(None)

        while any(events):
            i = argmin(events, lambda x: x['timestamp'] if x else 9999999999)
            yield (i, events[i])
            try:
                events[i] = next(event_iters[i])
            except StopIteration:
                events[i] = None

    def create_training_job(self, config, wait_for_completion=True, print_log=True,
                            check_interval=30, max_ingestion_time=None):
        """
        Create a training job

        :param config: the config for training
        :type config: dict
        :param wait_for_completion: if the program should keep running until job finishes
        :type wait_for_completion: bool
        :param check_interval: the time interval in seconds which the operator
            will check the status of any SageMaker job
        :type check_interval: int
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
            SageMaker jobs that run longer than this will fail. Setting this to
            None implies no timeout for any SageMaker job.
        :type max_ingestion_time: int
        :return: A response to training job creation
        """

        self.check_training_config(config)

        response = self.get_conn().create_training_job(**config)
        if print_log:
            self.check_training_status_with_log(config['TrainingJobName'],
                                                self.non_terminal_states,
                                                self.failed_states,
                                                wait_for_completion,
                                                check_interval, max_ingestion_time
                                                )
        elif wait_for_completion:
            describe_response = self.check_status(config['TrainingJobName'],
                                                  'TrainingJobStatus',
                                                  self.describe_training_job,
                                                  check_interval, max_ingestion_time
                                                  )

            billable_time = \
                (describe_response['TrainingEndTime'] - describe_response['TrainingStartTime']) * \
                describe_response['ResourceConfig']['InstanceCount']
            self.log.info('Billable seconds:{}'.format(int(billable_time.total_seconds()) + 1))

        return response

    def create_tuning_job(self, config, wait_for_completion=True,
                          check_interval=30, max_ingestion_time=None):
        """
        Create a tuning job

        :param config: the config for tuning
        :type config: dict
        :param wait_for_completion: if the program should keep running until job finishes
        :type wait_for_completion: bool
        :param check_interval: the time interval in seconds which the operator
            will check the status of any SageMaker job
        :type check_interval: int
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
            SageMaker jobs that run longer than this will fail. Setting this to
            None implies no timeout for any SageMaker job.
        :type max_ingestion_time: int
        :return: A response to tuning job creation
        """

        self.check_tuning_config(config)

        response = self.get_conn().create_hyper_parameter_tuning_job(**config)
        if wait_for_completion:
            self.check_status(config['HyperParameterTuningJobName'],
                              'HyperParameterTuningJobStatus',
                              self.describe_tuning_job,
                              check_interval, max_ingestion_time
                              )
        return response

    def create_transform_job(self, config, wait_for_completion=True,
                             check_interval=30, max_ingestion_time=None):
        """
        Create a transform job

        :param config: the config for transform job
        :type config: dict
        :param wait_for_completion: if the program should keep running until job finishes
        :type wait_for_completion: bool
        :param check_interval: the time interval in seconds which the operator
            will check the status of any SageMaker job
        :type check_interval: int
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
            SageMaker jobs that run longer than this will fail. Setting this to
            None implies no timeout for any SageMaker job.
        :type max_ingestion_time: int
        :return: A response to transform job creation
        """

        self.check_s3_url(config['TransformInput']['DataSource']['S3DataSource']['S3Uri'])

        response = self.get_conn().create_transform_job(**config)
        if wait_for_completion:
            self.check_status(config['TransformJobName'],
                              'TransformJobStatus',
                              self.describe_transform_job,
                              check_interval, max_ingestion_time
                              )
        return response

    def create_model(self, config):
        """
        Create a model job

        :param config: the config for model
        :type config: dict
        :return: A response to model creation
        """

        return self.get_conn().create_model(**config)

    def create_endpoint_config(self, config):
        """
        Create an endpoint config

        :param config: the config for endpoint-config
        :type config: dict
        :return: A response to endpoint config creation
        """

        return self.get_conn().create_endpoint_config(**config)

    def create_endpoint(self, config, wait_for_completion=True,
                        check_interval=30, max_ingestion_time=None):
        """
        Create an endpoint

        :param config: the config for endpoint
        :type config: dict
        :param wait_for_completion: if the program should keep running until job finishes
        :type wait_for_completion: bool
        :param check_interval: the time interval in seconds which the operator
            will check the status of any SageMaker job
        :type check_interval: int
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
            SageMaker jobs that run longer than this will fail. Setting this to
            None implies no timeout for any SageMaker job.
        :type max_ingestion_time: int
        :return: A response to endpoint creation
        """

        response = self.get_conn().create_endpoint(**config)
        if wait_for_completion:
            self.check_status(config['EndpointName'],
                              'EndpointStatus',
                              self.describe_endpoint,
                              check_interval, max_ingestion_time,
                              non_terminal_states=self.endpoint_non_terminal_states
                              )
        return response

    def update_endpoint(self, config, wait_for_completion=True,
                        check_interval=30, max_ingestion_time=None):
        """
        Update an endpoint

        :param config: the config for endpoint
        :type config: dict
        :param wait_for_completion: if the program should keep running until job finishes
        :type wait_for_completion: bool
        :param check_interval: the time interval in seconds which the operator
            will check the status of any SageMaker job
        :type check_interval: int
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
            SageMaker jobs that run longer than this will fail. Setting this to
            None implies no timeout for any SageMaker job.
        :type max_ingestion_time: int
        :return: A response to endpoint update
        """

        response = self.get_conn().update_endpoint(**config)
        if wait_for_completion:
            self.check_status(config['EndpointName'],
                              'EndpointStatus',
                              self.describe_endpoint,
                              check_interval, max_ingestion_time,
                              non_terminal_states=self.endpoint_non_terminal_states
                              )
        return response

    def describe_training_job(self, name):
        """
        Return the training job info associated with the name

        :param name: the name of the training job
        :type name: str
        :return: A dict contains all the training job info
        """

        return self.get_conn().describe_training_job(TrainingJobName=name)

    def describe_training_job_with_log(self, job_name, positions, stream_names,
                                       instance_count, state, last_description,
                                       last_describe_job_call):
        """
        Return the training job info associated with job_name and print CloudWatch logs
        """
        log_group = '/aws/sagemaker/TrainingJobs'

        if len(stream_names) < instance_count:
            # Log streams are created whenever a container starts writing to stdout/err, so this list
            # may be dynamic until we have a stream for every instance.
            logs_conn = self.get_log_conn()
            try:
                streams = logs_conn.describe_log_streams(
                    logGroupName=log_group,
                    logStreamNamePrefix=job_name + '/',
                    orderBy='LogStreamName',
                    limit=instance_count
                )
                stream_names = [s['logStreamName'] for s in streams['logStreams']]
                positions.update([(s, Position(timestamp=0, skip=0))
                                  for s in stream_names if s not in positions])
            except logs_conn.exceptions.ResourceNotFoundException:
                # On the very first training job run on an account, there's no log group until
                # the container starts logging, so ignore any errors thrown about that
                pass

        if len(stream_names) > 0:
            for idx, event in self.multi_stream_iter(log_group, stream_names, positions):
                self.log.info(event['message'])
                ts, count = positions[stream_names[idx]]
                if event['timestamp'] == ts:
                    positions[stream_names[idx]] = Position(timestamp=ts, skip=count + 1)
                else:
                    positions[stream_names[idx]] = Position(timestamp=event['timestamp'], skip=1)

        if state == LogState.COMPLETE:
            return state, last_description, last_describe_job_call

        if state == LogState.JOB_COMPLETE:
            state = LogState.COMPLETE
        elif time.time() - last_describe_job_call >= 30:
            description = self.describe_training_job(job_name)
            last_describe_job_call = time.time()

            if secondary_training_status_changed(description, last_description):
                self.log.info(secondary_training_status_message(description, last_description))
                last_description = description

            status = description['TrainingJobStatus']

            if status not in self.non_terminal_states:
                state = LogState.JOB_COMPLETE
        return state, last_description, last_describe_job_call

    def describe_tuning_job(self, name):
        """
        Return the tuning job info associated with the name

        :param name: the name of the tuning job
        :type name: string
        :return: A dict contains all the tuning job info
        """

        return self.get_conn().describe_hyper_parameter_tuning_job(HyperParameterTuningJobName=name)

    def describe_model(self, name):
        """
        Return the SageMaker model info associated with the name

        :param name: the name of the SageMaker model
        :type name: string
        :return: A dict contains all the model info
        """

        return self.get_conn().describe_model(ModelName=name)

    def describe_transform_job(self, name):
        """
        Return the transform job info associated with the name

        :param name: the name of the transform job
        :type name: string
        :return: A dict contains all the transform job info
        """

        return self.get_conn().describe_transform_job(TransformJobName=name)

    def describe_endpoint_config(self, name):
        """
        Return the endpoint config info associated with the name

        :param name: the name of the endpoint config
        :type name: string
        :return: A dict contains all the endpoint config info
        """

        return self.get_conn().describe_endpoint_config(EndpointConfigName=name)

    def describe_endpoint(self, name):
        """
        :param name: the name of the endpoint
        :type name: string
        :return: A dict contains all the endpoint info
        """

        return self.get_conn().describe_endpoint(EndpointName=name)

    def check_status(self, job_name, key,
                     describe_function, check_interval,
                     max_ingestion_time,
                     non_terminal_states=None):
        """
        Check status of a SageMaker job

        :param job_name: name of the job to check status
        :type job_name: str
        :param key: the key of the response dict
            that points to the state
        :type key: str
        :param describe_function: the function used to retrieve the status
        :type describe_function: python callable
        :param args: the arguments for the function
        :param check_interval: the time interval in seconds which the operator
            will check the status of any SageMaker job
        :type check_interval: int
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
            SageMaker jobs that run longer than this will fail. Setting this to
            None implies no timeout for any SageMaker job.
        :type max_ingestion_time: int
        :param non_terminal_states: the set of nonterminal states
        :type non_terminal_states: set
        :return: response of describe call after job is done
        """
        if not non_terminal_states:
            non_terminal_states = self.non_terminal_states

        sec = 0
        running = True

        while running:
            time.sleep(check_interval)
            sec = sec + check_interval

            try:
                response = describe_function(job_name)
                status = response[key]
                self.log.info('Job still running for %s seconds... '
                              'current status is %s' % (sec, status))
            except KeyError:
                raise AirflowException('Could not get status of the SageMaker job')
            except ClientError:
                raise AirflowException('AWS request failed, check logs for more info')

            if status in non_terminal_states:
                running = True
            elif status in self.failed_states:
                raise AirflowException('SageMaker job failed because %s' % response['FailureReason'])
            else:
                running = False

            if max_ingestion_time and sec > max_ingestion_time:
                # ensure that the job gets killed if the max ingestion time is exceeded
                raise AirflowException('SageMaker job took more than %s seconds', max_ingestion_time)

        self.log.info('SageMaker Job Compeleted')
        response = describe_function(job_name)
        return response

    def check_training_status_with_log(self, job_name, non_terminal_states, failed_states,
                                       wait_for_completion, check_interval, max_ingestion_time):
        """
        Display the logs for a given training job, optionally tailing them until the
        job is complete.

        :param job_name: name of the training job to check status and display logs for
        :type job_name: str
        :param non_terminal_states: the set of non_terminal states
        :type non_terminal_states: set
        :param failed_states: the set of failed states
        :type failed_states: set
        :param wait_for_completion: Whether to keep looking for new log entries
            until the job completes
        :type wait_for_completion: bool
        :param check_interval: The interval in seconds between polling for new log entries and job completion
        :type check_interval: int
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
            SageMaker jobs that run longer than this will fail. Setting this to
            None implies no timeout for any SageMaker job.
        :type max_ingestion_time: int
        :return: None
        """

        sec = 0
        description = self.describe_training_job(job_name)
        self.log.info(secondary_training_status_message(description, None))
        instance_count = description['ResourceConfig']['InstanceCount']
        status = description['TrainingJobStatus']

        stream_names = []  # The list of log streams
        positions = {}     # The current position in each stream, map of stream name -> position

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
        last_describe_job_call = time.time()
        last_description = description

        while True:
            time.sleep(check_interval)
            sec = sec + check_interval

            state, last_description, last_describe_job_call = \
                self.describe_training_job_with_log(job_name, positions, stream_names,
                                                    instance_count, state, last_description,
                                                    last_describe_job_call)
            if state == LogState.COMPLETE:
                break

            if max_ingestion_time and sec > max_ingestion_time:
                # ensure that the job gets killed if the max ingestion time is exceeded
                raise AirflowException('SageMaker job took more than %s seconds', max_ingestion_time)

        if wait_for_completion:
            status = last_description['TrainingJobStatus']
            if status in failed_states:
                reason = last_description.get('FailureReason', '(No reason provided)')
                raise AirflowException('Error training {}: {} Reason: {}'.format(job_name, status, reason))
            billable_time = (last_description['TrainingEndTime'] - last_description['TrainingStartTime']) \
                * instance_count
            self.log.info('Billable seconds:{}'.format(int(billable_time.total_seconds()) + 1))

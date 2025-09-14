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

import datetime
from unittest import mock

import pytest

from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.amazon.aws.executors.batch.utils import (
    CONFIG_DEFAULTS,
    CONFIG_GROUP_NAME,
    AllBatchConfigKeys,
    BatchExecutorException,
    BatchJob,
    BatchJobCollection,
    BatchJobInfo,
    BatchQueuedJob,
    BatchSubmitJobKwargsConfigKeys,
)
from airflow.utils.state import State


class TestBatchQueuedJob:
    """Tests for the BatchQueuedJob dataclass."""

    def test_batch_queued_job_creation(self):
        """Test BatchQueuedJob object creation."""
        key = mock.Mock(spec=TaskInstanceKey)
        command = ["airflow", "tasks", "run"]
        queue = "default_queue"
        executor_config = {"key": "value"}
        attempt_number = 1
        next_attempt_time = datetime.datetime.now()

        queued_job = BatchQueuedJob(
            key=key,
            command=command,
            queue=queue,
            executor_config=executor_config,
            attempt_number=attempt_number,
            next_attempt_time=next_attempt_time,
        )

        assert queued_job.key is key
        assert queued_job.command == command
        assert queued_job.queue == queue
        assert queued_job.executor_config == executor_config
        assert queued_job.attempt_number == attempt_number
        assert queued_job.next_attempt_time is next_attempt_time


class TestBatchJobInfo:
    """Tests for the BatchJobInfo dataclass."""

    def test_batch_job_info_creation(self):
        """Test BatchJobInfo object creation."""
        cmd = ["airflow", "tasks", "run"]
        queue = "default_queue"
        config = {"key": "value"}

        job_info = BatchJobInfo(cmd=cmd, queue=queue, config=config)

        assert job_info.cmd == cmd
        assert job_info.queue == queue
        assert job_info.config == config


class TestBatchJob:
    """Tests for the BatchJob class."""

    @pytest.mark.parametrize(
        "batch_status, expected_airflow_state",
        [
            ("SUBMITTED", State.QUEUED),
            ("PENDING", State.QUEUED),
            ("RUNNABLE", State.QUEUED),
            ("STARTING", State.QUEUED),
            ("RUNNING", State.RUNNING),
            ("SUCCEEDED", State.SUCCESS),
            ("FAILED", State.FAILED),
            ("UNKNOWN_STATUS", State.QUEUED),  # Default case
        ],
    )
    def test_get_job_state_mappings(self, batch_status, expected_airflow_state):
        """Test job state mappings from AWS Batch status to Airflow state."""
        job = BatchJob(job_id="job_id_123", status=batch_status)
        assert job.get_job_state() == expected_airflow_state

    def test_repr_method(self):
        """Test the __repr__ method for a meaningful string representation."""
        job_id = "test-job-123"
        status = "RUNNING"
        job = BatchJob(job_id=job_id, status=status)
        expected_repr = f"({job_id} -> {status}, {State.RUNNING})"
        assert repr(job) == expected_repr

    def test_status_reason_initialization(self):
        """Test BatchJob initialization with an optional status_reason."""
        job_id = "job-456"
        status = "FAILED"
        status_reason = "Insufficient resources"
        job = BatchJob(job_id=job_id, status=status, status_reason=status_reason)
        assert job.job_id == job_id
        assert job.status == status
        assert job.status_reason == status_reason

    def test_status_reason_default(self):
        """Test BatchJob initialization with a default status_reason."""
        job_id = "job-789"
        status = "SUCCEEDED"
        job = BatchJob(job_id=job_id, status=status)
        assert job.job_id == job_id
        assert job.status == status
        assert job.status_reason is None


class TestBatchJobCollection:
    """Tests for the BatchJobCollection class."""

    @pytest.fixture(autouse=True)
    def _setup_collection(self):
        """Set up a BatchJobCollection for testing."""
        self.collection = BatchJobCollection()
        self.key1 = mock.Mock(spec=TaskInstanceKey)
        self.key2 = mock.Mock(spec=TaskInstanceKey)
        self.job_id1 = "batch-job-001"
        self.job_id2 = "batch-job-002"
        self.cmd1 = ["command1"]
        self.cmd2 = ["command2"]
        self.queue1 = "queue1"
        self.queue2 = "queue2"
        self.config1 = {"conf1": "val1"}
        self.config2 = {"conf2": "val2"}

    def test_add_job(self):
        """Test adding a job to the collection."""
        self.collection.add_job(
            job_id=self.job_id1,
            airflow_task_key=self.key1,
            airflow_cmd=self.cmd1,
            queue=self.queue1,
            exec_config=self.config1,
            attempt_number=1,
        )
        assert len(self.collection) == 1
        assert self.collection.key_to_id[self.key1] == self.job_id1
        assert self.collection.id_to_key[self.job_id1] == self.key1
        assert self.collection.failure_count_by_id(self.job_id1) == 1
        assert self.collection.id_to_job_info[self.job_id1].cmd == self.cmd1
        assert self.collection.id_to_job_info[self.job_id1].queue == self.queue1
        assert self.collection.id_to_job_info[self.job_id1].config == self.config1

    def test_add_multiple_jobs(self):
        """Test adding multiple jobs to the collection."""
        self.collection.add_job(
            job_id=self.job_id1,
            airflow_task_key=self.key1,
            airflow_cmd=self.cmd1,
            queue=self.queue1,
            exec_config=self.config1,
            attempt_number=1,
        )
        self.collection.add_job(
            job_id=self.job_id2,
            airflow_task_key=self.key2,
            airflow_cmd=self.cmd2,
            queue=self.queue2,
            exec_config=self.config2,
            attempt_number=2,
        )
        assert len(self.collection) == 2
        assert self.collection.key_to_id[self.key1] == self.job_id1
        assert self.collection.key_to_id[self.key2] == self.job_id2
        assert self.collection.failure_count_by_id(self.job_id1) == 1
        assert self.collection.failure_count_by_id(self.job_id2) == 2

    def test_pop_by_id(self):
        """Test removing a job from the collection by its ID."""
        self.collection.add_job(
            job_id=self.job_id1,
            airflow_task_key=self.key1,
            airflow_cmd=self.cmd1,
            queue=self.queue1,
            exec_config=self.config1,
            attempt_number=1,
        )
        assert len(self.collection) == 1
        popped_key = self.collection.pop_by_id(self.job_id1)
        assert popped_key is self.key1
        assert len(self.collection) == 0
        assert self.job_id1 not in self.collection.id_to_key
        assert self.key1 not in self.collection.key_to_id
        # id_to_job_info is NOT removed by pop_by_id in the current implementation.
        assert self.job_id1 in self.collection.id_to_job_info
        assert self.collection.id_to_job_info[self.job_id1].cmd == self.cmd1
        # id_to_failure_counts is a defaultdict, so accessing a removed key returns 0.
        assert self.collection.id_to_failure_counts[self.job_id1] == 0

    def test_pop_non_existent_job_id(self):
        """Test popping a job ID that does not exist."""
        with pytest.raises(KeyError):
            self.collection.pop_by_id("non-existent-job-id")

    def test_failure_count_by_id(self):
        """Test getting failure count for a job ID."""
        attempt_number = 5
        self.collection.add_job(
            job_id=self.job_id1,
            airflow_task_key=self.key1,
            airflow_cmd=self.cmd1,
            queue=self.queue1,
            exec_config=self.config1,
            attempt_number=attempt_number,
        )
        assert self.collection.failure_count_by_id(self.job_id1) == attempt_number

    def test_failure_count_non_existent_job_id(self):
        """Test getting failure count for a non-existent job ID."""
        # id_to_failure_counts is a defaultdict, so it returns 0 for missing keys.
        assert self.collection.failure_count_by_id("non-existent-job-id") == 0

    def test_increment_failure_count(self):
        """Test incrementing the failure count for a job ID."""
        initial_attempt = 1
        self.collection.add_job(
            job_id=self.job_id1,
            airflow_task_key=self.key1,
            airflow_cmd=self.cmd1,
            queue=self.queue1,
            exec_config=self.config1,
            attempt_number=initial_attempt,
        )
        assert self.collection.failure_count_by_id(self.job_id1) == initial_attempt
        self.collection.increment_failure_count(self.job_id1)
        assert self.collection.failure_count_by_id(self.job_id1) == initial_attempt + 1

    def test_increment_failure_count_non_existent_job_id(self):
        """Test incrementing failure count for a non-existent job ID."""
        # id_to_failure_counts is a defaultdict, so incrementing a missing key sets it to 1.
        self.collection.increment_failure_count("non-existent-job-id")
        assert self.collection.failure_count_by_id("non-existent-job-id") == 1

    def test_get_all_jobs_empty(self):
        """Test getting all job IDs from an empty collection."""
        assert self.collection.get_all_jobs() == []

    def test_get_all_jobs_with_jobs(self):
        """Test getting all job IDs from a collection with jobs."""
        self.collection.add_job(
            job_id=self.job_id1,
            airflow_task_key=self.key1,
            airflow_cmd=self.cmd1,
            queue=self.queue1,
            exec_config=self.config1,
            attempt_number=1,
        )
        self.collection.add_job(
            job_id=self.job_id2,
            airflow_task_key=self.key2,
            airflow_cmd=self.cmd2,
            queue=self.queue2,
            exec_config=self.config2,
            attempt_number=1,
        )
        all_jobs = self.collection.get_all_jobs()
        assert len(all_jobs) == 2
        assert self.job_id1 in all_jobs
        assert self.job_id2 in all_jobs

    def test_len_method(self):
        """Test the __len__ method of the collection."""
        assert len(self.collection) == 0
        self.collection.add_job(
            job_id=self.job_id1,
            airflow_task_key=self.key1,
            airflow_cmd=self.cmd1,
            queue=self.queue1,
            exec_config=self.config1,
            attempt_number=1,
        )
        assert len(self.collection) == 1
        self.collection.add_job(
            job_id=self.job_id2,
            airflow_task_key=self.key2,
            airflow_cmd=self.cmd2,
            queue=self.queue2,
            exec_config=self.config2,
            attempt_number=1,
        )
        assert len(self.collection) == 2
        self.collection.pop_by_id(self.job_id1)
        assert len(self.collection) == 1


class TestConfigKeys:
    """Tests for configuration key constants."""

    def test_batch_submit_job_kwargs_config_keys_values(self):
        """Test BatchSubmitJobKwargsConfigKeys have correct string values."""
        assert BatchSubmitJobKwargsConfigKeys.JOB_NAME == "job_name"
        assert BatchSubmitJobKwargsConfigKeys.JOB_QUEUE == "job_queue"
        assert BatchSubmitJobKwargsConfigKeys.JOB_DEFINITION == "job_definition"
        assert BatchSubmitJobKwargsConfigKeys.EKS_PROPERTIES_OVERRIDE == "eks_properties_override"
        assert BatchSubmitJobKwargsConfigKeys.NODE_OVERRIDE == "node_override"

    def test_all_batch_config_keys_values(self):
        """Test AllBatchConfigKeys have correct string values, including inherited ones."""
        # Test keys specific to AllBatchConfigKeys
        assert AllBatchConfigKeys.MAX_SUBMIT_JOB_ATTEMPTS == "max_submit_job_attempts"
        assert AllBatchConfigKeys.AWS_CONN_ID == "conn_id"
        assert AllBatchConfigKeys.SUBMIT_JOB_KWARGS == "submit_job_kwargs"
        assert AllBatchConfigKeys.REGION_NAME == "region_name"
        assert AllBatchConfigKeys.CHECK_HEALTH_ON_STARTUP == "check_health_on_startup"

        # Test inherited keys from BatchSubmitJobKwargsConfigKeys
        assert AllBatchConfigKeys.JOB_NAME == "job_name"
        assert AllBatchConfigKeys.JOB_QUEUE == "job_queue"
        assert AllBatchConfigKeys.JOB_DEFINITION == "job_definition"
        assert AllBatchConfigKeys.EKS_PROPERTIES_OVERRIDE == "eks_properties_override"
        assert AllBatchConfigKeys.NODE_OVERRIDE == "node_override"

    def test_config_defaults(self):
        """Test that CONFIG_DEFAULTS is a dictionary with expected keys and values."""
        assert isinstance(CONFIG_DEFAULTS, dict)
        assert "conn_id" in CONFIG_DEFAULTS
        assert CONFIG_DEFAULTS["conn_id"] == "aws_default"
        assert "max_submit_job_attempts" in CONFIG_DEFAULTS
        assert CONFIG_DEFAULTS["max_submit_job_attempts"] == "3"
        assert "check_health_on_startup" in CONFIG_DEFAULTS
        assert CONFIG_DEFAULTS["check_health_on_startup"] == "True"

    def test_config_group_name(self):
        """Test that CONFIG_GROUP_NAME is a string."""
        assert isinstance(CONFIG_GROUP_NAME, str)
        assert CONFIG_GROUP_NAME == "aws_batch_executor"


class TestBatchExecutorException:
    """Tests for the BatchExecutorException class."""

    def test_exception_inheritance(self):
        """Test that BatchExecutorException is a subclass of Exception."""
        assert issubclass(BatchExecutorException, Exception)

    def test_exception_can_be_raised_and_caught(self):
        """Test that the exception can be raised and caught."""
        with pytest.raises(BatchExecutorException):
            raise BatchExecutorException("Test exception message")

    def test_exception_message(self):
        """Test that the exception stores the message correctly."""
        error_message = "An unexpected error occurred in the AWS Batch ecosystem."
        exc = BatchExecutorException(error_message)
        assert str(exc) == error_message

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

# pylint: disable=do-not-use-asserts, missing-docstring, redefined-outer-name


"""
Test AwsBatchWaiters

This test suite uses a large suite of moto mocks for the
AWS batch infrastructure.  These infrastructure mocks are
derived from the moto test suite for testing the batch client.

.. seealso::

    - https://github.com/spulec/moto/pull/1197/files
    - https://github.com/spulec/moto/blob/master/tests/test_batch/test_batch.py
"""

import inspect
import unittest
from typing import NamedTuple, Optional

import boto3
import botocore.client
import botocore.exceptions
import botocore.waiter
import mock
import pytest
from moto import mock_batch, mock_ec2, mock_ecs, mock_iam, mock_logs

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.batch_waiters import AwsBatchWaiters

# Use dummy AWS credentials
AWS_REGION = "eu-west-1"
AWS_ACCESS_KEY_ID = "airflow_dummy_key"
AWS_SECRET_ACCESS_KEY = "airflow_dummy_secret"


@pytest.fixture(scope="module")
def aws_region():
    return AWS_REGION


@pytest.fixture(scope="module")
def job_queue_name():
    return "moto_test_job_queue"


@pytest.fixture(scope="module")
def job_definition_name():
    return "moto_test_job_definition"


#
# AWS Clients
#


class AwsClients(NamedTuple):
    batch: "botocore.client.Batch"
    ec2: "botocore.client.EC2"
    ecs: "botocore.client.ECS"
    iam: "botocore.client.IAM"
    log: "botocore.client.CloudWatchLogs"


@pytest.yield_fixture(scope="module")
def batch_client(aws_region):
    with mock_batch():
        yield boto3.client("batch", region_name=aws_region)


@pytest.yield_fixture(scope="module")
def ec2_client(aws_region):
    with mock_ec2():
        yield boto3.client("ec2", region_name=aws_region)


@pytest.yield_fixture(scope="module")
def ecs_client(aws_region):
    with mock_ecs():
        yield boto3.client("ecs", region_name=aws_region)


@pytest.yield_fixture(scope="module")
def iam_client(aws_region):
    with mock_iam():
        yield boto3.client("iam", region_name=aws_region)


@pytest.yield_fixture(scope="module")
def logs_client(aws_region):
    with mock_logs():
        yield boto3.client("logs", region_name=aws_region)


@pytest.fixture(scope="module")
def aws_clients(batch_client, ec2_client, ecs_client, iam_client, logs_client):
    return AwsClients(
        batch=batch_client, ec2=ec2_client, ecs=ecs_client, iam=iam_client, log=logs_client
    )


#
# Batch Infrastructure
#


class Infrastructure:
    aws_region: str
    aws_clients: AwsClients
    vpc_id: Optional[str] = None
    subnet_id: Optional[str] = None
    security_group_id: Optional[str] = None
    iam_arn: Optional[str] = None
    compute_env_name: Optional[str] = None
    compute_env_arn: Optional[str] = None
    job_queue_name: Optional[str] = None
    job_queue_arn: Optional[str] = None
    job_definition_name: Optional[str] = None
    job_definition_arn: Optional[str] = None


def batch_infrastructure(
    aws_clients: AwsClients, aws_region: str, job_queue_name: str, job_definition_name: str
) -> Infrastructure:
    """
    This function is not a fixture so that tests can pass the AWS clients to it and then
    continue to use the infrastructure created by it while the client fixtures are in-tact for
    the duration of a test.
    """

    infrastructure = Infrastructure()
    infrastructure.aws_region = aws_region
    infrastructure.aws_clients = aws_clients

    resp = aws_clients.ec2.create_vpc(CidrBlock="172.30.0.0/24")
    vpc_id = resp["Vpc"]["VpcId"]

    resp = aws_clients.ec2.create_subnet(
        AvailabilityZone=f"{aws_region}a", CidrBlock="172.30.0.0/25", VpcId=vpc_id
    )
    subnet_id = resp["Subnet"]["SubnetId"]

    resp = aws_clients.ec2.create_security_group(
        Description="moto_test_sg_desc", GroupName="moto_test_sg", VpcId=vpc_id
    )
    sg_id = resp["GroupId"]

    resp = aws_clients.iam.create_role(
        RoleName="MotoTestRole", AssumeRolePolicyDocument="moto_test_policy"
    )
    iam_arn = resp["Role"]["Arn"]

    compute_env_name = "moto_test_compute_env"
    resp = aws_clients.batch.create_compute_environment(
        computeEnvironmentName=compute_env_name,
        type="UNMANAGED",
        state="ENABLED",
        serviceRole=iam_arn,
    )
    compute_env_arn = resp["computeEnvironmentArn"]

    resp = aws_clients.batch.create_job_queue(
        jobQueueName=job_queue_name,
        state="ENABLED",
        priority=123,
        computeEnvironmentOrder=[{"order": 123, "computeEnvironment": compute_env_arn}],
    )
    assert resp["jobQueueName"] == job_queue_name
    assert resp["jobQueueArn"]
    job_queue_arn = resp["jobQueueArn"]

    resp = aws_clients.batch.register_job_definition(
        jobDefinitionName=job_definition_name,
        type="container",
        containerProperties={
            "image": "busybox",
            "vcpus": 1,
            "memory": 64,
            "command": ["sleep", "10"],
        },
    )
    assert resp["jobDefinitionName"] == job_definition_name
    assert resp["jobDefinitionArn"]
    job_definition_arn = resp["jobDefinitionArn"]
    assert resp["revision"]
    assert resp["jobDefinitionArn"].endswith(
        "{0}:{1}".format(resp["jobDefinitionName"], resp["revision"])
    )

    infrastructure.vpc_id = vpc_id
    infrastructure.subnet_id = subnet_id
    infrastructure.security_group_id = sg_id
    infrastructure.iam_arn = iam_arn
    infrastructure.compute_env_name = compute_env_name
    infrastructure.compute_env_arn = compute_env_arn
    infrastructure.job_queue_name = job_queue_name
    infrastructure.job_queue_arn = job_queue_arn
    infrastructure.job_definition_name = job_definition_name
    infrastructure.job_definition_arn = job_definition_arn
    return infrastructure


#
# pytest tests
#


def test_aws_batch_waiters(aws_region):
    assert inspect.isclass(AwsBatchWaiters)
    batch_waiters = AwsBatchWaiters(region_name=aws_region)
    assert isinstance(batch_waiters, AwsBatchWaiters)


@mock_batch
@mock_ec2
@mock_ecs
@mock_iam
@mock_logs
def test_aws_batch_job_waiting(aws_clients, aws_region, job_queue_name, job_definition_name):
    """
    Submit batch jobs and wait for various job status indicators or errors.
    These batch job waiter tests can be slow and might need to be marked
    for conditional skips if they take too long, although it seems to
    run in about 30 sec to a minute.

    .. note::
        These tests have no control over how moto transitions the batch job status.

    .. seealso::
        - https://github.com/boto/botocore/blob/develop/botocore/waiter.py
        - https://github.com/spulec/moto/blob/master/moto/batch/models.py#L360
        - https://github.com/spulec/moto/blob/master/tests/test_batch/test_batch.py
    """

    aws_resources = batch_infrastructure(
        aws_clients, aws_region, job_queue_name, job_definition_name
    )
    batch_waiters = AwsBatchWaiters(region_name=aws_resources.aws_region)

    job_exists_waiter = batch_waiters.get_waiter("JobExists")
    assert job_exists_waiter
    assert isinstance(job_exists_waiter, botocore.waiter.Waiter)
    assert job_exists_waiter.__class__.__name__ == "Batch.Waiter.JobExists"

    job_running_waiter = batch_waiters.get_waiter("JobRunning")
    assert job_running_waiter
    assert isinstance(job_running_waiter, botocore.waiter.Waiter)
    assert job_running_waiter.__class__.__name__ == "Batch.Waiter.JobRunning"

    job_complete_waiter = batch_waiters.get_waiter("JobComplete")
    assert job_complete_waiter
    assert isinstance(job_complete_waiter, botocore.waiter.Waiter)
    assert job_complete_waiter.__class__.__name__ == "Batch.Waiter.JobComplete"

    # test waiting on a jobId that does not exist (this throws immediately)
    with pytest.raises(botocore.exceptions.WaiterError) as err:
        job_exists_waiter.config.delay = 0.2
        job_exists_waiter.config.max_attempts = 2
        job_exists_waiter.wait(jobs=["missing-job"])
    assert isinstance(err.value, botocore.exceptions.WaiterError)
    assert "Waiter JobExists failed" in str(err.value)

    # Submit a job and wait for various job status indicators;
    # moto transitions the batch job status automatically.

    job_name = "test-job"
    job_cmd = ['/bin/sh -c "for a in `seq 1 2`; do echo Hello World; sleep 0.25; done"']

    job_response = aws_clients.batch.submit_job(
        jobName=job_name,
        jobQueue=aws_resources.job_queue_arn,
        jobDefinition=aws_resources.job_definition_arn,
        containerOverrides={"command": job_cmd},
    )
    job_id = job_response["jobId"]

    job_description = aws_clients.batch.describe_jobs(jobs=[job_id])
    job_status = [job for job in job_description["jobs"] if job["jobId"] == job_id][0]["status"]
    assert job_status == "PENDING"

    # this should not raise a WaiterError and note there is no 'state' maintained in
    # the waiter that can be checked after calling wait() and it has no return value;
    # see https://github.com/boto/botocore/blob/develop/botocore/waiter.py#L287
    job_exists_waiter.config.delay = 0.2
    job_exists_waiter.config.max_attempts = 20
    job_exists_waiter.wait(jobs=[job_id])

    # test waiting for job completion with too few attempts (possibly before job is running)
    job_complete_waiter.config.delay = 0.1
    job_complete_waiter.config.max_attempts = 1
    with pytest.raises(botocore.exceptions.WaiterError) as err:
        job_complete_waiter.wait(jobs=[job_id])
    assert isinstance(err.value, botocore.exceptions.WaiterError)
    assert "Waiter JobComplete failed: Max attempts exceeded" in str(err.value)

    # wait for job to be running (or complete)
    job_running_waiter.config.delay = 0.25  # sec delays between status checks
    job_running_waiter.config.max_attempts = 50
    job_running_waiter.wait(jobs=[job_id])

    # wait for job completion
    job_complete_waiter.config.delay = 0.25
    job_complete_waiter.config.max_attempts = 50
    job_complete_waiter.wait(jobs=[job_id])

    job_description = aws_clients.batch.describe_jobs(jobs=[job_id])
    job_status = [job for job in job_description["jobs"] if job["jobId"] == job_id][0]["status"]
    assert job_status == "SUCCEEDED"


# pylint: enable=do-not-use-asserts


class TestAwsBatchWaiters(unittest.TestCase):
    @mock.patch.dict("os.environ", AWS_DEFAULT_REGION=AWS_REGION)
    @mock.patch.dict("os.environ", AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID)
    @mock.patch.dict("os.environ", AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY)
    @mock.patch("airflow.providers.amazon.aws.hooks.batch_client.AwsBaseHook")
    def setUp(self, aws_hook_mock):
        self.job_id = "8ba9d676-4108-4474-9dca-8bbac1da9b19"
        self.region_name = AWS_REGION
        self.aws_hook_mock = aws_hook_mock

        self.batch_waiters = AwsBatchWaiters(region_name=self.region_name)
        self.assertEqual(self.batch_waiters.aws_conn_id, None)
        self.assertEqual(self.batch_waiters.region_name, self.region_name)

        # init the mock hook
        self.assertEqual(self.batch_waiters.hook, self.aws_hook_mock.return_value)
        self.aws_hook_mock.assert_called_once_with(aws_conn_id=None)

        # init the mock client
        self.client_mock = self.batch_waiters.client
        self.aws_hook_mock.return_value.get_client_type.assert_called_once_with(
            "batch", region_name=self.region_name
        )

        # don't pause in these unit tests
        self.mock_delay = mock.Mock(return_value=None)
        self.batch_waiters.delay = self.mock_delay
        self.mock_exponential_delay = mock.Mock(return_value=0)
        self.batch_waiters.exponential_delay = self.mock_exponential_delay

    def test_default_config(self):
        # the default config is used when no custom config is provided
        config = self.batch_waiters.default_config
        self.assertEqual(config, self.batch_waiters.waiter_config)

        self.assertIsInstance(config, dict)
        self.assertEqual(config["version"], 2)
        self.assertIsInstance(config["waiters"], dict)

        waiters = list(sorted(config["waiters"].keys()))
        self.assertEqual(waiters, ["JobComplete", "JobExists", "JobRunning"])

    def test_list_waiters(self):
        # the default config is used when no custom config is provided
        config = self.batch_waiters.waiter_config

        self.assertIsInstance(config["waiters"], dict)
        waiters = list(sorted(config["waiters"].keys()))
        self.assertEqual(waiters, ["JobComplete", "JobExists", "JobRunning"])
        self.assertEqual(waiters, self.batch_waiters.list_waiters())

    def test_waiter_model(self):
        model = self.batch_waiters.waiter_model
        self.assertIsInstance(model, botocore.waiter.WaiterModel)

        # test some of the default config
        self.assertEqual(model.version, 2)
        waiters = sorted(model.waiter_names)
        self.assertEqual(waiters, ["JobComplete", "JobExists", "JobRunning"])

        # test errors when requesting a waiter with the wrong name
        with self.assertRaises(ValueError) as e:
            model.get_waiter("JobExist")
        self.assertIn("Waiter does not exist: JobExist", str(e.exception))

        # test some default waiter properties
        waiter = model.get_waiter("JobExists")
        self.assertIsInstance(waiter, botocore.waiter.SingleWaiterConfig)
        self.assertEqual(waiter.max_attempts, 100)
        waiter.max_attempts = 200
        self.assertEqual(waiter.max_attempts, 200)
        self.assertEqual(waiter.delay, 2)
        waiter.delay = 10
        self.assertEqual(waiter.delay, 10)
        self.assertEqual(waiter.operation, "DescribeJobs")

    def test_wait_for_job(self):
        import sys

        # mock delay for speedy test
        mock_jitter = mock.Mock(return_value=0)
        self.batch_waiters.add_jitter = mock_jitter

        with mock.patch.object(self.batch_waiters, "get_waiter") as get_waiter:

            self.batch_waiters.wait_for_job(self.job_id)

            self.assertEqual(
                get_waiter.call_args_list,
                [mock.call("JobExists"), mock.call("JobRunning"), mock.call("JobComplete")],
            )

            mock_waiter = get_waiter.return_value
            mock_waiter.wait.assert_called_with(jobs=[self.job_id])
            self.assertEqual(mock_waiter.wait.call_count, 3)

            mock_config = mock_waiter.config
            self.assertEqual(mock_config.delay, 0)
            self.assertEqual(mock_config.max_attempts, sys.maxsize)

    def test_wait_for_job_raises_for_client_error(self):
        # mock delay for speedy test
        mock_jitter = mock.Mock(return_value=0)
        self.batch_waiters.add_jitter = mock_jitter

        with mock.patch.object(self.batch_waiters, "get_waiter") as get_waiter:
            mock_waiter = get_waiter.return_value
            mock_waiter.wait.side_effect = botocore.exceptions.ClientError(
                error_response={"Error": {"Code": "TooManyRequestsException"}},
                operation_name="get job description",
            )
            with self.assertRaises(AirflowException):
                self.batch_waiters.wait_for_job(self.job_id)

            self.assertEqual(get_waiter.call_args_list, [mock.call("JobExists")])
            mock_waiter.wait.assert_called_with(jobs=[self.job_id])
            self.assertEqual(mock_waiter.wait.call_count, 1)

    def test_wait_for_job_raises_for_waiter_error(self):
        # mock delay for speedy test
        mock_jitter = mock.Mock(return_value=0)
        self.batch_waiters.add_jitter = mock_jitter

        with mock.patch.object(self.batch_waiters, "get_waiter") as get_waiter:
            mock_waiter = get_waiter.return_value
            mock_waiter.wait.side_effect = botocore.exceptions.WaiterError(
                name="JobExists", reason="unit test error", last_response={}
            )
            with self.assertRaises(AirflowException):
                self.batch_waiters.wait_for_job(self.job_id)

            self.assertEqual(get_waiter.call_args_list, [mock.call("JobExists")])
            mock_waiter.wait.assert_called_with(jobs=[self.job_id])
            self.assertEqual(mock_waiter.wait.call_count, 1)


if __name__ == "__main__":
    unittest.main()

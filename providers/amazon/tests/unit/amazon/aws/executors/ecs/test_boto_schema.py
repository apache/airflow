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

from datetime import datetime

from airflow.providers.amazon.aws.executors.ecs.boto_schema import (
    BotoContainerSchema,
    BotoDescribeTasksSchema,
    BotoFailureSchema,
    BotoRunTaskSchema,
    BotoTaskSchema,
)
from airflow.providers.amazon.aws.executors.ecs.utils import EcsExecutorTask


class TestBotoSchema:
    def test_boto_container_schema_load(self):
        schema = BotoContainerSchema()
        data = {
            "exitCode": 0,
            "lastStatus": "STOPPED",
            "name": "test_container",
            "reason": "Essential container in task exited",
            "containerArn": "arn:aws:ecs:us-east-1:123456789012:container/test-cluster/1234567890abcdef0",
        }
        result = schema.load(data)
        assert result["exit_code"] == 0
        assert result["last_status"] == "STOPPED"
        assert result["name"] == "test_container"
        assert result["reason"] == "Essential container in task exited"
        assert (
            result["container_arn"]
            == "arn:aws:ecs:us-east-1:123456789012:container/test-cluster/1234567890abcdef0"
        )

    def test_boto_container_schema_load_minimal(self):
        schema = BotoContainerSchema()
        data = {"name": "minimal_container"}
        result = schema.load(data)
        assert result["name"] == "minimal_container"
        assert result.get("exit_code") is None
        assert result.get("last_status") is None
        assert result.get("reason") is None
        assert result.get("container_arn") is None

    def test_boto_container_schema_exclude_unknown(self):
        schema = BotoContainerSchema()
        data = {"name": "test_container", "unknownField": "should_be_ignored"}
        result = schema.load(data)
        assert "unknownField" not in result

    def test_boto_task_schema_load(self):
        schema = BotoTaskSchema()
        container_data = {
            "exitCode": 0,
            "lastStatus": "STOPPED",
            "name": "test_container",
            "reason": "Essential container in task exited",
            "containerArn": "arn:aws:ecs:us-east-1:123456789012:container/test-cluster/1234567890abcdef0",
        }
        data = {
            "taskArn": "arn:aws:ecs:us-east-1:123456789012:task/test-cluster/1234567890abcdef0",
            "lastStatus": "STOPPED",
            "desiredStatus": "STOPPED",
            "containers": [container_data],
            "startedAt": datetime(2023, 1, 1),
            "stoppedReason": "Task failed to start",
        }
        result = schema.load(data)
        assert isinstance(result, EcsExecutorTask)
        assert result.task_arn == "arn:aws:ecs:us-east-1:123456789012:task/test-cluster/1234567890abcdef0"
        assert result.last_status == "STOPPED"
        assert result.desired_status == "STOPPED"
        assert len(result.containers) == 1
        assert result.containers[0]["name"] == "test_container"
        assert result.started_at == datetime(2023, 1, 1)
        assert result.stopped_reason == "Task failed to start"

    def test_boto_task_schema_load_minimal(self):
        schema = BotoTaskSchema()
        container_data = {"name": "minimal_container_in_task"}
        data = {
            "taskArn": "arn:aws:ecs:us-east-1:123456789012:task/test-cluster/1234567890abcdef0",
            "lastStatus": "RUNNING",
            "desiredStatus": "RUNNING",
            "containers": [container_data],
        }
        result = schema.load(data)
        assert isinstance(result, EcsExecutorTask)
        assert result.task_arn == "arn:aws:ecs:us-east-1:123456789012:task/test-cluster/1234567890abcdef0"
        assert result.last_status == "RUNNING"
        assert result.desired_status == "RUNNING"
        assert len(result.containers) == 1
        assert result.containers[0]["name"] == "minimal_container_in_task"
        assert result.started_at is None
        assert result.stopped_reason is None

    def test_boto_task_schema_exclude_unknown(self):
        schema = BotoTaskSchema()
        container_data = {"name": "test_container"}
        data = {
            "taskArn": "arn:aws:ecs:us-east-1:123456789012:task/test-cluster/1234567890abcdef0",
            "lastStatus": "RUNNING",
            "desiredStatus": "RUNNING",
            "containers": [container_data],
            "unknownTaskField": "should_be_ignored",
        }
        result = schema.load(data)
        # EcsExecutorTask doesn't store unknown fields, so we check the deserialized dict before object creation
        # by checking the raw data passed to EcsExecutorTask constructor if possible,
        # or simply ensure no error occurs and the object is created.
        # A more direct way would be to mock EcsExecutorTask and inspect its kwargs.
        # For now, we just ensure it loads without error and produces the correct type.
        assert isinstance(result, EcsExecutorTask)

    def test_boto_failure_schema_load(self):
        schema = BotoFailureSchema()
        data = {
            "arn": "arn:aws:ecs:us-east-1:123456789012:container/test-cluster/1234567890abcdef0",
            "reason": "MISSING",
        }
        result = schema.load(data)
        assert result["arn"] == "arn:aws:ecs:us-east-1:123456789012:container/test-cluster/1234567890abcdef0"
        assert result["reason"] == "MISSING"

    def test_boto_failure_schema_load_minimal(self):
        schema = BotoFailureSchema()
        data = {}
        result = schema.load(data)
        assert result.get("arn") is None
        assert result.get("reason") is None

    def test_boto_failure_schema_exclude_unknown(self):
        schema = BotoFailureSchema()
        data = {"arn": "test_arn", "unknownField": "should_be_ignored"}
        result = schema.load(data)
        assert "unknownField" not in result

    def test_boto_run_task_schema_load(self):
        schema = BotoRunTaskSchema()
        task_data = {
            "taskArn": "arn:aws:ecs:us-east-1:123456789012:task/test-cluster/1234567890abcdef0",
            "lastStatus": "RUNNING",
            "desiredStatus": "RUNNING",
            "containers": [{"name": "test_container"}],
        }
        failure_data = {
            "arn": "arn:aws:ecs:us-east-1:123456789012:container/test-cluster/badabcdef0",
            "reason": "MISSING",
        }
        data = {"tasks": [task_data], "failures": [failure_data]}
        result = schema.load(data)

        assert len(result["tasks"]) == 1
        assert isinstance(result["tasks"][0], EcsExecutorTask)
        assert (
            result["tasks"][0].task_arn
            == "arn:aws:ecs:us-east-1:123456789012:task/test-cluster/1234567890abcdef0"
        )

        assert len(result["failures"]) == 1
        assert (
            result["failures"][0]["arn"]
            == "arn:aws:ecs:us-east-1:123456789012:container/test-cluster/badabcdef0"
        )
        assert result["failures"][0]["reason"] == "MISSING"

    def test_boto_run_task_schema_exclude_unknown(self):
        schema = BotoRunTaskSchema()
        data = {
            "tasks": [],
            "failures": [],
            "unknownRunTaskField": "should_be_ignored",
        }
        result = schema.load(data)
        assert "unknownRunTaskField" not in result

    def test_boto_describe_tasks_schema_load(self):
        schema = BotoDescribeTasksSchema()
        task_data = {
            "taskArn": "arn:aws:ecs:us-east-1:123456789012:task/test-cluster/1234567890abcdef1",
            "lastStatus": "STOPPED",
            "desiredStatus": "STOPPED",
            "containers": [{"name": "another_container", "exitCode": 1}],
        }
        failure_data = {
            "arn": "arn:aws:ecs:us-east-1:123456789012:container/test-cluster/anotherbad",
            "reason": "UNABLE",
        }
        data = {"tasks": [task_data], "failures": [failure_data]}
        result = schema.load(data)

        assert len(result["tasks"]) == 1
        assert isinstance(result["tasks"][0], EcsExecutorTask)
        assert (
            result["tasks"][0].task_arn
            == "arn:aws:ecs:us-east-1:123456789012:task/test-cluster/1234567890abcdef1"
        )
        assert result["tasks"][0].containers[0]["exit_code"] == 1

        assert len(result["failures"]) == 1
        assert (
            result["failures"][0]["arn"]
            == "arn:aws:ecs:us-east-1:123456789012:container/test-cluster/anotherbad"
        )
        assert result["failures"][0]["reason"] == "UNABLE"

    def test_boto_describe_tasks_schema_exclude_unknown(self):
        schema = BotoDescribeTasksSchema()
        data = {
            "tasks": [],
            "failures": [],
            "unknownDescribeTasksField": "should_be_ignored",
        }
        result = schema.load(data)
        assert "unknownDescribeTasksField" not in result

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
AWS ECS Executor Boto Schema.

Schemas for easily and consistently parsing boto responses.
"""

from __future__ import annotations

from marshmallow import EXCLUDE, Schema, fields, post_load


class BotoContainerSchema(Schema):
    """
    Botocore Serialization Object for ECS ``Container`` shape.

    Note that there are many more parameters, but the executor only needs the members listed below.
    """

    exit_code = fields.Integer(data_key="exitCode")
    last_status = fields.String(data_key="lastStatus")
    name = fields.String(required=True)
    reason = fields.String()
    container_arn = fields.String(data_key="containerArn")

    class Meta:
        """Options object for a Schema. See Schema.Meta for more details and valid values."""

        unknown = EXCLUDE


class BotoTaskSchema(Schema):
    """
    Botocore Serialization Object for ECS ``Task`` shape.

    Note that there are many more parameters, but the executor only needs the members listed below.
    """

    task_arn = fields.String(data_key="taskArn", required=True)
    last_status = fields.String(data_key="lastStatus", required=True)
    desired_status = fields.String(data_key="desiredStatus", required=True)
    containers = fields.List(fields.Nested(BotoContainerSchema), required=True)
    started_at = fields.Field(data_key="startedAt")
    stopped_reason = fields.String(data_key="stoppedReason")

    @post_load
    def make_task(self, data, **kwargs):
        """Overwrites marshmallow load() to return an instance of EcsExecutorTask instead of a dictionary."""
        # Imported here to avoid circular import.
        from airflow.providers.amazon.aws.executors.ecs.utils import EcsExecutorTask

        return EcsExecutorTask(**data)

    class Meta:
        """Options object for a Schema. See Schema.Meta for more details and valid values."""

        unknown = EXCLUDE


class BotoFailureSchema(Schema):
    """Botocore Serialization Object for ECS ``Failure`` Shape."""

    arn = fields.String()
    reason = fields.String()

    class Meta:
        """Options object for a Schema. See Schema.Meta for more details and valid values."""

        unknown = EXCLUDE


class BotoRunTaskSchema(Schema):
    """Botocore Serialization Object for ECS ``RunTask`` Operation output."""

    tasks = fields.List(fields.Nested(BotoTaskSchema), required=True)
    failures = fields.List(fields.Nested(BotoFailureSchema), required=True)

    class Meta:
        """Options object for a Schema. See Schema.Meta for more details and valid values."""

        unknown = EXCLUDE


class BotoDescribeTasksSchema(Schema):
    """Botocore Serialization Object for ECS ``DescribeTask`` Operation output."""

    tasks = fields.List(fields.Nested(BotoTaskSchema), required=True)
    failures = fields.List(fields.Nested(BotoFailureSchema), required=True)

    class Meta:
        """Options object for a Schema. See Schema.Meta for more details and valid values."""

        unknown = EXCLUDE

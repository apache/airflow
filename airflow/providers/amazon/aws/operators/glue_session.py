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

from functools import cached_property
from typing import TYPE_CHECKING, Sequence

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.glue_session import (
    GlueSessionHook,
    GlueSessionProtocol,
    GlueSessionStates,
)
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.triggers.glue_session import GlueSessionReadyTrigger
from airflow.utils.helpers import prune_dict

if TYPE_CHECKING:
    import boto3

    from airflow.utils.context import Context


class GlueSessionBaseOperator(AwsBaseOperator[GlueSessionHook]):
    """This is the base operator for all Glue service operators."""

    aws_hook_class = GlueSessionHook

    @cached_property
    def client(self) -> GlueSessionProtocol | boto3.client:
        """Create and return the GlueSessionHook's client."""
        return self.hook.conn

    def _complete_exec_with_session(self, context, event=None):
        """To be used as trigger callback for operators that return the session description."""
        if event["status"] != "success":
            raise AirflowException(f"Error while waiting for operation on session to complete: {event}")
        session_id = event.get("id")
        # We cannot get the cluster definition from the waiter on success, so we have to query it here.
        details = self.hook.conn.get_session(Id=session_id)["Session"]
        return details


class GlueCreateSessionOperator(GlueSessionBaseOperator):
    """Create an AWS Glue Session.

    AWS Glue is a serverless Spark ETL service for running Spark Jobs on the AWS
    cloud. Language support: Python and Scala.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GlueCreateSessionOperator`

    :param session_id: session id
    :param session_desc: session description
    :param region_name: aws region name (example: us-east-1)
    :param iam_role_name: AWS IAM Role for Glue Session Execution. If set `iam_role_arn` must equal None.
    :param iam_role_arn: AWS IAM Role ARN for Glue Session Execution, If set `iam_role_name` must equal None.
    :param num_of_dpus: Number of AWS Glue DPUs to allocate to this Session (the default is 10).
    :param create_session_kwargs: Extra arguments for Glue Session Creation
    :param wait_for_completion: If True, waits for the session to be ready. (default: True)
    :param waiter_delay: The amount of time in seconds to wait between attempts,
        if not set then the default waiter value will be used.
    :param waiter_max_attempts: The maximum number of attempts to be made,
        if not set then the default waiter value will be used.
    :param deferrable: If True, the operator will wait asynchronously for the session to be ready.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
    :param delete_session_on_kill: If True, Operator will delete the session when task is killed.
    """

    template_fields: Sequence[str] = (
        "session_id",
        "create_session_kwargs",
        "iam_role_name",
        "iam_role_arn",
        "wait_for_completion",
        "deferrable",
    )
    template_fields_renderers = {
        "create_session_kwargs": "json",
    }

    def __init__(
        self,
        *,
        session_id: str = "aws_glue_default_session",
        session_desc: str = "AWS Glue Session with Airflow",
        aws_conn_id: str = "aws_default",
        iam_role_name: str | None = None,
        iam_role_arn: str | None = None,
        num_of_dpus: int | None = None,
        create_session_kwargs: dict | None = None,
        wait_for_completion: bool = True,
        waiter_delay: int = 15,
        waiter_max_attempts: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        session_poll_interval: int | float = 6,
        delete_session_on_kill: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.session_id = session_id
        self.session_desc = session_desc
        self.aws_conn_id = aws_conn_id
        self.iam_role_name = iam_role_name
        self.iam_role_arn = iam_role_arn
        self.create_session_kwargs = create_session_kwargs or {}
        self.wait_for_completion = wait_for_completion
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.session_poll_interval = session_poll_interval
        self.delete_session_on_kill = delete_session_on_kill
        self.deferrable = deferrable

        worker_type_exists = "WorkerType" in self.create_session_kwargs
        num_workers_exists = "NumberOfWorkers" in self.create_session_kwargs

        if self.iam_role_arn and self.iam_role_name:
            raise ValueError("Cannot set iam_role_arn and iam_role_name simultaneously")
        if worker_type_exists and num_workers_exists:
            if num_of_dpus is not None:
                raise ValueError("Cannot specify num_of_dpus with custom WorkerType")
        elif not worker_type_exists and num_workers_exists:
            raise ValueError("Need to specify custom WorkerType when specifying NumberOfWorkers")
        elif worker_type_exists and not num_workers_exists:
            raise ValueError("Need to specify NumberOfWorkers when specifying custom WorkerType")
        elif num_of_dpus is None:
            self.num_of_dpus: int | float = 10
        else:
            self.num_of_dpus = num_of_dpus

    def preprocess_config(self) -> dict:
        default_command = {
            "Name": "glueetl",
            "PythonVersion": "3",
        }
        command = self.create_session_kwargs.pop("Command", default_command)
        if not self.iam_role_arn:
            role_arn = self.hook.expand_role(str(self.iam_role_name))
        else:
            role_arn = str(self.iam_role_arn)

        config = {
            "Id": self.session_id,
            "Description": self.session_desc,
            "Role": role_arn,
            "Command": command,
            **self.create_session_kwargs,
        }

        if hasattr(self, "num_of_dpus"):
            config["MaxCapacity"] = self.num_of_dpus

        return config

    def execute(self, context: Context):
        """Create AWS Glue Session from Airflow.

        :return: the current Glue session ID.
        """
        self.log.info(
            "Initializing AWS Glue Session: %s",
            self.session_id,
        )

        if self.hook.has_session(self.session_id):
            return self.session_id

        config = self.preprocess_config()
        result = self.client.create_session(**config)
        session_details = result["Session"]
        session_state = session_details.get("Status")

        self.log.info("AWS Glue Session: %s created.", self.session_id)

        if session_state == GlueSessionStates.READY:
            self.log.info("AWS Glue Session %r in state: %r.", self.session_id, session_state)
        elif self.deferrable:
            self.defer(
                trigger=GlueSessionReadyTrigger(
                    session_id=self.session_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                    aws_conn_id=self.aws_conn_id,
                    region_name=self.region_name,
                ),
                method_name="_complete_exec_with_session",
            )
            # self.defer raises a special exception, so execution stops here in this case.
        elif self.wait_for_completion:
            self._wait_for_task_ended()

        return self.session_id

    def on_kill(self):
        """Cancel the creating AWS Glue Session."""
        if self.delete_session_on_kill:
            self.log.info("Deleting AWS Glue Session: %s.", self.session_id)
            self.client.delete_session(self.session_id)

    def _wait_for_task_ended(self) -> None:
        if not self.client or not self.session_id:
            return

        waiter = self.client.get_waiter("session_ready")
        waiter.wait(
            Id=self.session_id,
            WaiterConfig=prune_dict(
                {
                    "Delay": self.waiter_delay,
                    "MaxAttempts": self.waiter_max_attempts,
                }
            ),
        )


class GlueDeleteSessionOperator(GlueSessionBaseOperator):
    """Delete an AWS Glue Session.

    AWS Glue is a serverless Spark ETL service for running Spark Jobs on the AWS
    cloud. Language support: Python and Scala.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GlueDeleteSessionOperator`

    :param id: session id
    """

    template_fields: Sequence[str] = ("session_id",)

    def __init__(
        self,
        *args,
        session_id: str,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.session_id = session_id
        self.aws_conn_id = aws_conn_id

    def execute(self, context: Context):
        """Delete AWS Glue Session from Airflow.

        :return: the current Glue session id.
        """
        self.log.info(
            "Initializing AWS Glue Session: %s. Waitinf for completion",
            self.session_id,
        )
        self.client.delete_session(self.session_id)

        self.log.info("AWS Glue Session: %s deleted.", self.session_id)
        return self.session_id

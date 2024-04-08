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

import asyncio
import time

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class GlueSessionHook(AwsBaseHook):
    """
    Interact with AWS Glue Session.

    Provide thick wrapper around :external+boto3:py:class:`boto3.client("glue") <Glue.Client>`.

    :param id: session id
    :param desc: session description
    :param iam_role_name: AWS IAM Role for Glue Session Execution. If set `iam_role_arn` must equal None.
    :param iam_role_arn: AWS IAM Role ARN for Glue Session Execution, If set `iam_role_name` must equal None.
    :param num_of_dpus: Number of AWS Glue DPUs to allocate to this Session
    :param create_session_kwargs: Extra arguments for Glue Session Creation

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(
        self,
        session_id: str | None = None,
        desc: str | None = None,
        iam_role_name: str | None = None,
        iam_role_arn: str | None = None,
        num_of_dpus: int | None = None,
        create_session_kwargs: dict | None = None,
        session_poll_interval: int | float = 6,
        *args,
        **kwargs,
    ) -> None:
        self.session_id = session_id
        self.desc = desc
        self.role_name = iam_role_name
        self.role_arn = iam_role_arn
        self.create_session_kwargs = create_session_kwargs or {}
        self.session_poll_interval = session_poll_interval

        worker_type_exists = "WorkerType" in self.create_session_kwargs
        num_workers_exists = "NumberOfWorkers" in self.create_session_kwargs

        if self.role_arn and self.role_name:
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

        kwargs["client_type"] = "glue"
        super().__init__(*args, **kwargs)

    def create_glue_session_config(self) -> dict:
        default_command = {
            "Name": "glueetl",
            "PythonVersion": "3",
        }
        command = self.create_session_kwargs.pop("Command", default_command)
        if not self.role_arn:
            execution_role = self.get_iam_execution_role()
            role_arn = execution_role["Role"]["Arn"]
        else:
            role_arn = self.role_arn

        config = {
            "Id": self.session_id,
            "Description": self.desc,
            "Role": role_arn,
            "Command": command,
            **self.create_session_kwargs,
        }

        if hasattr(self, "num_of_dpus"):
            config["MaxCapacity"] = self.num_of_dpus

        return config

    def list_sessions(self) -> list:
        """
        Get list of Sessions.

        .. seealso::
            - :external+boto3:py:meth:`Glue.Client.list_sessions`
        """
        return self.conn.list_sessions()

    def get_iam_execution_role(self) -> dict:
        try:
            iam_client = self.get_session(region_name=self.region_name).client(
                "iam", endpoint_url=self.conn_config.endpoint_url, config=self.config, verify=self.verify
            )
            glue_execution_role = iam_client.get_role(RoleName=self.role_name)
            self.log.info("Iam Role Name: %s", self.role_name)
            return glue_execution_role
        except Exception as general_error:
            self.log.error("Failed to create aws glue session, error: %s", general_error)
            raise

    def has_session(self, session_id: str) -> bool:
        """
        Check if the session already exists.

        .. seealso::
            - :external+boto3:py:meth:`Glue.Client.get_session`

        :param session_id: unique sessioin id per AWS account
        :return: Returns True if the session already exists and False if not.
        """
        self.log.info("Checking if session already exists: %s", session_id)

        try:
            self.conn.get_session(Id=session_id)
            return True
        except self.conn.exceptions.EntityNotFoundException:
            return False

    def initialize_session(self) -> str | None:
        """
        Get (or creates) and returns the Session id.

        .. seealso::
            - :external+boto3:py:meth:`Glue.Client.create_session`

        :return:Id of the Session
        """
        if self.has_session(self.session_id):
            return self.session_id

        try:
            config = self.create_glue_session_config()
            self.log.info("Creating session: %s", self.session_id)
            response = self.conn.create_session(**config)
            self.log.info(response)

            return self.session_id
        except Exception as general_error:
            self.log.error("Failed to create aws glue session, error: %s", general_error)
            raise

    def get_session_state(self, session_id: str) -> str:
        """
        Get state of the Glue session; the session state can be provisioning, ready, failed, stopped, stopping or timeout.

        .. seealso::
            - :external+boto3:py:meth:`Glue.Client.get_session`

        :param session_id: unique session id
        :return: State of the Glue session
        """
        session = self.conn.get_session(Id=session_id)
        return session["Session"]["Status"]

    async def async_get_session_state(self, session_id: str) -> str:
        """
        Get state of the Glue session; the session state can be provisioning, ready, failed, stopped, stopping or timeout.

        The async version of get_session_state.
        """
        async with self.async_conn as client:
            session = await client.get_session(Id=session_id)
        return session["Session"]["Status"]

    def session_readiness(self, session_id: str) -> dict[str, str]:
        """
        Wait until Glue session with session_id is ready; return ready state if ready or raises AirflowException.

        :param session_id: unique session id
        :return: Dict of SessionState
        """
        while True:
            session_state = self.get_session_state(session_id)
            ret = self._handle_state(session_state, session_id)
            if ret:
                return ret
            else:
                time.sleep(self.session_poll_interval)

    async def async_session_readiness(self, session_id: str) -> dict[str, str]:
        """
        Wait until Glue session with session_id is ready; return ready state if ready or raises AirflowException.

        :param session_id: unique session id
        :return: Dict of SessionState
        """
        while True:
            session_state = await self.async_get_session_state(session_id)
            ret = self._handle_state(session_state, session_id)
            if ret:
                return ret
            else:
                await asyncio.sleep(self.session_poll_interval)

    def _handle_state(
        self,
        state: str,
        session_id: str,
    ) -> dict | None:
        """Process Glue Session state while polling; used by both sync and async methods."""
        failed_states = ["FAILED", "STOPPED", "TIMEOUT"]
        ready_state = "READY"

        if state is ready_state:
            self.log.info("Session %s State: %s", session_id, state)
            return {"SessionState": state}
        if state in failed_states:
            session_error_message = f"Exiting Session {session_id} State: {state}"
            self.log.info(session_error_message)
            raise AirflowException(session_error_message)
        else:
            self.log.info(
                "Polling for AWS Glue Session %s current run state with status %s",
                session_id,
                state,
            )
            return None

    def delete_session(self, session_id: str) -> str:
        """
        Deletes and returns the Session id.

        .. seealso::
            - :external+boto3:py:meth:`Glue.Client.delete_session`

        :return:Id of the Session
        """
        if not self.has_session(session_id):
            return session_id

        self.log.info("Deleting session: %s", session_id)
        self.conn.delete_session(Id=session_id)

        return session_id

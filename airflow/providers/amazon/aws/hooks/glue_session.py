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

from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.utils import _StringCompareEnum
from airflow.typing_compat import Protocol, runtime_checkable

if TYPE_CHECKING:
    from botocore.waiter import Waiter


class GlueSessionStates(_StringCompareEnum):
    """Contains the possible State values of an Glue Session."""

    READY = "READY"
    PROVISIONING = "PROVISIONING"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"


class GlueSessionHook(AwsBaseHook):
    """
    Interact with AWS Glue Session.

    Provide thick wrapper around :external+boto3:py:class:`boto3.client("glue") <Glue.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
        - `Glue Service \
        <https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api.html>`__
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "glue"
        super().__init__(*args, **kwargs)

    def list_sessions(self) -> list:
        """
        Get list of Sessions.

        .. seealso::
            - :external+boto3:py:meth:`Glue.Client.list_sessions`
        """
        return self.conn.list_sessions()

    def has_session(self, session_id) -> bool:
        """
        Check if the session already exists.

        .. seealso::
            - :external+boto3:py:meth:`Glue.Client.get_session`

        :param session_id: unique session id per AWS account
        :return: Returns True if the session already exists and False if not.
        """
        self.log.info("Checking if session already exists: %s", session_id)

        try:
            self.conn.get_session(Id=session_id)
            return True
        except self.conn.exceptions.EntityNotFoundException:
            return False

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


@runtime_checkable
class GlueSessionProtocol(Protocol):
    """
    A structured Protocol for ``boto3.client('glue')``.

    This is used for type hints on :py:meth:`.GlueOperator.client`.

    .. seealso::

        - https://mypy.readthedocs.io/en/latest/protocols.html
        - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html
    """

    def create_session(self, **kwargs) -> dict:
        """Create a session.

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.create_session
        """
        ...

    def delete_session(self, Id: str) -> dict:
        """Delete the session.

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.delete_session
        """
        ...

    def get_session(self, Id: str) -> dict:
        """Retrieve the session.

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_session
        """
        ...

    def get_waiter(self, waiter_name: str, **kwargs) -> Waiter:
        """Return an object that can wait for some condition..

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_waiter
        """
        ...

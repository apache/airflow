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
from typing import TYPE_CHECKING, Any, Sequence, Union

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.glue_session import GlueSessionHook
from airflow.providers.amazon.aws.triggers.glue_session import GlueSessionReadyTrigger
from airflow.providers.amazon.aws.utils import validate_execute_complete_event

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GlueCreateSessionOperator(BaseOperator):
    """Create an AWS Glue Session.

    AWS Glue is a serverless Spark ETL service for running Spark Jobs on the AWS
    cloud. Language support: Python and Scala.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GlueCreateSessionOperator`

    :param session_id: session id
    :param session_desc: session description
    :param iam_role_name: AWS IAM Role for Glue Session Execution. If set `iam_role_arn` must equal None.
    :param iam_role_arn: AWS IAM Role ARN for Glue Session Execution, If set `iam_role_name` must equal None.
    :param num_of_dpus: Number of AWS Glue DPUs to allocate to this Session
    :param create_session_kwargs: Extra arguments for Glue Session Creation
    """

    template_fields: Sequence[str] = (
        "session_id",
        "create_session_kwargs",
        "iam_role_name",
        "iam_role_arn",
    )
    template_ext: Sequence[str] = ()
    template_fields_renderers = {
        "create_session_kwargs": "json",
    }
    ui_color = "#ededed"

    operator_extra_links = ()

    def __init__(
        self,
        *,
        session_id: str = "aws_glue_default_session",
        session_desc: str = "AWS Glue Session with Airflow",
        aws_conn_id: str = "aws_default",
        region_name: str | None = None,
        iam_role_name: str | None = None,
        iam_role_arn: str | None = None,
        num_of_dpus: int | None = None,
        create_session_kwargs: dict | None = None,
        wait_for_readiness: bool = True,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        session_poll_interval: int | float = 6,
        delete_session_on_kill: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.session_id = session_id
        self.session_desc = session_desc
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.iam_role_name = iam_role_name
        self.iam_role_arn = iam_role_arn
        self.num_of_dpus = num_of_dpus
        self.create_session_kwargs = create_session_kwargs or {}
        self.wait_for_readiness = wait_for_readiness
        self.session_poll_interval = session_poll_interval
        self.delete_session_on_kill = delete_session_on_kill
        self.deferrable = deferrable

    @cached_property
    def glue_session_hook(self) -> GlueSessionHook:
        return GlueSessionHook(
            session_id=self.session_id,
            desc=self.session_desc,
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            iam_role_arn=self.iam_role_arn,
            iam_role_name=self.iam_role_name,
            num_of_dpus=self.num_of_dpus,
            create_session_kwargs=self.create_session_kwargs,
        )

    def execute(self, context: Context):
        """Create AWS Glue Session from Airflow.

        :return: the current Glue session ID.
        """
        self.log.info(
            "Initializing AWS Glue Session: %s. Waiting for completion",
            self.session_id,
        )
        self.glue_session_hook.initialize_session()

        self.log.info("AWS Glue Session: %s created.", self.session_id)

        if self.deferrable:
            self.defer(
                trigger=GlueSessionReadyTrigger(
                    session_id=self.session_id,
                    aws_conn_id=self.aws_conn_id,
                    session_poll_interval=self.session_poll_interval,
                ),
                method_name="execute_complete",
            )
        elif self.wait_for_readiness:
            glue_session = self.glue_session_hook.session_readiness(self.session_id)
            self.log.info(
                "AWS Glue Session: %s status: %s",
                self.session_id,
                glue_session["SessionState"],
            )
        else:
            self.log.info("AWS Glue Session: %s", self.session_id)
        return self.session_id

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> str:
        event = validate_execute_complete_event(event)

        if event["status"] != "ready":
            raise AirflowException(f"Error in glue session: {event}")
        return event["value"]

    def on_kill(self):
        """Cancel the creating AWS Glue Session."""
        if self.delete_session_on_kill:
            self.log.info("Deleting AWS Glue Session: %s.", self.session_id)
            self.glue_session_hook.delete_session(self.session_id)


class GlueDeleteSessionOperator(BaseOperator):
    """Delete an AWS Glue Session.

    AWS Glue is a serverless Spark ETL service for running Spark Jobs on the AWS
    cloud. Language support: Python and Scala.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GlueDeleteSessionOperator`

    :param id: session id
    """

    template_fields: Sequence[str] = ("session_id",)
    template_ext: Sequence[str] = ()
    template_fields_renderers = {}
    ui_color = "#ededed"

    operator_extra_links = ()

    def __init__(
        self,
        *args,
        session_id: Union[str, None] = None,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.id = session_id
        self.aws_conn_id = aws_conn_id

    @cached_property
    def glue_session_hook(self) -> GlueSessionHook:
        return GlueSessionHook(
            id=self.id,
            aws_conn_id=self.aws_conn_id,
        )

    def execute(self, context: Context):
        """Delete AWS Glue Session from Airflow.

        :return: the current Glue session id.
        """
        self.log.info(
            "Initializing AWS Glue Session: %s. Waitinf for completion",
            self.id,
        )
        self.glue_session_hook.delete_session()

        self.log.info("AWS Glue Session: %s deleted.", self.id)
        return self.id

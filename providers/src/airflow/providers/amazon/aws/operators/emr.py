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

import ast
from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any, Sequence
from uuid import uuid4

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.emr import (
    EmrContainerHook,
    EmrHook,
    EmrServerlessHook,
)
from airflow.providers.amazon.aws.links.emr import (
    EmrClusterLink,
    EmrLogsLink,
    EmrServerlessCloudWatchLogsLink,
    EmrServerlessDashboardLink,
    EmrServerlessLogsLink,
    EmrServerlessS3LogsLink,
    get_log_uri,
)
from airflow.providers.amazon.aws.triggers.emr import (
    EmrAddStepsTrigger,
    EmrContainerTrigger,
    EmrCreateJobFlowTrigger,
    EmrServerlessCancelJobsTrigger,
    EmrServerlessCreateApplicationTrigger,
    EmrServerlessDeleteApplicationTrigger,
    EmrServerlessStartApplicationTrigger,
    EmrServerlessStartJobTrigger,
    EmrServerlessStopApplicationTrigger,
    EmrTerminateJobFlowTrigger,
)
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.providers.amazon.aws.utils.waiter import waiter
from airflow.providers.amazon.aws.utils.waiter_with_logging import wait
from airflow.utils.helpers import exactly_one, prune_dict
from airflow.utils.types import NOTSET, ArgNotSet

if TYPE_CHECKING:
    from airflow.utils.context import Context


class EmrAddStepsOperator(BaseOperator):
    """
    An operator that adds steps to an existing EMR job_flow.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EmrAddStepsOperator`

    :param job_flow_id: id of the JobFlow to add steps to. (templated)
    :param job_flow_name: name of the JobFlow to add steps to. Use as an alternative to passing
        job_flow_id. will search for id of JobFlow with matching name in one of the states in
        param cluster_states. Exactly one cluster like this should exist or will fail. (templated)
    :param cluster_states: Acceptable cluster states when searching for JobFlow id by job_flow_name.
        (templated)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param steps: boto3 style steps or reference to a steps file (must be '.json') to
        be added to the jobflow. (templated)
    :param wait_for_completion: If True, the operator will wait for all the steps to be completed.
    :param execution_role_arn: The ARN of the runtime role for a step on the cluster.
    :param do_xcom_push: if True, job_flow_id is pushed to XCom with key job_flow_id.
    :param wait_for_completion: Whether to wait for job run completion. (default: True)
    :param deferrable: If True, the operator will wait asynchronously for the job to complete.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
    """

    template_fields: Sequence[str] = (
        "job_flow_id",
        "job_flow_name",
        "cluster_states",
        "steps",
        "execution_role_arn",
    )
    template_ext: Sequence[str] = (".json",)
    template_fields_renderers = {"steps": "json"}
    ui_color = "#f9c915"
    operator_extra_links = (
        EmrClusterLink(),
        EmrLogsLink(),
    )

    def __init__(
        self,
        *,
        job_flow_id: str | None = None,
        job_flow_name: str | None = None,
        cluster_states: list[str] | None = None,
        aws_conn_id: str | None = "aws_default",
        steps: list[dict] | str | None = None,
        wait_for_completion: bool = False,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        execution_role_arn: str | None = None,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        **kwargs,
    ):
        if not exactly_one(job_flow_id is None, job_flow_name is None):
            raise AirflowException(
                "Exactly one of job_flow_id or job_flow_name must be specified."
            )
        super().__init__(**kwargs)
        cluster_states = cluster_states or []
        steps = steps or []
        self.aws_conn_id = aws_conn_id
        self.job_flow_id = job_flow_id
        self.job_flow_name = job_flow_name
        self.cluster_states = cluster_states
        self.steps = steps
        self.wait_for_completion = False if deferrable else wait_for_completion
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.execution_role_arn = execution_role_arn
        self.deferrable = deferrable

    def execute(self, context: Context) -> list[str]:
        emr_hook = EmrHook(aws_conn_id=self.aws_conn_id)

        job_flow_id = self.job_flow_id or emr_hook.get_cluster_id_by_name(
            str(self.job_flow_name), self.cluster_states
        )

        if not job_flow_id:
            raise AirflowException(f"No cluster found for name: {self.job_flow_name}")

        if self.do_xcom_push:
            context["ti"].xcom_push(key="job_flow_id", value=job_flow_id)

        EmrClusterLink.persist(
            context=context,
            operator=self,
            region_name=emr_hook.conn_region_name,
            aws_partition=emr_hook.conn_partition,
            job_flow_id=job_flow_id,
        )
        EmrLogsLink.persist(
            context=context,
            operator=self,
            region_name=emr_hook.conn_region_name,
            aws_partition=emr_hook.conn_partition,
            job_flow_id=self.job_flow_id,
            log_uri=get_log_uri(emr_client=emr_hook.conn, job_flow_id=job_flow_id),
        )

        self.log.info("Adding steps to %s", job_flow_id)

        # steps may arrive as a string representing a list
        # e.g. if we used XCom or a file then: steps="[{ step1 }, { step2 }]"
        steps = self.steps
        if isinstance(steps, str):
            steps = ast.literal_eval(steps)
        step_ids = emr_hook.add_job_flow_steps(
            job_flow_id=job_flow_id,
            steps=steps,
            wait_for_completion=self.wait_for_completion,
            waiter_delay=self.waiter_delay,
            waiter_max_attempts=self.waiter_max_attempts,
            execution_role_arn=self.execution_role_arn,
        )
        if self.deferrable:
            self.defer(
                trigger=EmrAddStepsTrigger(
                    job_flow_id=job_flow_id,
                    step_ids=step_ids,
                    aws_conn_id=self.aws_conn_id,
                    waiter_max_attempts=self.waiter_max_attempts,
                    waiter_delay=self.waiter_delay,
                ),
                method_name="execute_complete",
            )

        return step_ids

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None
    ) -> str:
        event = validate_execute_complete_event(event)

        if event["status"] != "success":
            raise AirflowException(f"Error while running steps: {event}")

        self.log.info("Steps completed successfully")
        return event["value"]


class EmrStartNotebookExecutionOperator(BaseOperator):
    """
    An operator that starts an EMR notebook execution.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EmrStartNotebookExecutionOperator`

    :param editor_id: The unique identifier of the EMR notebook to use for notebook execution.
    :param relative_path: The path and file name of the notebook file for this execution,
        relative to the path specified for the EMR notebook.
    :param cluster_id: The unique identifier of the EMR cluster the notebook is attached to.
    :param service_role: The name or ARN of the IAM role that is used as the service role
        for Amazon EMR (the EMR role) for the notebook execution.
    :param notebook_execution_name: Optional name for the notebook execution.
    :param notebook_params: Input parameters in JSON format passed to the EMR notebook at
        runtime for execution.
    :param notebook_instance_security_group_id: The unique identifier of the Amazon EC2
        security group to associate with the EMR notebook for this notebook execution.
    :param master_instance_security_group_id: Optional unique ID of an EC2 security
        group to associate with the master instance of the EMR cluster for this notebook execution.
    :param tags: Optional list of key value pair to associate with the notebook execution.
    :param waiter_max_attempts: Maximum number of tries before failing.
    :param waiter_delay: Number of seconds between polling the state of the notebook.
    """

    template_fields: Sequence[str] = (
        "editor_id",
        "cluster_id",
        "relative_path",
        "service_role",
        "notebook_execution_name",
        "notebook_params",
        "notebook_instance_security_group_id",
        "master_instance_security_group_id",
        "tags",
        "waiter_delay",
        "waiter_max_attempts",
    )

    def __init__(
        self,
        editor_id: str,
        relative_path: str,
        cluster_id: str,
        service_role: str,
        notebook_execution_name: str | None = None,
        notebook_params: str | None = None,
        notebook_instance_security_group_id: str | None = None,
        master_instance_security_group_id: str | None = None,
        tags: list | None = None,
        wait_for_completion: bool = False,
        aws_conn_id: str | None = "aws_default",
        waiter_max_attempts: int | None = None,
        waiter_delay: int | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.editor_id = editor_id
        self.relative_path = relative_path
        self.service_role = service_role
        self.notebook_execution_name = (
            notebook_execution_name or f"emr_notebook_{uuid4()}"
        )
        self.notebook_params = notebook_params or ""
        self.notebook_instance_security_group_id = (
            notebook_instance_security_group_id or ""
        )
        self.tags = tags or []
        self.wait_for_completion = wait_for_completion
        self.cluster_id = cluster_id
        self.aws_conn_id = aws_conn_id
        self.waiter_max_attempts = waiter_max_attempts or 25
        self.waiter_delay = waiter_delay or 60
        self.master_instance_security_group_id = master_instance_security_group_id

    def execute(self, context: Context):
        execution_engine = {
            "Id": self.cluster_id,
            "Type": "EMR",
            "MasterInstanceSecurityGroupId": self.master_instance_security_group_id or "",
        }
        emr_hook = EmrHook(aws_conn_id=self.aws_conn_id)

        response = emr_hook.conn.start_notebook_execution(
            EditorId=self.editor_id,
            RelativePath=self.relative_path,
            NotebookExecutionName=self.notebook_execution_name,
            NotebookParams=self.notebook_params,
            ExecutionEngine=execution_engine,
            ServiceRole=self.service_role,
            NotebookInstanceSecurityGroupId=self.notebook_instance_security_group_id,
            Tags=self.tags,
        )

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Starting notebook execution failed: {response}")

        self.log.info("Notebook execution started: %s", response["NotebookExecutionId"])
        notebook_execution_id = response["NotebookExecutionId"]
        if self.wait_for_completion:
            emr_hook.get_waiter("notebook_running").wait(
                NotebookExecutionId=notebook_execution_id,
                WaiterConfig=prune_dict(
                    {
                        "Delay": self.waiter_delay,
                        "MaxAttempts": self.waiter_max_attempts,
                    }
                ),
            )

            # The old Waiter method raised an exception if the notebook
            # failed, adding that here.  This could maybe be deprecated
            # later to bring it in line with how other waiters behave.
            failure_states = {"FAILED"}
            final_status = emr_hook.conn.describe_notebook_execution(
                NotebookExecutionId=notebook_execution_id
            )["NotebookExecution"]["Status"]
            if final_status in failure_states:
                raise AirflowException(
                    f"Notebook Execution reached failure state {final_status}."
                )

        return notebook_execution_id


class EmrStopNotebookExecutionOperator(BaseOperator):
    """
    An operator that stops a running EMR notebook execution.

     .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EmrStopNotebookExecutionOperator`

    :param notebook_execution_id: The unique identifier of the notebook execution.
    :param wait_for_completion: If True, the operator will wait for the notebook.
        to be in a STOPPED or FINISHED state. Defaults to False.
    :param aws_conn_id: aws connection to use.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param waiter_max_attempts: Maximum number of tries before failing.
    :param waiter_delay: Number of seconds between polling the state of the notebook.
    """

    template_fields: Sequence[str] = (
        "notebook_execution_id",
        "waiter_delay",
        "waiter_max_attempts",
    )

    def __init__(
        self,
        notebook_execution_id: str,
        wait_for_completion: bool = False,
        aws_conn_id: str | None = "aws_default",
        waiter_max_attempts: int | None = None,
        waiter_delay: int | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.notebook_execution_id = notebook_execution_id
        self.wait_for_completion = wait_for_completion
        self.aws_conn_id = aws_conn_id
        self.waiter_max_attempts = waiter_max_attempts or 25
        self.waiter_delay = waiter_delay or 60

    def execute(self, context: Context) -> None:
        emr_hook = EmrHook(aws_conn_id=self.aws_conn_id)
        emr_hook.conn.stop_notebook_execution(
            NotebookExecutionId=self.notebook_execution_id
        )

        if self.wait_for_completion:
            emr_hook.get_waiter("notebook_stopped").wait(
                NotebookExecutionId=self.notebook_execution_id,
                WaiterConfig=prune_dict(
                    {
                        "Delay": self.waiter_delay,
                        "MaxAttempts": self.waiter_max_attempts,
                    }
                ),
            )


class EmrEksCreateClusterOperator(BaseOperator):
    """
    An operator that creates EMR on EKS virtual clusters.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EmrEksCreateClusterOperator`

    :param virtual_cluster_name: The name of the EMR EKS virtual cluster to create.
    :param eks_cluster_name: The EKS cluster used by the EMR virtual cluster.
    :param eks_namespace: namespace used by the EKS cluster.
    :param virtual_cluster_id: The EMR on EKS virtual cluster id.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param tags: The tags assigned to created cluster.
        Defaults to None
    """

    template_fields: Sequence[str] = (
        "virtual_cluster_name",
        "eks_cluster_name",
        "eks_namespace",
    )
    ui_color = "#f9c915"

    def __init__(
        self,
        *,
        virtual_cluster_name: str,
        eks_cluster_name: str,
        eks_namespace: str,
        virtual_cluster_id: str = "",
        aws_conn_id: str | None = "aws_default",
        tags: dict | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.virtual_cluster_name = virtual_cluster_name
        self.eks_cluster_name = eks_cluster_name
        self.eks_namespace = eks_namespace
        self.virtual_cluster_id = virtual_cluster_id
        self.aws_conn_id = aws_conn_id
        self.tags = tags

    @cached_property
    def hook(self) -> EmrContainerHook:
        """Create and return an EmrContainerHook."""
        return EmrContainerHook(self.aws_conn_id)

    def execute(self, context: Context) -> str | None:
        """Create EMR on EKS virtual Cluster."""
        self.virtual_cluster_id = self.hook.create_emr_on_eks_cluster(
            self.virtual_cluster_name,
            self.eks_cluster_name,
            self.eks_namespace,
            self.tags,
        )
        return self.virtual_cluster_id


class EmrContainerOperator(BaseOperator):
    """
    An operator that submits jobs to EMR on EKS virtual clusters.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EmrContainerOperator`

    :param name: The name of the job run.
    :param virtual_cluster_id: The EMR on EKS virtual cluster ID
    :param execution_role_arn: The IAM role ARN associated with the job run.
    :param release_label: The Amazon EMR release version to use for the job run.
    :param job_driver: Job configuration details, e.g. the Spark job parameters.
    :param configuration_overrides: The configuration overrides for the job run,
        specifically either application configuration or monitoring configuration.
    :param client_request_token: The client idempotency token of the job run request.
        Use this if you want to specify a unique ID to prevent two jobs from getting started.
        If no token is provided, a UUIDv4 token will be generated for you.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param wait_for_completion: Whether or not to wait in the operator for the job to complete.
    :param poll_interval: Time (in seconds) to wait between two consecutive calls to check query status on EMR
    :param max_polling_attempts: Maximum number of times to wait for the job run to finish.
        Defaults to None, which will poll until the job is *not* in a pending, submitted, or running state.
    :param job_retry_max_attempts: Maximum number of times to retry when the EMR job fails.
        Defaults to None, which disable the retry.
    :param tags: The tags assigned to job runs.
        Defaults to None
    :param deferrable: Run operator in the deferrable mode.
    """

    template_fields: Sequence[str] = (
        "name",
        "virtual_cluster_id",
        "execution_role_arn",
        "release_label",
        "job_driver",
        "configuration_overrides",
    )
    ui_color = "#f9c915"

    def __init__(
        self,
        *,
        name: str,
        virtual_cluster_id: str,
        execution_role_arn: str,
        release_label: str,
        job_driver: dict,
        configuration_overrides: dict | None = None,
        client_request_token: str | None = None,
        aws_conn_id: str | None = "aws_default",
        wait_for_completion: bool = True,
        poll_interval: int = 30,
        tags: dict | None = None,
        max_polling_attempts: int | None = None,
        job_retry_max_attempts: int | None = None,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.virtual_cluster_id = virtual_cluster_id
        self.execution_role_arn = execution_role_arn
        self.release_label = release_label
        self.job_driver = job_driver
        self.configuration_overrides = configuration_overrides or {}
        self.aws_conn_id = aws_conn_id
        self.client_request_token = client_request_token or str(uuid4())
        self.wait_for_completion = wait_for_completion
        self.poll_interval = poll_interval
        self.max_polling_attempts = max_polling_attempts
        self.job_retry_max_attempts = job_retry_max_attempts
        self.tags = tags
        self.job_id: str | None = None
        self.deferrable = deferrable

    @cached_property
    def hook(self) -> EmrContainerHook:
        """Create and return an EmrContainerHook."""
        return EmrContainerHook(
            self.aws_conn_id,
            virtual_cluster_id=self.virtual_cluster_id,
        )

    def execute(self, context: Context) -> str | None:
        """Run job on EMR Containers."""
        self.job_id = self.hook.submit_job(
            self.name,
            self.execution_role_arn,
            self.release_label,
            self.job_driver,
            self.configuration_overrides,
            self.client_request_token,
            self.tags,
            self.job_retry_max_attempts,
        )
        if self.deferrable:
            query_status = self.hook.check_query_status(job_id=self.job_id)
            self.check_failure(query_status)
            if query_status in EmrContainerHook.SUCCESS_STATES:
                return self.job_id
            timeout = (
                timedelta(seconds=self.max_polling_attempts * self.poll_interval)
                if self.max_polling_attempts
                else self.execution_timeout
            )
            self.defer(
                timeout=timeout,
                trigger=EmrContainerTrigger(
                    virtual_cluster_id=self.virtual_cluster_id,
                    job_id=self.job_id,
                    aws_conn_id=self.aws_conn_id,
                    waiter_delay=self.poll_interval,
                    waiter_max_attempts=self.max_polling_attempts,
                )
                if self.max_polling_attempts
                else EmrContainerTrigger(
                    virtual_cluster_id=self.virtual_cluster_id,
                    job_id=self.job_id,
                    aws_conn_id=self.aws_conn_id,
                    waiter_delay=self.poll_interval,
                ),
                method_name="execute_complete",
            )
        if self.wait_for_completion:
            query_status = self.hook.poll_query_status(
                self.job_id,
                max_polling_attempts=self.max_polling_attempts,
                poll_interval=self.poll_interval,
            )

            self.check_failure(query_status)
            if not query_status or query_status in EmrContainerHook.INTERMEDIATE_STATES:
                raise AirflowException(
                    f"Final state of EMR Containers job is {query_status}. "
                    f"Max tries of poll status exceeded, query_execution_id is {self.job_id}."
                )

        return self.job_id

    def check_failure(self, query_status):
        if query_status in EmrContainerHook.FAILURE_STATES:
            error_message = self.hook.get_job_failure_reason(self.job_id)
            raise AirflowException(
                f"EMR Containers job failed. Final state is {query_status}. "
                f"query_execution_id is {self.job_id}. Error: {error_message}"
            )

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None
    ) -> str:
        event = validate_execute_complete_event(event)

        if event["status"] != "success":
            raise AirflowException(f"Error while running job: {event}")

        return event["job_id"]

    def on_kill(self) -> None:
        """Cancel the submitted job run."""
        if self.job_id:
            self.log.info("Stopping job run with jobId - %s", self.job_id)
            response = self.hook.stop_query(self.job_id)
            http_status_code = None
            try:
                http_status_code = response["ResponseMetadata"]["HTTPStatusCode"]
            except Exception as ex:
                self.log.error("Exception while cancelling query: %s", ex)
            finally:
                if http_status_code is None or http_status_code != 200:
                    self.log.error("Unable to request query cancel on EMR. Exiting")
                else:
                    self.log.info(
                        "Polling EMR for query with id %s to reach final state",
                        self.job_id,
                    )
                    self.hook.poll_query_status(self.job_id)


class EmrCreateJobFlowOperator(BaseOperator):
    """
    Creates an EMR JobFlow, reading the config from the EMR connection.

    A dictionary of JobFlow overrides can be passed that override the config from the connection.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EmrCreateJobFlowOperator`

    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node)
    :param emr_conn_id: :ref:`Amazon Elastic MapReduce Connection <howto/connection:emr>`.
        Use to receive an initial Amazon EMR cluster configuration:
        ``boto3.client('emr').run_job_flow`` request body.
        If this is None or empty or the connection does not exist,
        then an empty initial configuration is used.
    :param job_flow_overrides: boto3 style arguments or reference to an arguments file
        (must be '.json') to override specific ``emr_conn_id`` extra parameters. (templated)
    :param region_name: Region named passed to EmrHook
    :param wait_for_completion: Whether to finish task immediately after creation (False) or wait for jobflow
        completion (True)
    :param waiter_max_attempts: Maximum number of tries before failing.
    :param waiter_delay: Number of seconds between polling the state of the notebook.
    :param deferrable: If True, the operator will wait asynchronously for the crawl to complete.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
    """

    template_fields: Sequence[str] = (
        "job_flow_overrides",
        "waiter_delay",
        "waiter_max_attempts",
    )
    template_ext: Sequence[str] = (".json",)
    template_fields_renderers = {"job_flow_overrides": "json"}
    ui_color = "#f9c915"
    operator_extra_links = (
        EmrClusterLink(),
        EmrLogsLink(),
    )

    def __init__(
        self,
        *,
        aws_conn_id: str | None = "aws_default",
        emr_conn_id: str | None = "emr_default",
        job_flow_overrides: str | dict[str, Any] | None = None,
        region_name: str | None = None,
        wait_for_completion: bool = False,
        waiter_max_attempts: int | None = None,
        waiter_delay: int | None = None,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.emr_conn_id = emr_conn_id
        self.job_flow_overrides = job_flow_overrides or {}
        self.region_name = region_name
        self.wait_for_completion = wait_for_completion
        self.waiter_max_attempts = waiter_max_attempts or 60
        self.waiter_delay = waiter_delay or 60
        self.deferrable = deferrable

    @cached_property
    def _emr_hook(self) -> EmrHook:
        """Create and return an EmrHook."""
        return EmrHook(
            aws_conn_id=self.aws_conn_id,
            emr_conn_id=self.emr_conn_id,
            region_name=self.region_name,
        )

    def execute(self, context: Context) -> str | None:
        self.log.info(
            "Creating job flow using aws_conn_id: %s, emr_conn_id: %s",
            self.aws_conn_id,
            self.emr_conn_id,
        )
        if isinstance(self.job_flow_overrides, str):
            job_flow_overrides: dict[str, Any] = ast.literal_eval(self.job_flow_overrides)
            self.job_flow_overrides = job_flow_overrides
        else:
            job_flow_overrides = self.job_flow_overrides
        response = self._emr_hook.create_job_flow(job_flow_overrides)

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Job flow creation failed: {response}")

        self._job_flow_id = response["JobFlowId"]
        self.log.info("Job flow with id %s created", self._job_flow_id)
        EmrClusterLink.persist(
            context=context,
            operator=self,
            region_name=self._emr_hook.conn_region_name,
            aws_partition=self._emr_hook.conn_partition,
            job_flow_id=self._job_flow_id,
        )
        if self._job_flow_id:
            EmrLogsLink.persist(
                context=context,
                operator=self,
                region_name=self._emr_hook.conn_region_name,
                aws_partition=self._emr_hook.conn_partition,
                job_flow_id=self._job_flow_id,
                log_uri=get_log_uri(
                    emr_client=self._emr_hook.conn, job_flow_id=self._job_flow_id
                ),
            )
        if self.deferrable:
            self.defer(
                trigger=EmrCreateJobFlowTrigger(
                    job_flow_id=self._job_flow_id,
                    aws_conn_id=self.aws_conn_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                ),
                method_name="execute_complete",
                # timeout is set to ensure that if a trigger dies, the timeout does not restart
                # 60 seconds is added to allow the trigger to exit gracefully (i.e. yield TriggerEvent)
                timeout=timedelta(
                    seconds=self.waiter_max_attempts * self.waiter_delay + 60
                ),
            )
        if self.wait_for_completion:
            self._emr_hook.get_waiter("job_flow_waiting").wait(
                ClusterId=self._job_flow_id,
                WaiterConfig=prune_dict(
                    {
                        "Delay": self.waiter_delay,
                        "MaxAttempts": self.waiter_max_attempts,
                    }
                ),
            )
        return self._job_flow_id

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None
    ) -> str:
        event = validate_execute_complete_event(event)

        if event["status"] != "success":
            raise AirflowException(f"Error creating jobFlow: {event}")

        self.log.info("JobFlow created successfully")
        return event["job_flow_id"]

    def on_kill(self) -> None:
        """Terminate the EMR cluster (job flow) unless TerminationProtected is enabled on the cluster."""
        if self._job_flow_id:
            self.log.info("Terminating job flow %s", self._job_flow_id)
            self._emr_hook.conn.terminate_job_flows(JobFlowIds=[self._job_flow_id])


class EmrModifyClusterOperator(BaseOperator):
    """
    An operator that modifies an existing EMR cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EmrModifyClusterOperator`

    :param cluster_id: cluster identifier
    :param step_concurrency_level: Concurrency of the cluster
    :param aws_conn_id: aws connection to uses
    :param aws_conn_id: aws connection to uses
    :param do_xcom_push: if True, cluster_id is pushed to XCom with key cluster_id.
    """

    template_fields: Sequence[str] = ("cluster_id", "step_concurrency_level")
    template_ext: Sequence[str] = ()
    ui_color = "#f9c915"
    operator_extra_links = (
        EmrClusterLink(),
        EmrLogsLink(),
    )

    def __init__(
        self,
        *,
        cluster_id: str,
        step_concurrency_level: int,
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.cluster_id = cluster_id
        self.step_concurrency_level = step_concurrency_level

    def execute(self, context: Context) -> int:
        emr_hook = EmrHook(aws_conn_id=self.aws_conn_id)
        emr = emr_hook.get_conn()

        if self.do_xcom_push:
            context["ti"].xcom_push(key="cluster_id", value=self.cluster_id)

        EmrClusterLink.persist(
            context=context,
            operator=self,
            region_name=emr_hook.conn_region_name,
            aws_partition=emr_hook.conn_partition,
            job_flow_id=self.cluster_id,
        )
        EmrLogsLink.persist(
            context=context,
            operator=self,
            region_name=emr_hook.conn_region_name,
            aws_partition=emr_hook.conn_partition,
            job_flow_id=self.cluster_id,
            log_uri=get_log_uri(emr_client=emr_hook.conn, job_flow_id=self.cluster_id),
        )

        self.log.info("Modifying cluster %s", self.cluster_id)
        response = emr.modify_cluster(
            ClusterId=self.cluster_id, StepConcurrencyLevel=self.step_concurrency_level
        )

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Modify cluster failed: {response}")
        else:
            self.log.info("Steps concurrency level %d", response["StepConcurrencyLevel"])
            return response["StepConcurrencyLevel"]


class EmrTerminateJobFlowOperator(BaseOperator):
    """
    Operator to terminate EMR JobFlows.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EmrTerminateJobFlowOperator`

    :param job_flow_id: id of the JobFlow to terminate. (templated)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param waiter_delay: Time (in seconds) to wait between two consecutive calls to check JobFlow status
    :param waiter_max_attempts: The maximum number of times to poll for JobFlow status.
    :param deferrable: If True, the operator will wait asynchronously for the crawl to complete.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
    """

    template_fields: Sequence[str] = ("job_flow_id",)
    template_ext: Sequence[str] = ()
    ui_color = "#f9c915"
    operator_extra_links = (
        EmrClusterLink(),
        EmrLogsLink(),
    )

    def __init__(
        self,
        *,
        job_flow_id: str,
        aws_conn_id: str | None = "aws_default",
        waiter_delay: int = 60,
        waiter_max_attempts: int = 20,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_flow_id = job_flow_id
        self.aws_conn_id = aws_conn_id
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.deferrable = deferrable

    def execute(self, context: Context) -> None:
        emr_hook = EmrHook(aws_conn_id=self.aws_conn_id)
        emr = emr_hook.get_conn()

        EmrClusterLink.persist(
            context=context,
            operator=self,
            region_name=emr_hook.conn_region_name,
            aws_partition=emr_hook.conn_partition,
            job_flow_id=self.job_flow_id,
        )
        EmrLogsLink.persist(
            context=context,
            operator=self,
            region_name=emr_hook.conn_region_name,
            aws_partition=emr_hook.conn_partition,
            job_flow_id=self.job_flow_id,
            log_uri=get_log_uri(emr_client=emr, job_flow_id=self.job_flow_id),
        )

        self.log.info("Terminating JobFlow %s", self.job_flow_id)
        response = emr.terminate_job_flows(JobFlowIds=[self.job_flow_id])

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"JobFlow termination failed: {response}")

        self.log.info("Terminating JobFlow with id %s", self.job_flow_id)

        if self.deferrable:
            self.defer(
                trigger=EmrTerminateJobFlowTrigger(
                    job_flow_id=self.job_flow_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
                # timeout is set to ensure that if a trigger dies, the timeout does not restart
                # 60 seconds is added to allow the trigger to exit gracefully (i.e. yield TriggerEvent)
                timeout=timedelta(
                    seconds=self.waiter_max_attempts * self.waiter_delay + 60
                ),
            )

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None
    ) -> None:
        event = validate_execute_complete_event(event)

        if event["status"] != "success":
            raise AirflowException(f"Error terminating JobFlow: {event}")

        self.log.info("Jobflow terminated successfully.")


class EmrServerlessCreateApplicationOperator(BaseOperator):
    """
    Operator to create Serverless EMR Application.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EmrServerlessCreateApplicationOperator`

    :param release_label: The EMR release version associated with the application.
    :param job_type: The type of application you want to start, such as Spark or Hive.
    :param wait_for_completion: If true, wait for the Application to start before returning. Default to True.
        If set to False, ``waiter_max_attempts`` and ``waiter_delay`` will only be applied when
        waiting for the application to be in the ``CREATED`` state.
    :param client_request_token: The client idempotency token of the application to create.
      Its value must be unique for each request.
    :param config: Optional dictionary for arbitrary parameters to the boto API create_application call.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :waiter_max_attempts: Number of times the waiter should poll the application to check the state.
        If not set, the waiter will use its default value.
    :param waiter_delay: Number of seconds between polling the state of the application.
    :param deferrable: If True, the operator will wait asynchronously for application to be created.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False, but can be overridden in config file by setting default_deferrable to True)
    """

    def __init__(
        self,
        release_label: str,
        job_type: str,
        client_request_token: str = "",
        config: dict | None = None,
        wait_for_completion: bool = True,
        aws_conn_id: str | None = "aws_default",
        waiter_max_attempts: int | ArgNotSet = NOTSET,
        waiter_delay: int | ArgNotSet = NOTSET,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        **kwargs,
    ):
        waiter_delay = 60 if waiter_delay is NOTSET else waiter_delay
        waiter_max_attempts = 25 if waiter_max_attempts is NOTSET else waiter_max_attempts

        self.aws_conn_id = aws_conn_id
        self.release_label = release_label
        self.job_type = job_type
        self.wait_for_completion = wait_for_completion
        self.kwargs = kwargs
        self.config = config or {}
        self.waiter_max_attempts = int(waiter_max_attempts)  # type: ignore[arg-type]
        self.waiter_delay = int(waiter_delay)  # type: ignore[arg-type]
        self.deferrable = deferrable
        super().__init__(**kwargs)

        self.client_request_token = client_request_token or str(uuid4())

    @cached_property
    def hook(self) -> EmrServerlessHook:
        """Create and return an EmrServerlessHook."""
        return EmrServerlessHook(aws_conn_id=self.aws_conn_id)

    def execute(self, context: Context) -> str | None:
        response = self.hook.conn.create_application(
            clientToken=self.client_request_token,
            releaseLabel=self.release_label,
            type=self.job_type,
            **self.config,
        )
        application_id = response["applicationId"]

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Application Creation failed: {response}")

        self.log.info("EMR serverless application created: %s", application_id)
        if self.deferrable:
            self.defer(
                trigger=EmrServerlessCreateApplicationTrigger(
                    application_id=application_id,
                    aws_conn_id=self.aws_conn_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                ),
                timeout=timedelta(seconds=self.waiter_max_attempts * self.waiter_delay),
                method_name="start_application_deferred",
            )

        waiter = self.hook.get_waiter("serverless_app_created")
        wait(
            waiter=waiter,
            waiter_delay=self.waiter_delay,
            waiter_max_attempts=self.waiter_max_attempts,
            args={"applicationId": application_id},
            failure_message="Serverless Application creation failed",
            status_message="Serverless Application status is",
            status_args=["application.state", "application.stateDetails"],
        )
        self.log.info("Starting application %s", application_id)
        self.hook.conn.start_application(applicationId=application_id)

        if self.wait_for_completion:
            waiter = self.hook.get_waiter("serverless_app_started")
            wait(
                waiter=waiter,
                waiter_max_attempts=self.waiter_max_attempts,
                waiter_delay=self.waiter_delay,
                args={"applicationId": application_id},
                failure_message="Serverless Application failed to start",
                status_message="Serverless Application status is",
                status_args=["application.state", "application.stateDetails"],
            )
        return application_id

    def start_application_deferred(
        self, context: Context, event: dict[str, Any] | None = None
    ) -> None:
        if event is None:
            self.log.error("Trigger error: event is None")
            raise AirflowException("Trigger error: event is None")
        elif event["status"] != "success":
            raise AirflowException(
                f"Application {event['application_id']} failed to create"
            )
        self.log.info("Starting application %s", event["application_id"])
        self.hook.conn.start_application(applicationId=event["application_id"])
        self.defer(
            trigger=EmrServerlessStartApplicationTrigger(
                application_id=event["application_id"],
                aws_conn_id=self.aws_conn_id,
                waiter_delay=self.waiter_delay,
                waiter_max_attempts=self.waiter_max_attempts,
            ),
            timeout=timedelta(seconds=self.waiter_max_attempts * self.waiter_delay),
            method_name="execute_complete",
        )

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None
    ) -> None:
        event = validate_execute_complete_event(event)

        if event["status"] != "success":
            raise AirflowException(
                f"Trigger error: Application failed to start, event is {event}"
            )

        self.log.info("Application %s started", event["application_id"])
        return event["application_id"]


class EmrServerlessStartJobOperator(BaseOperator):
    """
    Operator to start EMR Serverless job.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EmrServerlessStartJobOperator`

    :param application_id: ID of the EMR Serverless application to start.
    :param execution_role_arn: ARN of role to perform action.
    :param job_driver: Driver that the job runs on.
    :param configuration_overrides: Configuration specifications to override existing configurations.
    :param client_request_token: The client idempotency token of the application to create.
      Its value must be unique for each request.
    :param config: Optional dictionary for arbitrary parameters to the boto API start_job_run call.
    :param wait_for_completion: If true, waits for the job to start before returning. Defaults to True.
        If set to False, ``waiter_countdown`` and ``waiter_check_interval_seconds`` will only be applied
        when waiting for the application be to in the ``STARTED`` state.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param name: Name for the EMR Serverless job. If not provided, a default name will be assigned.
    :waiter_max_attempts: Number of times the waiter should poll the application to check the state.
        If not set, the waiter will use its default value.
    :param waiter_delay: Number of seconds between polling the state of the job run.
    :param deferrable: If True, the operator will wait asynchronously for the crawl to complete.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False, but can be overridden in config file by setting default_deferrable to True)
    :param enable_application_ui_links: If True, the operator will generate one-time links to EMR Serverless
        application UIs. The generated links will allow any user with access to the DAG to see the Spark or
        Tez UI or Spark stdout logs. Defaults to False.
    """

    template_fields: Sequence[str] = (
        "application_id",
        "config",
        "execution_role_arn",
        "job_driver",
        "configuration_overrides",
        "name",
        "aws_conn_id",
    )

    template_fields_renderers = {
        "config": "json",
        "configuration_overrides": "json",
    }

    operator_extra_links = (
        EmrServerlessS3LogsLink(),
        EmrServerlessCloudWatchLogsLink(),
        EmrServerlessDashboardLink(),
        EmrServerlessLogsLink(),
    )

    def __init__(
        self,
        application_id: str,
        execution_role_arn: str,
        job_driver: dict,
        configuration_overrides: dict | None = None,
        client_request_token: str = "",
        config: dict | None = None,
        wait_for_completion: bool = True,
        aws_conn_id: str | None = "aws_default",
        name: str | None = None,
        waiter_max_attempts: int | ArgNotSet = NOTSET,
        waiter_delay: int | ArgNotSet = NOTSET,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        enable_application_ui_links: bool = False,
        **kwargs,
    ):
        waiter_delay = 60 if waiter_delay is NOTSET else waiter_delay
        waiter_max_attempts = 25 if waiter_max_attempts is NOTSET else waiter_max_attempts

        self.aws_conn_id = aws_conn_id
        self.application_id = application_id
        self.execution_role_arn = execution_role_arn
        self.job_driver = job_driver
        self.configuration_overrides = configuration_overrides
        self.wait_for_completion = wait_for_completion
        self.config = config or {}
        self.name = name
        self.waiter_max_attempts = int(waiter_max_attempts)  # type: ignore[arg-type]
        self.waiter_delay = int(waiter_delay)  # type: ignore[arg-type]
        self.job_id: str | None = None
        self.deferrable = deferrable
        self.enable_application_ui_links = enable_application_ui_links
        super().__init__(**kwargs)

        self.client_request_token = client_request_token or str(uuid4())

    @cached_property
    def hook(self) -> EmrServerlessHook:
        """Create and return an EmrServerlessHook."""
        return EmrServerlessHook(aws_conn_id=self.aws_conn_id)

    def execute(
        self, context: Context, event: dict[str, Any] | None = None
    ) -> str | None:
        app_state = self.hook.conn.get_application(applicationId=self.application_id)[
            "application"
        ]["state"]
        if app_state not in EmrServerlessHook.APPLICATION_SUCCESS_STATES:
            self.log.info("Application state is %s", app_state)
            self.log.info("Starting application %s", self.application_id)
            self.hook.conn.start_application(applicationId=self.application_id)
            waiter = self.hook.get_waiter("serverless_app_started")
            if self.deferrable:
                self.defer(
                    trigger=EmrServerlessStartApplicationTrigger(
                        application_id=self.application_id,
                        waiter_delay=self.waiter_delay,
                        waiter_max_attempts=self.waiter_max_attempts,
                        aws_conn_id=self.aws_conn_id,
                    ),
                    method_name="execute",
                    timeout=timedelta(
                        seconds=self.waiter_max_attempts * self.waiter_delay
                    ),
                )
            wait(
                waiter=waiter,
                waiter_max_attempts=self.waiter_max_attempts,
                waiter_delay=self.waiter_delay,
                args={"applicationId": self.application_id},
                failure_message="Serverless Application failed to start",
                status_message="Serverless Application status is",
                status_args=["application.state", "application.stateDetails"],
            )
        self.log.info("Starting job on Application: %s", self.application_id)
        self.name = self.name or self.config.pop(
            "name", f"emr_serverless_job_airflow_{uuid4()}"
        )
        args = {
            "clientToken": self.client_request_token,
            "applicationId": self.application_id,
            "executionRoleArn": self.execution_role_arn,
            "jobDriver": self.job_driver,
            "name": self.name,
            **self.config,
        }
        if self.configuration_overrides is not None:
            args["configurationOverrides"] = self.configuration_overrides
        response = self.hook.conn.start_job_run(
            **args,
        )

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"EMR serverless job failed to start: {response}")

        self.job_id = response["jobRunId"]
        self.log.info("EMR serverless job started: %s", self.job_id)

        self.persist_links(context)

        if self.wait_for_completion:
            if self.deferrable:
                self.defer(
                    trigger=EmrServerlessStartJobTrigger(
                        application_id=self.application_id,
                        job_id=self.job_id,
                        waiter_delay=self.waiter_delay,
                        waiter_max_attempts=self.waiter_max_attempts,
                        aws_conn_id=self.aws_conn_id,
                    ),
                    method_name="execute_complete",
                    timeout=timedelta(
                        seconds=self.waiter_max_attempts * self.waiter_delay
                    ),
                )
            else:
                waiter = self.hook.get_waiter("serverless_job_completed")
                wait(
                    waiter=waiter,
                    waiter_max_attempts=self.waiter_max_attempts,
                    waiter_delay=self.waiter_delay,
                    args={"applicationId": self.application_id, "jobRunId": self.job_id},
                    failure_message="Serverless Job failed",
                    status_message="Serverless Job status is",
                    status_args=["jobRun.state", "jobRun.stateDetails"],
                )

        return self.job_id

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None
    ) -> None:
        event = validate_execute_complete_event(event)

        if event["status"] == "success":
            self.log.info("Serverless job completed")
            return event["job_id"]

    def on_kill(self) -> None:
        """
        Cancel the submitted job run.

        Note: this method will not run in deferrable mode.
        """
        if self.job_id:
            self.log.info("Stopping job run with jobId - %s", self.job_id)
            response = self.hook.conn.cancel_job_run(
                applicationId=self.application_id, jobRunId=self.job_id
            )
            http_status_code = (
                response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                if response
                else None
            )
            if http_status_code is None or http_status_code != 200:
                self.log.error(
                    "Unable to request query cancel on EMR Serverless. Exiting"
                )
                return
            self.log.info(
                "Polling EMR Serverless for query with id %s to reach final state",
                self.job_id,
            )
            # This should be replaced with a boto waiter when available.
            waiter(
                get_state_callable=self.hook.conn.get_job_run,
                get_state_args={
                    "applicationId": self.application_id,
                    "jobRunId": self.job_id,
                },
                parse_response=["jobRun", "state"],
                desired_state=EmrServerlessHook.JOB_TERMINAL_STATES,
                failure_states=set(),
                object_type="job",
                action="cancelled",
                countdown=self.waiter_delay * self.waiter_max_attempts,
                check_interval_seconds=self.waiter_delay,
            )

    def is_monitoring_in_job_override(
        self, config_key: str, job_override: dict | None
    ) -> bool:
        """
        Check if monitoring is enabled for the job.

        Note: This is not compatible with application defaults:
        https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/default-configs.html

        This is used to determine what extra links should be shown.
        """
        monitoring_config = (job_override or {}).get("monitoringConfiguration")
        if monitoring_config is None or config_key not in monitoring_config:
            return False

        # CloudWatch can have an "enabled" flag set to False
        if config_key == "cloudWatchLoggingConfiguration":
            return monitoring_config.get(config_key).get("enabled") is True

        return config_key in monitoring_config

    def persist_links(self, context: Context):
        """Populate the relevant extra links for the EMR Serverless jobs."""
        # Persist the EMR Serverless Dashboard link (Spark/Tez UI)
        if self.enable_application_ui_links:
            EmrServerlessDashboardLink.persist(
                context=context,
                operator=self,
                region_name=self.hook.conn_region_name,
                aws_partition=self.hook.conn_partition,
                conn_id=self.hook.aws_conn_id,
                application_id=self.application_id,
                job_run_id=self.job_id,
            )

        # If this is a Spark job, persist the EMR Serverless logs link (Driver stdout)
        if self.enable_application_ui_links and "sparkSubmit" in self.job_driver:
            EmrServerlessLogsLink.persist(
                context=context,
                operator=self,
                region_name=self.hook.conn_region_name,
                aws_partition=self.hook.conn_partition,
                conn_id=self.hook.aws_conn_id,
                application_id=self.application_id,
                job_run_id=self.job_id,
            )

        # Add S3 and/or CloudWatch links if either is enabled
        if self.is_monitoring_in_job_override(
            "s3MonitoringConfiguration", self.configuration_overrides
        ):
            log_uri = (
                (self.configuration_overrides or {})
                .get("monitoringConfiguration", {})
                .get("s3MonitoringConfiguration", {})
                .get("logUri")
            )
            EmrServerlessS3LogsLink.persist(
                context=context,
                operator=self,
                region_name=self.hook.conn_region_name,
                aws_partition=self.hook.conn_partition,
                log_uri=log_uri,
                application_id=self.application_id,
                job_run_id=self.job_id,
            )
            emrs_s3_url = EmrServerlessS3LogsLink().format_link(
                aws_domain=EmrServerlessCloudWatchLogsLink.get_aws_domain(
                    self.hook.conn_partition
                ),
                region_name=self.hook.conn_region_name,
                aws_partition=self.hook.conn_partition,
                log_uri=log_uri,
                application_id=self.application_id,
                job_run_id=self.job_id,
            )
            self.log.info("S3 logs available at: %s", emrs_s3_url)

        if self.is_monitoring_in_job_override(
            "cloudWatchLoggingConfiguration", self.configuration_overrides
        ):
            cloudwatch_config = (
                (self.configuration_overrides or {})
                .get("monitoringConfiguration", {})
                .get("cloudWatchLoggingConfiguration", {})
            )
            log_group_name = cloudwatch_config.get("logGroupName", "/aws/emr-serverless")
            log_stream_prefix = cloudwatch_config.get("logStreamNamePrefix", "")
            log_stream_prefix = f"{log_stream_prefix}/applications/{self.application_id}/jobs/{self.job_id}"

            EmrServerlessCloudWatchLogsLink.persist(
                context=context,
                operator=self,
                region_name=self.hook.conn_region_name,
                aws_partition=self.hook.conn_partition,
                awslogs_group=log_group_name,
                stream_prefix=log_stream_prefix,
            )
            emrs_cloudwatch_url = EmrServerlessCloudWatchLogsLink().format_link(
                aws_domain=EmrServerlessCloudWatchLogsLink.get_aws_domain(
                    self.hook.conn_partition
                ),
                region_name=self.hook.conn_region_name,
                aws_partition=self.hook.conn_partition,
                awslogs_group=log_group_name,
                stream_prefix=log_stream_prefix,
            )
            self.log.info("CloudWatch logs available at: %s", emrs_cloudwatch_url)


class EmrServerlessStopApplicationOperator(BaseOperator):
    """
    Operator to stop an EMR Serverless application.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EmrServerlessStopApplicationOperator`

    :param application_id: ID of the EMR Serverless application to stop.
    :param wait_for_completion: If true, wait for the Application to stop before returning. Default to True
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param force_stop: If set to True, any job for that app that is not in a terminal state will be cancelled.
        Otherwise, trying to stop an app with running jobs will return an error.
        If you want to wait for the jobs to finish gracefully, use
        :class:`airflow.providers.amazon.aws.sensors.emr.EmrServerlessJobSensor`
    :waiter_max_attempts: Number of times the waiter should poll the application to check the state.
        Default is 25.
    :param waiter_delay: Number of seconds between polling the state of the application.
        Default is 60 seconds.
    :param deferrable: If True, the operator will wait asynchronously for the application to stop.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False, but can be overridden in config file by setting default_deferrable to True)
    """

    template_fields: Sequence[str] = ("application_id",)

    def __init__(
        self,
        application_id: str,
        wait_for_completion: bool = True,
        aws_conn_id: str | None = "aws_default",
        waiter_max_attempts: int | ArgNotSet = NOTSET,
        waiter_delay: int | ArgNotSet = NOTSET,
        force_stop: bool = False,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        **kwargs,
    ):
        waiter_delay = 60 if waiter_delay is NOTSET else waiter_delay
        waiter_max_attempts = 25 if waiter_max_attempts is NOTSET else waiter_max_attempts

        self.aws_conn_id = aws_conn_id
        self.application_id = application_id
        self.wait_for_completion = False if deferrable else wait_for_completion
        self.waiter_max_attempts = int(waiter_max_attempts)  # type: ignore[arg-type]
        self.waiter_delay = int(waiter_delay)  # type: ignore[arg-type]
        self.force_stop = force_stop
        self.deferrable = deferrable
        super().__init__(**kwargs)

    @cached_property
    def hook(self) -> EmrServerlessHook:
        """Create and return an EmrServerlessHook."""
        return EmrServerlessHook(aws_conn_id=self.aws_conn_id)

    def execute(self, context: Context) -> None:
        self.log.info("Stopping application: %s", self.application_id)

        if self.force_stop:
            count = self.hook.cancel_running_jobs(
                application_id=self.application_id,
                wait_for_completion=False,
            )
            if count > 0:
                self.log.info(
                    "now waiting for the %s cancelled job(s) to terminate", count
                )
                if self.deferrable:
                    self.defer(
                        trigger=EmrServerlessCancelJobsTrigger(
                            application_id=self.application_id,
                            aws_conn_id=self.aws_conn_id,
                            waiter_delay=self.waiter_delay,
                            waiter_max_attempts=self.waiter_max_attempts,
                        ),
                        timeout=timedelta(
                            seconds=self.waiter_max_attempts * self.waiter_delay
                        ),
                        method_name="stop_application",
                    )
                self.hook.get_waiter("no_job_running").wait(
                    applicationId=self.application_id,
                    states=list(self.hook.JOB_INTERMEDIATE_STATES.union({"CANCELLING"})),
                    WaiterConfig={
                        "Delay": self.waiter_delay,
                        "MaxAttempts": self.waiter_max_attempts,
                    },
                )
            else:
                self.log.info(
                    "no running jobs found with application ID %s", self.application_id
                )

        self.hook.conn.stop_application(applicationId=self.application_id)
        if self.deferrable:
            self.defer(
                trigger=EmrServerlessStopApplicationTrigger(
                    application_id=self.application_id,
                    aws_conn_id=self.aws_conn_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                ),
                timeout=timedelta(seconds=self.waiter_max_attempts * self.waiter_delay),
                method_name="execute_complete",
            )
        if self.wait_for_completion:
            waiter = self.hook.get_waiter("serverless_app_stopped")
            wait(
                waiter=waiter,
                waiter_max_attempts=self.waiter_max_attempts,
                waiter_delay=self.waiter_delay,
                args={"applicationId": self.application_id},
                failure_message="Error stopping application",
                status_message="Serverless Application status is",
                status_args=["application.state", "application.stateDetails"],
            )
            self.log.info(
                "EMR serverless application %s stopped successfully", self.application_id
            )

    def stop_application(
        self, context: Context, event: dict[str, Any] | None = None
    ) -> None:
        if event is None:
            self.log.error("Trigger error: event is None")
            raise AirflowException("Trigger error: event is None")
        elif event["status"] == "success":
            self.hook.conn.stop_application(applicationId=self.application_id)
            self.defer(
                trigger=EmrServerlessStopApplicationTrigger(
                    application_id=self.application_id,
                    aws_conn_id=self.aws_conn_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                ),
                timeout=timedelta(seconds=self.waiter_max_attempts * self.waiter_delay),
                method_name="execute_complete",
            )

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None
    ) -> None:
        event = validate_execute_complete_event(event)

        if event["status"] == "success":
            self.log.info(
                "EMR serverless application %s stopped successfully", self.application_id
            )


class EmrServerlessDeleteApplicationOperator(EmrServerlessStopApplicationOperator):
    """
    Operator to delete EMR Serverless application.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EmrServerlessDeleteApplicationOperator`

    :param application_id: ID of the EMR Serverless application to delete.
    :param wait_for_completion: If true, wait for the Application to be deleted before returning.
        Defaults to True. Note that this operator will always wait for the application to be STOPPED first.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :waiter_max_attempts: Number of times the waiter should poll the application to check the state.
        Defaults to 25.
    :param waiter_delay: Number of seconds between polling the state of the application.
        Defaults to 60 seconds.
    :param deferrable: If True, the operator will wait asynchronously for application to be deleted.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False, but can be overridden in config file by setting default_deferrable to True)
    :param force_stop: If set to True, any job for that app that is not in a terminal state will be cancelled.
        Otherwise, trying to delete an app with running jobs will return an error.
        If you want to wait for the jobs to finish gracefully, use
        :class:`airflow.providers.amazon.aws.sensors.emr.EmrServerlessJobSensor`
    """

    template_fields: Sequence[str] = ("application_id",)

    def __init__(
        self,
        application_id: str,
        wait_for_completion: bool = True,
        aws_conn_id: str | None = "aws_default",
        waiter_max_attempts: int | ArgNotSet = NOTSET,
        waiter_delay: int | ArgNotSet = NOTSET,
        force_stop: bool = False,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        **kwargs,
    ):
        waiter_delay = 60 if waiter_delay is NOTSET else waiter_delay
        waiter_max_attempts = 25 if waiter_max_attempts is NOTSET else waiter_max_attempts

        self.wait_for_delete_completion = wait_for_completion
        # super stops the app
        super().__init__(
            application_id=application_id,
            # when deleting an app, we always need to wait for it to stop before we can call delete()
            wait_for_completion=True,
            aws_conn_id=aws_conn_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            force_stop=force_stop,
            **kwargs,
        )
        self.deferrable = deferrable
        self.wait_for_delete_completion = False if deferrable else wait_for_completion

    def execute(self, context: Context) -> None:
        # super stops the app (or makes sure it's already stopped)
        super().execute(context)

        self.log.info("Now deleting application: %s", self.application_id)
        response = self.hook.conn.delete_application(applicationId=self.application_id)

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Application deletion failed: {response}")

        if self.deferrable:
            self.defer(
                trigger=EmrServerlessDeleteApplicationTrigger(
                    application_id=self.application_id,
                    aws_conn_id=self.aws_conn_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                ),
                timeout=timedelta(seconds=self.waiter_max_attempts * self.waiter_delay),
                method_name="execute_complete",
            )

        elif self.wait_for_delete_completion:
            waiter = self.hook.get_waiter("serverless_app_terminated")

            wait(
                waiter=waiter,
                waiter_max_attempts=self.waiter_max_attempts,
                waiter_delay=self.waiter_delay,
                args={"applicationId": self.application_id},
                failure_message="Error terminating application",
                status_message="Serverless Application status is",
                status_args=["application.state", "application.stateDetails"],
            )

        self.log.info("EMR serverless application deleted")

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None
    ) -> None:
        event = validate_execute_complete_event(event)

        if event["status"] == "success":
            self.log.info(
                "EMR serverless application %s deleted successfully", self.application_id
            )

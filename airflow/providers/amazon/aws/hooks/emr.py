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

import json
import time
import warnings
from typing import Any

from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.utils.waiter_with_logging import wait


class EmrHook(AwsBaseHook):
    """
    Interact with Amazon Elastic MapReduce Service (EMR).

    Provide thick wrapper around :external+boto3:py:class:`boto3.client("emr") <EMR.Client>`.

    :param emr_conn_id: :ref:`Amazon Elastic MapReduce Connection <howto/connection:emr>`.
        This attribute is only necessary when using
        the :meth:`airflow.providers.amazon.aws.hooks.emr.EmrHook.create_job_flow`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    conn_name_attr = "emr_conn_id"
    default_conn_name = "emr_default"
    conn_type = "emr"
    hook_name = "Amazon Elastic MapReduce"

    def __init__(self, emr_conn_id: str | None = default_conn_name, *args, **kwargs) -> None:
        self.emr_conn_id = emr_conn_id
        kwargs["client_type"] = "emr"
        super().__init__(*args, **kwargs)

    def get_cluster_id_by_name(self, emr_cluster_name: str, cluster_states: list[str]) -> str | None:
        """
        Fetch id of EMR cluster with given name and (optional) states; returns only if single id is found.

        .. seealso::
            - :external+boto3:py:meth:`EMR.Client.list_clusters`

        :param emr_cluster_name: Name of a cluster to find
        :param cluster_states: State(s) of cluster to find
        :return: id of the EMR cluster
        """
        response_iterator = (
            self.get_conn().get_paginator("list_clusters").paginate(ClusterStates=cluster_states)
        )
        matching_clusters = [
            cluster
            for page in response_iterator
            for cluster in page["Clusters"]
            if cluster["Name"] == emr_cluster_name
        ]

        if len(matching_clusters) == 1:
            cluster_id = matching_clusters[0]["Id"]
            self.log.info("Found cluster name = %s id = %s", emr_cluster_name, cluster_id)
            return cluster_id
        elif len(matching_clusters) > 1:
            raise AirflowException(f"More than one cluster found for name {emr_cluster_name}")
        else:
            self.log.info("No cluster found for name %s", emr_cluster_name)
            return None

    def create_job_flow(self, job_flow_overrides: dict[str, Any]) -> dict[str, Any]:
        """
        Create and start running a new cluster (job flow).

        .. seealso::
            - :external+boto3:py:meth:`EMR.Client.run_job_flow`

        This method uses ``EmrHook.emr_conn_id`` to receive the initial Amazon EMR cluster configuration.
        If ``EmrHook.emr_conn_id`` is empty or the connection does not exist, then an empty initial
        configuration is used.

        :param job_flow_overrides: Is used to overwrite the parameters in the initial Amazon EMR configuration
            cluster. The resulting configuration will be used in the
            :external+boto3:py:meth:`EMR.Client.run_job_flow`.

        .. seealso::
            - :ref:`Amazon Elastic MapReduce Connection <howto/connection:emr>`
            - :external+boto3:py:meth:`EMR.Client.run_job_flow`
            - `API RunJobFlow <https://docs.aws.amazon.com/emr/latest/APIReference/API_RunJobFlow.html>`_
        """
        config = {}
        if self.emr_conn_id:
            try:
                emr_conn = self.get_connection(self.emr_conn_id)
            except AirflowNotFoundException:
                warnings.warn(
                    f"Unable to find {self.hook_name} Connection ID {self.emr_conn_id!r}, "
                    "using an empty initial configuration. If you want to get rid of this warning "
                    "message please provide a valid `emr_conn_id` or set it to None.",
                    UserWarning,
                    stacklevel=2,
                )
            else:
                if emr_conn.conn_type and emr_conn.conn_type != self.conn_type:
                    warnings.warn(
                        f"{self.hook_name} Connection expected connection type {self.conn_type!r}, "
                        f"Connection {self.emr_conn_id!r} has conn_type={emr_conn.conn_type!r}. "
                        f"This connection might not work correctly.",
                        UserWarning,
                        stacklevel=2,
                    )
                config = emr_conn.extra_dejson.copy()
        config.update(job_flow_overrides)

        response = self.get_conn().run_job_flow(**config)

        return response

    def add_job_flow_steps(
        self,
        job_flow_id: str,
        steps: list[dict] | str | None = None,
        wait_for_completion: bool = False,
        waiter_delay: int | None = None,
        waiter_max_attempts: int | None = None,
        execution_role_arn: str | None = None,
    ) -> list[str]:
        """
        Add new steps to a running cluster.

        .. seealso::
            - :external+boto3:py:meth:`EMR.Client.add_job_flow_steps`

        :param job_flow_id: The id of the job flow to which the steps are being added
        :param steps: A list of the steps to be executed by the job flow
        :param wait_for_completion: If True, wait for the steps to be completed. Default is False
        :param waiter_delay: The amount of time in seconds to wait between attempts. Default is 5
        :param waiter_max_attempts: The maximum number of attempts to be made. Default is 100
        :param execution_role_arn: The ARN of the runtime role for a step on the cluster.
        """
        config = {}
        waiter_delay = waiter_delay or 30
        waiter_max_attempts = waiter_max_attempts or 60

        if execution_role_arn:
            config["ExecutionRoleArn"] = execution_role_arn
        response = self.get_conn().add_job_flow_steps(JobFlowId=job_flow_id, Steps=steps, **config)

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Adding steps failed: {response}")

        self.log.info("Steps %s added to JobFlow", response["StepIds"])
        if wait_for_completion:
            waiter = self.get_conn().get_waiter("step_complete")
            for step_id in response["StepIds"]:
                try:
                    wait(
                        waiter=waiter,
                        waiter_max_attempts=waiter_max_attempts,
                        waiter_delay=waiter_delay,
                        args={"ClusterId": job_flow_id, "StepId": step_id},
                        failure_message=f"EMR Steps failed: {step_id}",
                        status_message="EMR Step status is",
                        status_args=["Step.Status.State", "Step.Status.StateChangeReason"],
                    )
                except AirflowException as ex:
                    if "EMR Steps failed" in str(ex):
                        resp = self.get_conn().describe_step(ClusterId=job_flow_id, StepId=step_id)
                        failure_details = resp["Step"]["Status"].get("FailureDetails", None)
                        if failure_details:
                            self.log.error("EMR Steps failed: %s", failure_details)
                    raise
        return response["StepIds"]

    def test_connection(self):
        """
        Return failed state for test Amazon Elastic MapReduce Connection (untestable).

        We need to overwrite this method because this hook is based on
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsGenericHook`,
        otherwise it will try to test connection to AWS STS by using the default boto3 credential strategy.
        """
        msg = (
            f"{self.hook_name!r} Airflow Connection cannot be tested, by design it stores "
            f"only key/value pairs and does not make a connection to an external resource."
        )
        return False, msg

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for Amazon Elastic MapReduce Connection."""
        return {
            "hidden_fields": ["host", "schema", "port", "login", "password"],
            "relabeling": {
                "extra": "Run Job Flow Configuration",
            },
            "placeholders": {
                "extra": json.dumps(
                    {
                        "Name": "MyClusterName",
                        "ReleaseLabel": "emr-5.36.0",
                        "Applications": [{"Name": "Spark"}],
                        "Instances": {
                            "InstanceGroups": [
                                {
                                    "Name": "Primary node",
                                    "Market": "SPOT",
                                    "InstanceRole": "MASTER",
                                    "InstanceType": "m5.large",
                                    "InstanceCount": 1,
                                },
                            ],
                            "KeepJobFlowAliveWhenNoSteps": False,
                            "TerminationProtected": False,
                        },
                        "StepConcurrencyLevel": 2,
                    },
                    indent=2,
                ),
            },
        }


class EmrServerlessHook(AwsBaseHook):
    """
    Interact with Amazon EMR Serverless.

    Provide thin wrapper around :py:class:`boto3.client("emr-serverless") <EMRServerless.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    JOB_INTERMEDIATE_STATES = {"PENDING", "RUNNING", "SCHEDULED", "SUBMITTED"}
    JOB_FAILURE_STATES = {"FAILED", "CANCELLING", "CANCELLED"}
    JOB_SUCCESS_STATES = {"SUCCESS"}
    JOB_TERMINAL_STATES = JOB_SUCCESS_STATES.union(JOB_FAILURE_STATES)

    APPLICATION_INTERMEDIATE_STATES = {"CREATING", "STARTING", "STOPPING"}
    APPLICATION_FAILURE_STATES = {"STOPPED", "TERMINATED"}
    APPLICATION_SUCCESS_STATES = {"CREATED", "STARTED"}

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs["client_type"] = "emr-serverless"
        super().__init__(*args, **kwargs)

    def cancel_running_jobs(
        self, application_id: str, waiter_config: dict | None = None, wait_for_completion: bool = True
    ) -> int:
        """
        Cancel jobs in an intermediate state, and return the number of cancelled jobs.

        If wait_for_completion is True, then the method will wait until all jobs are
        cancelled before returning.

        Note: if new jobs are triggered while this operation is ongoing,
        it's going to time out and return an error.
        """
        paginator = self.conn.get_paginator("list_job_runs")
        results_per_response = 50
        iterator = paginator.paginate(
            applicationId=application_id,
            states=list(self.JOB_INTERMEDIATE_STATES),
            PaginationConfig={
                "PageSize": results_per_response,
            },
        )
        count = 0
        for r in iterator:
            job_ids = [jr["id"] for jr in r["jobRuns"]]
            count += len(job_ids)
            if job_ids:
                self.log.info(
                    "Cancelling %s pending job(s) for the application %s so that it can be stopped",
                    len(job_ids),
                    application_id,
                )
                for job_id in job_ids:
                    self.conn.cancel_job_run(applicationId=application_id, jobRunId=job_id)
        if wait_for_completion:
            if count > 0:
                self.log.info("now waiting for the %s cancelled job(s) to terminate", count)
                self.get_waiter("no_job_running").wait(
                    applicationId=application_id,
                    states=list(self.JOB_INTERMEDIATE_STATES.union({"CANCELLING"})),
                    WaiterConfig=waiter_config or {},
                )

        return count


class EmrContainerHook(AwsBaseHook):
    """
    Interact with Amazon EMR Containers (Amazon EMR on EKS).

    Provide thick wrapper around :py:class:`boto3.client("emr-containers") <EMRContainers.Client>`.

    :param virtual_cluster_id: Cluster ID of the EMR on EKS virtual cluster

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    INTERMEDIATE_STATES = (
        "PENDING",
        "SUBMITTED",
        "RUNNING",
    )
    FAILURE_STATES = (
        "FAILED",
        "CANCELLED",
        "CANCEL_PENDING",
    )
    SUCCESS_STATES = ("COMPLETED",)
    TERMINAL_STATES = (
        "COMPLETED",
        "FAILED",
        "CANCELLED",
        "CANCEL_PENDING",
    )

    def __init__(self, *args: Any, virtual_cluster_id: str | None = None, **kwargs: Any) -> None:
        super().__init__(client_type="emr-containers", *args, **kwargs)  # type: ignore
        self.virtual_cluster_id = virtual_cluster_id

    def create_emr_on_eks_cluster(
        self,
        virtual_cluster_name: str,
        eks_cluster_name: str,
        eks_namespace: str,
        tags: dict | None = None,
    ) -> str:
        response = self.conn.create_virtual_cluster(
            name=virtual_cluster_name,
            containerProvider={
                "id": eks_cluster_name,
                "type": "EKS",
                "info": {"eksInfo": {"namespace": eks_namespace}},
            },
            tags=tags or {},
        )

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Create EMR EKS Cluster failed: {response}")
        else:
            self.log.info(
                "Create EMR EKS Cluster success - virtual cluster id %s",
                response["id"],
            )
            return response["id"]

    def submit_job(
        self,
        name: str,
        execution_role_arn: str,
        release_label: str,
        job_driver: dict,
        configuration_overrides: dict | None = None,
        client_request_token: str | None = None,
        tags: dict | None = None,
        retry_max_attempts: int | None = None,
    ) -> str:
        """
        Submit a job to the EMR Containers API and return the job ID.

        A job run is a unit of work, such as a Spark jar, PySpark script,
        or SparkSQL query, that you submit to Amazon EMR on EKS.

        .. seealso::
            - :external+boto3:py:meth:`EMRContainers.Client.start_job_run`

        :param name: The name of the job run.
        :param execution_role_arn: The IAM role ARN associated with the job run.
        :param release_label: The Amazon EMR release version to use for the job run.
        :param job_driver: Job configuration details, e.g. the Spark job parameters.
        :param configuration_overrides: The configuration overrides for the job run,
            specifically either application configuration or monitoring configuration.
        :param client_request_token: The client idempotency token of the job run request.
            Use this if you want to specify a unique ID to prevent two jobs from getting started.
        :param tags: The tags assigned to job runs.
        :param retry_max_attempts: The maximum number of attempts on the job's driver.
        :return: The ID of the job run request.
        """
        params = {
            "name": name,
            "virtualClusterId": self.virtual_cluster_id,
            "executionRoleArn": execution_role_arn,
            "releaseLabel": release_label,
            "jobDriver": job_driver,
            "configurationOverrides": configuration_overrides or {},
            "tags": tags or {},
        }
        if client_request_token:
            params["clientToken"] = client_request_token
        if retry_max_attempts:
            params["retryPolicyConfiguration"] = {
                "maxAttempts": retry_max_attempts,
            }

        response = self.conn.start_job_run(**params)

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Start Job Run failed: {response}")
        else:
            self.log.info(
                "Start Job Run success - Job Id %s and virtual cluster id %s",
                response["id"],
                response["virtualClusterId"],
            )
            return response["id"]

    def get_job_failure_reason(self, job_id: str) -> str | None:
        """
        Fetch the reason for a job failure (e.g. error message). Returns None or reason string.

        .. seealso::
            - :external+boto3:py:meth:`EMRContainers.Client.describe_job_run`

        :param job_id: The ID of the job run request.
        """
        try:
            response = self.conn.describe_job_run(
                virtualClusterId=self.virtual_cluster_id,
                id=job_id,
            )
            failure_reason = response["jobRun"]["failureReason"]
            state_details = response["jobRun"]["stateDetails"]
            return f"{failure_reason} - {state_details}"
        except KeyError:
            self.log.error("Could not get status of the EMR on EKS job")
        except ClientError as ex:
            self.log.error("AWS request failed, check logs for more info: %s", ex)

        return None

    def check_query_status(self, job_id: str) -> str | None:
        """
        Fetch the status of submitted job run. Returns None or one of valid query states.

        .. seealso::
            - :external+boto3:py:meth:`EMRContainers.Client.describe_job_run`

        :param job_id: The ID of the job run request.
        """
        try:
            response = self.conn.describe_job_run(
                virtualClusterId=self.virtual_cluster_id,
                id=job_id,
            )
            return response["jobRun"]["state"]
        except self.conn.exceptions.ResourceNotFoundException:
            # If the job is not found, we raise an exception as something fatal has happened.
            raise AirflowException(f"Job ID {job_id} not found on Virtual Cluster {self.virtual_cluster_id}")
        except ClientError as ex:
            # If we receive a generic ClientError, we swallow the exception so that the
            self.log.error("AWS request failed, check logs for more info: %s", ex)
            return None

    def poll_query_status(
        self,
        job_id: str,
        poll_interval: int = 30,
        max_polling_attempts: int | None = None,
    ) -> str | None:
        """
        Poll the status of submitted job run until query state reaches final state; returns the final state.

        :param job_id: The ID of the job run request.
        :param poll_interval: Time (in seconds) to wait between calls to check query status on EMR
        :param max_polling_attempts: Number of times to poll for query state before function exits
        """
        poll_attempt = 1
        while True:
            query_state = self.check_query_status(job_id)
            if query_state in self.TERMINAL_STATES:
                self.log.info(
                    "Try %s: Query execution completed. Final state is %s", poll_attempt, query_state
                )
                return query_state
            if query_state is None:
                self.log.info("Try %s: Invalid query state. Retrying again", poll_attempt)
            else:
                self.log.info("Try %s: Query is still in non-terminal state - %s", poll_attempt, query_state)
            if (
                max_polling_attempts and poll_attempt >= max_polling_attempts
            ):  # Break loop if max_polling_attempts reached
                return query_state
            poll_attempt += 1
            time.sleep(poll_interval)

    def stop_query(self, job_id: str) -> dict:
        """
        Cancel the submitted job_run.

        .. seealso::
            - :external+boto3:py:meth:`EMRContainers.Client.cancel_job_run`

        :param job_id: The ID of the job run to cancel.
        """
        return self.conn.cancel_job_run(
            virtualClusterId=self.virtual_cluster_id,
            id=job_id,
        )

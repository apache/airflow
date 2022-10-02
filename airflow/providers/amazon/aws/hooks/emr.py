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
import warnings
from time import sleep
from typing import Any, Callable

from botocore.exceptions import ClientError

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class EmrHook(AwsBaseHook):
    """
    Interact with Amazon Elastic MapReduce Service.

    :param emr_conn_id: :ref:`Amazon Elastic MapReduce Connection <howto/connection:emr>`.
        This attribute is only necessary when using
        the :meth:`~airflow.providers.amazon.aws.hooks.emr.EmrHook.create_job_flow` method.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    conn_name_attr = 'emr_conn_id'
    default_conn_name = 'emr_default'
    conn_type = 'emr'
    hook_name = 'Amazon Elastic MapReduce'

    def __init__(self, emr_conn_id: str | None = default_conn_name, *args, **kwargs) -> None:
        self.emr_conn_id = emr_conn_id
        kwargs["client_type"] = "emr"
        super().__init__(*args, **kwargs)

    def get_cluster_id_by_name(self, emr_cluster_name: str, cluster_states: list[str]) -> str | None:
        """
        Fetch id of EMR cluster with given name and (optional) states.
        Will return only if single id is found.

        :param emr_cluster_name: Name of a cluster to find
        :param cluster_states: State(s) of cluster to find
        :return: id of the EMR cluster
        """
        response = self.get_conn().list_clusters(ClusterStates=cluster_states)

        matching_clusters = list(
            filter(lambda cluster: cluster['Name'] == emr_cluster_name, response['Clusters'])
        )

        if len(matching_clusters) == 1:
            cluster_id = matching_clusters[0]['Id']
            self.log.info('Found cluster name = %s id = %s', emr_cluster_name, cluster_id)
            return cluster_id
        elif len(matching_clusters) > 1:
            raise AirflowException(f'More than one cluster found for name {emr_cluster_name}')
        else:
            self.log.info('No cluster found for name %s', emr_cluster_name)
            return None

    def create_job_flow(self, job_flow_overrides: dict[str, Any]) -> dict[str, Any]:
        """
        Create and start running a new cluster (job flow).

        This method uses ``EmrHook.emr_conn_id`` to receive the initial Amazon EMR cluster configuration.
        If ``EmrHook.emr_conn_id`` is empty or the connection does not exist, then an empty initial
        configuration is used.

        :param job_flow_overrides: Is used to overwrite the parameters in the initial Amazon EMR configuration
            cluster. The resulting configuration will be used in the boto3 emr client run_job_flow method.

        .. seealso::
            - :ref:`Amazon Elastic MapReduce Connection <howto/connection:emr>`
            - `API RunJobFlow <https://docs.aws.amazon.com/emr/latest/APIReference/API_RunJobFlow.html>`_
            - `boto3 emr client run_job_flow method <https://boto3.amazonaws.com/v1/documentation/\
               api/latest/reference/services/emr.html#EMR.Client.run_job_flow>`_.
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

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Returns custom UI field behaviour for Amazon Elastic MapReduce Connection."""
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
    Interact with EMR Serverless API.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    JOB_INTERMEDIATE_STATES = {'PENDING', 'RUNNING', 'SCHEDULED', 'SUBMITTED'}
    JOB_FAILURE_STATES = {'FAILED', 'CANCELLING', 'CANCELLED'}
    JOB_SUCCESS_STATES = {'SUCCESS'}
    JOB_TERMINAL_STATES = JOB_SUCCESS_STATES.union(JOB_FAILURE_STATES)

    APPLICATION_INTERMEDIATE_STATES = {'CREATING', 'STARTING', 'STOPPING'}
    APPLICATION_FAILURE_STATES = {'STOPPED', 'TERMINATED'}
    APPLICATION_SUCCESS_STATES = {'CREATED', 'STARTED'}

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs["client_type"] = "emr-serverless"
        super().__init__(*args, **kwargs)

    @cached_property
    def conn(self):
        """Get the underlying boto3 EmrServerlessAPIService client (cached)"""
        return super().conn

    # This method should be replaced with boto waiters which would implement timeouts and backoff nicely.
    def waiter(
        self,
        get_state_callable: Callable,
        get_state_args: dict,
        parse_response: list,
        desired_state: set,
        failure_states: set,
        object_type: str,
        action: str,
        countdown: int = 25 * 60,
        check_interval_seconds: int = 60,
    ) -> None:
        """
        Will run the sensor until it turns True.

        :param get_state_callable: A callable to run until it returns True
        :param get_state_args: Arguments to pass to get_state_callable
        :param parse_response: Dictionary keys to extract state from response of get_state_callable
        :param desired_state: Wait until the getter returns this value
        :param failure_states: A set of states which indicate failure and should throw an
            exception if any are reached before the desired_state
        :param object_type: Used for the reporting string. What are you waiting for? (application, job, etc)
        :param action: Used for the reporting string. What action are you waiting for? (created, deleted, etc)
        :param countdown: Total amount of time the waiter should wait for the desired state
            before timing out (in seconds). Defaults to 25 * 60 seconds.
        :param check_interval_seconds: Number of seconds waiter should wait before attempting
            to retry get_state_callable. Defaults to 60 seconds.
        """
        response = get_state_callable(**get_state_args)
        state: str = self.get_state(response, parse_response)
        while state not in desired_state:
            if state in failure_states:
                raise AirflowException(f'{object_type.title()} reached failure state {state}.')
            if countdown >= check_interval_seconds:
                countdown -= check_interval_seconds
                self.log.info('Waiting for %s to be %s.', object_type.lower(), action.lower())
                sleep(check_interval_seconds)
                state = self.get_state(get_state_callable(**get_state_args), parse_response)
            else:
                message = f'{object_type.title()} still not {action.lower()} after the allocated time limit.'
                self.log.error(message)
                raise RuntimeError(message)

    def get_state(self, response, keys) -> str:
        value = response
        for key in keys:
            if value is not None:
                value = value.get(key, None)
        return value


class EmrContainerHook(AwsBaseHook):
    """
    Interact with AWS EMR Virtual Cluster to run, poll jobs and return job status
    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

    :param virtual_cluster_id: Cluster ID of the EMR on EKS virtual cluster
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

        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise AirflowException(f'Create EMR EKS Cluster failed: {response}')
        else:
            self.log.info(
                "Create EMR EKS Cluster success - virtual cluster id %s",
                response['id'],
            )
            return response['id']

    def submit_job(
        self,
        name: str,
        execution_role_arn: str,
        release_label: str,
        job_driver: dict,
        configuration_overrides: dict | None = None,
        client_request_token: str | None = None,
        tags: dict | None = None,
    ) -> str:
        """
        Submit a job to the EMR Containers API and return the job ID.
        A job run is a unit of work, such as a Spark jar, PySpark script,
        or SparkSQL query, that you submit to Amazon EMR on EKS.
        See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr-containers.html#EMRContainers.Client.start_job_run  # noqa: E501

        :param name: The name of the job run.
        :param execution_role_arn: The IAM role ARN associated with the job run.
        :param release_label: The Amazon EMR release version to use for the job run.
        :param job_driver: Job configuration details, e.g. the Spark job parameters.
        :param configuration_overrides: The configuration overrides for the job run,
            specifically either application configuration or monitoring configuration.
        :param client_request_token: The client idempotency token of the job run request.
            Use this if you want to specify a unique ID to prevent two jobs from getting started.
        :param tags: The tags assigned to job runs.
        :return: Job ID
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

        response = self.conn.start_job_run(**params)

        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise AirflowException(f'Start Job Run failed: {response}')
        else:
            self.log.info(
                "Start Job Run success - Job Id %s and virtual cluster id %s",
                response['id'],
                response['virtualClusterId'],
            )
            return response['id']

    def get_job_failure_reason(self, job_id: str) -> str | None:
        """
        Fetch the reason for a job failure (e.g. error message). Returns None or reason string.

        :param job_id: Id of submitted job run
        :return: str
        """
        # We absorb any errors if we can't retrieve the job status
        reason = None

        try:
            response = self.conn.describe_job_run(
                virtualClusterId=self.virtual_cluster_id,
                id=job_id,
            )
            failure_reason = response['jobRun']['failureReason']
            state_details = response["jobRun"]["stateDetails"]
            reason = f"{failure_reason} - {state_details}"
        except KeyError:
            self.log.error('Could not get status of the EMR on EKS job')
        except ClientError as ex:
            self.log.error('AWS request failed, check logs for more info: %s', ex)

        return reason

    def check_query_status(self, job_id: str) -> str | None:
        """
        Fetch the status of submitted job run. Returns None or one of valid query states.
        See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr-containers.html#EMRContainers.Client.describe_job_run  # noqa: E501
        :param job_id: Id of submitted job run
        :return: str
        """
        try:
            response = self.conn.describe_job_run(
                virtualClusterId=self.virtual_cluster_id,
                id=job_id,
            )
            return response["jobRun"]["state"]
        except self.conn.exceptions.ResourceNotFoundException:
            # If the job is not found, we raise an exception as something fatal has happened.
            raise AirflowException(f'Job ID {job_id} not found on Virtual Cluster {self.virtual_cluster_id}')
        except ClientError as ex:
            # If we receive a generic ClientError, we swallow the exception so that the
            self.log.error('AWS request failed, check logs for more info: %s', ex)
            return None

    def poll_query_status(
        self,
        job_id: str,
        max_tries: int | None = None,
        poll_interval: int = 30,
        max_polling_attempts: int | None = None,
    ) -> str | None:
        """
        Poll the status of submitted job run until query state reaches final state.
        Returns one of the final states.

        :param job_id: Id of submitted job run
        :param max_tries: Deprecated - Use max_polling_attempts instead
        :param poll_interval: Time (in seconds) to wait between calls to check query status on EMR
        :param max_polling_attempts: Number of times to poll for query state before function exits
        :return: str
        """
        if max_tries:
            warnings.warn(
                f"Method `{self.__class__.__name__}.max_tries` is deprecated and will be removed "
                "in a future release.  Please use method `max_polling_attempts` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            if max_polling_attempts and max_polling_attempts != max_tries:
                raise Exception("max_polling_attempts must be the same value as max_tries")
            else:
                max_polling_attempts = max_tries

        try_number = 1
        final_query_state = None  # Query state when query reaches final state or max_polling_attempts reached

        while True:
            query_state = self.check_query_status(job_id)
            if query_state is None:
                self.log.info("Try %s: Invalid query state. Retrying again", try_number)
            elif query_state in self.TERMINAL_STATES:
                self.log.info("Try %s: Query execution completed. Final state is %s", try_number, query_state)
                final_query_state = query_state
                break
            else:
                self.log.info("Try %s: Query is still in non-terminal state - %s", try_number, query_state)
            if (
                max_polling_attempts and try_number >= max_polling_attempts
            ):  # Break loop if max_polling_attempts reached
                final_query_state = query_state
                break
            try_number += 1
            sleep(poll_interval)
        return final_query_state

    def stop_query(self, job_id: str) -> dict:
        """
        Cancel the submitted job_run

        :param job_id: Id of submitted job_run
        :return: dict
        """
        return self.conn.cancel_job_run(
            virtualClusterId=self.virtual_cluster_id,
            id=job_id,
        )

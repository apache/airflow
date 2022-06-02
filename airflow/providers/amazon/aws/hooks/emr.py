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
from time import sleep
from typing import Any, Dict, List, Optional

from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class EmrHook(AwsBaseHook):
    """
    Interact with AWS EMR. emr_conn_id is only necessary for using the
    create_job_flow method.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    conn_name_attr = 'emr_conn_id'
    default_conn_name = 'emr_default'
    conn_type = 'emr'
    hook_name = 'Amazon Elastic MapReduce'

    def __init__(self, emr_conn_id: Optional[str] = default_conn_name, *args, **kwargs) -> None:
        self.emr_conn_id = emr_conn_id
        kwargs["client_type"] = "emr"
        super().__init__(*args, **kwargs)

    def get_cluster_id_by_name(self, emr_cluster_name: str, cluster_states: List[str]) -> Optional[str]:
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

    def create_job_flow(self, job_flow_overrides: Dict[str, Any]) -> Dict[str, Any]:
        """
        Creates a job flow using the config from the EMR connection.
        Keys of the json extra hash may have the arguments of the boto3
        run_job_flow method.
        Overrides for this config may be passed as the job_flow_overrides.
        """
        if not self.emr_conn_id:
            raise AirflowException('emr_conn_id must be present to use create_job_flow')

        emr_conn = self.get_connection(self.emr_conn_id)

        config = emr_conn.extra_dejson.copy()
        config.update(job_flow_overrides)

        response = self.get_conn().run_job_flow(**config)

        return response


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

    def __init__(self, *args: Any, virtual_cluster_id: Optional[str] = None, **kwargs: Any) -> None:
        super().__init__(client_type="emr-containers", *args, **kwargs)  # type: ignore
        self.virtual_cluster_id = virtual_cluster_id

    def submit_job(
        self,
        name: str,
        execution_role_arn: str,
        release_label: str,
        job_driver: dict,
        configuration_overrides: Optional[dict] = None,
        client_request_token: Optional[str] = None,
        tags: Optional[dict] = None,
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

    def get_job_failure_reason(self, job_id: str) -> Optional[str]:
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

    def check_query_status(self, job_id: str) -> Optional[str]:
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
        self, job_id: str, max_tries: Optional[int] = None, poll_interval: int = 30
    ) -> Optional[str]:
        """
        Poll the status of submitted job run until query state reaches final state.
        Returns one of the final states.

        :param job_id: Id of submitted job run
        :param max_tries: Number of times to poll for query state before function exits
        :param poll_interval: Time (in seconds) to wait between calls to check query status on EMR
        :return: str
        """
        try_number = 1
        final_query_state = None  # Query state when query reaches final state or max_tries reached

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
            if max_tries and try_number >= max_tries:  # Break loop if max_tries reached
                final_query_state = query_state
                break
            try_number += 1
            sleep(poll_interval)
        return final_query_state

    def stop_query(self, job_id: str) -> Dict:
        """
        Cancel the submitted job_run

        :param job_id: Id of submitted job_run
        :return: dict
        """
        return self.conn.cancel_job_run(
            virtualClusterId=self.virtual_cluster_id,
            id=job_id,
        )

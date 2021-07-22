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
from typing import Any, Dict, Optional
import botocore.exceptions

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class EmrContainersHook(AwsBaseHook):
    """
    Interact with AWS EMR for EKS.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    hook_name = 'Elastic MapReduce Containers'

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "emr-containers"
        super().__init__(*args, **kwargs)

    def handle_aws_client_error(self, error_response: dict) -> None:
        """Logs errors from the client

        :param error_response: A dictionary for AWS service exceptions
        :type error_response: str
        """
        self.log.error("%s: %s", self.hook_name, error_response.get("Code"))
        self.log.error("%s: %s", self.hook_name, error_response.get("Message"))

    def get_job_by_id(self, job_id: str, cluster_id: str) -> Optional[dict]:
        """Get job details by job id and virtual cluster id.

        If the job is found, returns a response describing the job.
        Implements https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr-containers.html#EMRContainers.Client.describe_job_run

        :param job_id: The ID of the job run request
        :type job_id: str
        :param cluster_id: The ID of the virtual cluster for which the job run is submitted
        :type cluster_id: str
        :return: A dictionary representing the job
        :rtype: dict
        """

        try:
            return self.get_conn().describe_job_run(
                id=job_id,
                virtualClusterId=cluster_id
            )
        except botocore.exceptions.ClientError as err:
            error_response = err.response.get("Error", {})
            self.handle_aws_client_error(error_response)
            if error_response.get("Code") in ("ValidationException", "InternalServerException"):
                raise AirflowException(error_response.get("Message"))

    def start_job(
        self,
        cluster_id: str,
        execution_role_arn: str,
        emr_release_label: str,
        job_driver: Dict[str, Any],
        **kwargs: Any
    ) -> Dict[str, str]:
        """Starts a spark job using EMR in EKS

        Refer to https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr-containers.html#EMRContainers.Client.start_job_run

        :param cluster_id: The ID of the virtual cluster for which the job run is submitted
        :type cluster_id: str
        :param execution_role_arn: The execution role ARN for the job run.
        :type execution_role_arn: str
        :param emr_release_label: The Amazon EMR release version to use for the job run.
        :type emr_release_label: str
        :param job_driver: The job driver for the job run.
        :type job_driver: dict
        :param configuration_overrides: The configuration overrides for the job run.
        :type configuration_overrides: dict
        :param tags: The tags assigned to job runs
        :type tags: dict
        :param name: The name of the job run.
        :type name: str
        :param client_token: The client idempotency token of the job run request. Provided if not populated
        :type client_token: str
        :return: A response with job run details
        :rtype: dict

        """
        params = {
            "virtualClusterId": cluster_id,
            "executionRoleArn": execution_role_arn,
            "releaseLabel": emr_release_label,
            "jobDriver": job_driver
        }
        optional_params = (
            ("configurationOverrides", "configuration_overrides"),
            ("name", "name"),
            ("clientToken", "client_token"),
            ("tags", "tags")
        )
        for aws_var_name, airflow_var_name in optional_params:
            if kwargs.get(airflow_var_name):
                params[aws_var_name] = kwargs[airflow_var_name]

        try:
            return self.get_conn().start_job_run(**params)
        except botocore.exceptions.ClientError as err:
            error_response = err.response.get("Error", {})
            self.handle_aws_client_error(error_response)
            raise AirflowException(error_response.get("Message"))

    def terminate_job_by_id(self, job_id: str, cluster_id: str) -> Optional[dict]:
        """Terminates a job by job id and virtual cluster id.

        If the job is found, returns a response with job id and cluster id.
        Implements https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr-containers.html#EMRContainers.Client.cancel_job_run

        :param job_id: The ID of the job run request
        :type job_id: str
        :param cluster_id: The ID of the virtual cluster for which the job run is submitted
        :type cluster_id: str
        :return: A dictionary representing the job
        :rtype: dict
        """

        try:
            return self.get_conn().cancel_job_run(
                id=job_id,
                virtualClusterId=cluster_id
            )
        except botocore.exceptions.ClientError as err:
            error_response = err.response.get("Error", {})
            self.handle_aws_client_error(error_response)
            raise AirflowException(error_response.get("Message"))

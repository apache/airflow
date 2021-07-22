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

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.emr_containers import EmrContainersHook


class EmrContainersStartJobOperator(BaseOperator):
    """Operator to start a job.

    A job run is a unit of work, such as a Spark jar, PySpark script, or SparkSQL query,
    that you submit to Amazon EMR on EKS.

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
    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
    """

    ui_color = '#f9c915'

    def __init__(
        self,
        *,
        cluster_id: str,
        execution_role_arn: str,
        emr_release_label: str,
        job_driver: Dict[str, Any],
        configuration_overrides: Optional[dict] = None,
        tags: Optional[dict] = None,
        name: Optional[str] = None,
        client_token: Optional[str] = None,
        aws_conn_id: str = 'aws_default',
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.start_job_params = dict(
            cluster_id=cluster_id,
            execution_role_arn=execution_role_arn,
            emr_release_label=emr_release_label,
            job_driver=job_driver,
            configuration_overrides=configuration_overrides,
            tags=tags,
            name=name,
            client_token=client_token,
        )
        self.aws_conn_id = aws_conn_id

    def execute(self, context: Dict[str, Any]) -> str:
        """Start a job in EMR EKS

        :return: A job id
        :rtype: str
        """
        emr_containers = EmrContainersHook(aws_conn_id=self.aws_conn_id)

        self.log.info('Starting job in EMR Containers')
        response = emr_containers.start_job(**self.start_job_params)
        self.log.info(
            'Job %s has been started in EMR Containers in cluster %s',
            response["id"],
            response["virtualClusterId"],
        )
        return response["id"]

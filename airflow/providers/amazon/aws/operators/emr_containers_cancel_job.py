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

from typing import Any, Dict

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.emr_containers import EmrContainersHook


class EmrContainersCancelJobOperator(BaseOperator):
    """Operator to a cancel a job run.

    Cancels a job run. A job run is a unit of work,
    such as a Spark jar, PySpark script, or SparkSQL query, that you submit to Amazon EMR on EKS.

    :param job_id: The ID of the job run to cancel.
    :type job_id: str
    :param cluster_id: The ID of the virtual cluster for which the job run will be canceled
    :type cluster_id: str
    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
    """

    ui_color = '#f9c915'

    def __init__(self, *, job_id: str, cluster_id: str, aws_conn_id: str = 'aws_default', **kwargs):
        super().__init__(**kwargs)
        self.job_id = job_id
        self.cluster_id = cluster_id
        self.aws_conn_id = aws_conn_id

    def execute(self, context: Dict[str, Any]) -> str:
        emr_containers = EmrContainersHook(aws_conn_id=self.aws_conn_id)

        self.log.info('Cancelling EMR Containers job %s', self.job_id)
        response = emr_containers.terminate_job_by_id(job_id=self.job_id, cluster_id=self.cluster_id)
        self.log.info('EMR Containers job %s has been cancelled', response["id"])
        return response["id"]

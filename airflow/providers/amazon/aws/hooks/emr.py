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
from typing import Any, Dict, List, Optional

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

    def __init__(self, emr_conn_id: Optional[str] = None, *args, **kwargs) -> None:
        self.emr_conn_id = emr_conn_id
        kwargs["client_type"] = "emr"
        super().__init__(*args, **kwargs)

    def get_cluster_id_by_name(self, emr_cluster_name: str, cluster_states: List[str]) -> Optional[str]:
        """
        Fetch id of EMR cluster with given name and (optional) states.
        Will return only if single id is found.

        :param emr_cluster_name: Name of a cluster to find
        :type emr_cluster_name: str
        :param cluster_states: State(s) of cluster to find
        :type cluster_states: list
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

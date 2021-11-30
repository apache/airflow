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

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift import RedshiftClusterStates, RedshiftHook


class RedshiftPauseClusterOperator(BaseOperator):
    """
    Pause an AWS Redshift Cluster if it has status `available`.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RedshiftPauseClusterOperator`

    :param cluster_identifier: id of the AWS Redshift Cluster
    :type cluster_identifier: str
    :param aws_conn_id: aws connection to use
    :type aws_conn_id: str
    :param check_interval: time in seconds that the job should wait in
        between each instance state checks until operation is completed
    :type check_interval: float
    """

    template_fields = ("cluster_identifier",)
    ui_color = "#eeaa11"
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        *,
        cluster_identifier: str,
        aws_conn_id: str = "aws_default",
        check_interval: float = 15,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster_identifier = cluster_identifier
        self.aws_conn_id = aws_conn_id
        self.check_interval = check_interval

    def execute(self, context):
        redshift_hook = RedshiftHook(aws_conn_id=self.aws_conn_id)
        self.log.info("Pausing Redshift cluster %s", self.cluster_identifier)
        cluster_state = redshift_hook.cluster_status(cluster_identifier=self.cluster_identifier)
        if cluster_state == RedshiftClusterStates.AVAILABLE:
            redshift_hook.get_conn().pause_cluster(ClusterIdentifier=self.cluster_identifier)

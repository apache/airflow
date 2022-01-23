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
from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift_cluster import RedshiftHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class RedshiftResumeClusterOperator(BaseOperator):
    """
    Resume a paused AWS Redshift Cluster

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RedshiftResumeClusterOperator`

    :param cluster_identifier: id of the AWS Redshift Cluster
    :param aws_conn_id: aws connection to use
    """

    template_fields: Sequence[str] = ("cluster_identifier",)
    ui_color = "#eeaa11"
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        *,
        cluster_identifier: str,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster_identifier = cluster_identifier
        self.aws_conn_id = aws_conn_id

    def execute(self, context: 'Context'):
        redshift_hook = RedshiftHook(aws_conn_id=self.aws_conn_id)
        cluster_state = redshift_hook.cluster_status(cluster_identifier=self.cluster_identifier)
        if cluster_state == 'paused':
            self.log.info("Starting Redshift cluster %s", self.cluster_identifier)
            redshift_hook.get_conn().resume_cluster(ClusterIdentifier=self.cluster_identifier)
        else:
            self.log.warning(
                "Unable to resume cluster since cluster is currently in status: %s", cluster_state
            )


class RedshiftPauseClusterOperator(BaseOperator):
    """
    Pause an AWS Redshift Cluster if it has status `available`.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RedshiftPauseClusterOperator`

    :param cluster_identifier: id of the AWS Redshift Cluster
    :param aws_conn_id: aws connection to use
    """

    template_fields: Sequence[str] = ("cluster_identifier",)
    ui_color = "#eeaa11"
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        *,
        cluster_identifier: str,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster_identifier = cluster_identifier
        self.aws_conn_id = aws_conn_id

    def execute(self, context: 'Context'):
        redshift_hook = RedshiftHook(aws_conn_id=self.aws_conn_id)
        cluster_state = redshift_hook.cluster_status(cluster_identifier=self.cluster_identifier)
        if cluster_state == 'available':
            self.log.info("Pausing Redshift cluster %s", self.cluster_identifier)
            redshift_hook.get_conn().pause_cluster(ClusterIdentifier=self.cluster_identifier)
        else:
            self.log.warning(
                "Unable to pause cluster since cluster is currently in status: %s", cluster_state
            )

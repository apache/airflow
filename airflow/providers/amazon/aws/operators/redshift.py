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
from typing import Dict, Iterable, Optional, Union

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift import RedshiftHook, RedshiftSQLHook


class RedshiftSQLOperator(BaseOperator):
    """
    Executes SQL Statements against an Amazon Redshift cluster

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RedshiftSQLOperator`

    :param sql: the sql code to be executed
    :type sql: Can receive a str representing a sql statement,
        or an iterable of str (sql statements)
    :param redshift_conn_id: reference to
        :ref:`Amazon Redshift connection id<howto/connection:redshift>`
    :type redshift_conn_id: str
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: dict or iterable
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :type autocommit: bool
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)

    def __init__(
        self,
        *,
        sql: Optional[Union[Dict, Iterable]],
        redshift_conn_id: str = 'redshift_default',
        parameters: Optional[dict] = None,
        autocommit: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.autocommit = autocommit
        self.parameters = parameters

    def get_hook(self) -> RedshiftSQLHook:
        """Create and return RedshiftSQLHook.
        :return RedshiftSQLHook: A RedshiftSQLHook instance.
        """
        return RedshiftSQLHook(redshift_conn_id=self.redshift_conn_id)

    def execute(self, context: dict) -> None:
        """Execute a statement against Amazon Redshift"""
        self.log.info(f"Executing statement: {self.sql}")
        hook = self.get_hook()
        hook.run(self.sql, autocommit=self.autocommit, parameters=self.parameters)


class RedshiftResumeClusterOperator(BaseOperator):
    """
    Resume a paused AWS Redshift Cluster

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RedshiftResumeClusterOperator`

    :param cluster_identifier: id of the AWS Redshift Cluster
    :type cluster_identifier: str
    :param aws_conn_id: aws connection to use
    :type aws_conn_id: str
    """

    template_fields = ("cluster_identifier",)
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

    def execute(self, context):
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
    :type cluster_identifier: str
    :param aws_conn_id: aws connection to use
    :type aws_conn_id: str
    """

    template_fields = ("cluster_identifier",)
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

    def execute(self, context):
        redshift_hook = RedshiftHook(aws_conn_id=self.aws_conn_id)
        cluster_state = redshift_hook.cluster_status(cluster_identifier=self.cluster_identifier)
        if cluster_state == 'available':
            self.log.info("Pausing Redshift cluster %s", self.cluster_identifier)
            redshift_hook.get_conn().pause_cluster(ClusterIdentifier=self.cluster_identifier)
        else:
            self.log.warning(
                "Unable to pause cluster since cluster is currently in status: %s", cluster_state
            )

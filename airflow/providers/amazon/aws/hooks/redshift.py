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
"""Interact with AWS Redshift clusters."""
import sys
from typing import Dict, List, Optional, Union

if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property

import redshift_connector
from redshift_connector import Connection as RedshiftConnection
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

from airflow.hooks.dbapi import DbApiHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class RedshiftHook(AwsBaseHook):
    """
    Interact with AWS Redshift, using the boto3 library

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :type aws_conn_id: str
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "redshift"
        super().__init__(*args, **kwargs)

    # TODO: Wrap create_cluster_snapshot
    def cluster_status(self, cluster_identifier: str) -> str:
        """
        Return status of a cluster

        :param cluster_identifier: unique identifier of a cluster
        :type cluster_identifier: str
        :param skip_final_cluster_snapshot: determines cluster snapshot creation
        :type skip_final_cluster_snapshot: bool
        :param final_cluster_snapshot_identifier: Optional[str]
        :type final_cluster_snapshot_identifier: Optional[str]
        """
        try:
            response = self.get_conn().describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters']
            return response[0]['ClusterStatus'] if response else None
        except self.get_conn().exceptions.ClusterNotFoundFault:
            return 'cluster_not_found'

    def delete_cluster(
        self,
        cluster_identifier: str,
        skip_final_cluster_snapshot: bool = True,
        final_cluster_snapshot_identifier: Optional[str] = None,
    ):
        """
        Delete a cluster and optionally create a snapshot

        :param cluster_identifier: unique identifier of a cluster
        :type cluster_identifier: str
        :param skip_final_cluster_snapshot: determines cluster snapshot creation
        :type skip_final_cluster_snapshot: bool
        :param final_cluster_snapshot_identifier: name of final cluster snapshot
        :type final_cluster_snapshot_identifier: str
        """
        final_cluster_snapshot_identifier = final_cluster_snapshot_identifier or ''

        response = self.get_conn().delete_cluster(
            ClusterIdentifier=cluster_identifier,
            SkipFinalClusterSnapshot=skip_final_cluster_snapshot,
            FinalClusterSnapshotIdentifier=final_cluster_snapshot_identifier,
        )
        return response['Cluster'] if response['Cluster'] else None

    def describe_cluster_snapshots(self, cluster_identifier: str) -> Optional[List[str]]:
        """
        Gets a list of snapshots for a cluster

        :param cluster_identifier: unique identifier of a cluster
        :type cluster_identifier: str
        """
        response = self.get_conn().describe_cluster_snapshots(ClusterIdentifier=cluster_identifier)
        if 'Snapshots' not in response:
            return None
        snapshots = response['Snapshots']
        snapshots = [snapshot for snapshot in snapshots if snapshot["Status"]]
        snapshots.sort(key=lambda x: x['SnapshotCreateTime'], reverse=True)
        return snapshots

    def restore_from_cluster_snapshot(self, cluster_identifier: str, snapshot_identifier: str) -> str:
        """
        Restores a cluster from its snapshot

        :param cluster_identifier: unique identifier of a cluster
        :type cluster_identifier: str
        :param snapshot_identifier: unique identifier for a snapshot of a cluster
        :type snapshot_identifier: str
        """
        response = self.get_conn().restore_from_cluster_snapshot(
            ClusterIdentifier=cluster_identifier, SnapshotIdentifier=snapshot_identifier
        )
        return response['Cluster'] if response['Cluster'] else None

    def create_cluster_snapshot(self, snapshot_identifier: str, cluster_identifier: str) -> str:
        """
        Creates a snapshot of a cluster

        :param snapshot_identifier: unique identifier for a snapshot of a cluster
        :type snapshot_identifier: str
        :param cluster_identifier: unique identifier of a cluster
        :type cluster_identifier: str
        """
        response = self.get_conn().create_cluster_snapshot(
            SnapshotIdentifier=snapshot_identifier,
            ClusterIdentifier=cluster_identifier,
        )
        return response['Snapshot'] if response['Snapshot'] else None


class RedshiftSQLHook(DbApiHook):
    """
    Execute statements against Amazon Redshift, using redshift_connector

    This hook requires the redshift_conn_id connection.

    :param redshift_conn_id: reference to
        :ref:`Amazon Redshift connection id<howto/connection:redshift>`
    :type redshift_conn_id: str

    .. note::
        get_sqlalchemy_engine() and get_uri() depend on sqlalchemy-amazon-redshift
    """

    conn_name_attr = 'redshift_conn_id'
    default_conn_name = 'redshift_default'
    conn_type = 'redshift'
    hook_name = 'Amazon Redshift'
    supports_autocommit = True

    @staticmethod
    def get_ui_field_behavior() -> Dict:
        """Returns custom field behavior"""
        return {
            "hidden_fields": [],
            "relabeling": {'login': 'User', 'schema': 'Database'},
        }

    @cached_property
    def conn(self):
        return self.get_connection(self.redshift_conn_id)  # type: ignore[attr-defined]

    def _get_conn_params(self) -> Dict[str, Union[str, int]]:
        """Helper method to retrieve connection args"""
        conn = self.conn

        conn_params: Dict[str, Union[str, int]] = {}

        if conn.login:
            conn_params['user'] = conn.login
        if conn.password:
            conn_params['password'] = conn.password
        if conn.host:
            conn_params['host'] = conn.host
        if conn.port:
            conn_params['port'] = conn.port
        if conn.schema:
            conn_params['database'] = conn.schema

        return conn_params

    def get_uri(self) -> str:
        """Overrides DbApiHook get_uri to use redshift_connector sqlalchemy dialect as driver name"""
        conn_params = self._get_conn_params()

        if 'user' in conn_params:
            conn_params['username'] = conn_params.pop('user')

        return str(URL(drivername='redshift+redshift_connector', **conn_params))

    def get_sqlalchemy_engine(self, engine_kwargs=None):
        """Overrides DbApiHook get_sqlalchemy_engine to pass redshift_connector specific kwargs"""
        conn_kwargs = self.conn.extra_dejson
        if engine_kwargs is None:
            engine_kwargs = {}

        if "connect_args" in engine_kwargs:
            engine_kwargs["connect_args"] = {**conn_kwargs, **engine_kwargs["connect_args"]}
        else:
            engine_kwargs["connect_args"] = conn_kwargs

        return create_engine(self.get_uri(), **engine_kwargs)

    def get_table_primary_key(self, table: str, schema: Optional[str] = "public") -> Optional[List[str]]:
        """
        Helper method that returns the table primary key
        :param table: Name of the target table
        :type table: str
        :param table: Name of the target schema, public by default
        :type table: str
        :return: Primary key columns list
        :rtype: List[str]
        """
        sql = """
            select kcu.column_name
            from information_schema.table_constraints tco
                    join information_schema.key_column_usage kcu
                        on kcu.constraint_name = tco.constraint_name
                            and kcu.constraint_schema = tco.constraint_schema
                            and kcu.constraint_name = tco.constraint_name
            where tco.constraint_type = 'PRIMARY KEY'
            and kcu.table_schema = %s
            and kcu.table_name = %s
        """
        pk_columns = [row[0] for row in self.get_records(sql, (schema, table))]
        return pk_columns or None

    def get_conn(self) -> RedshiftConnection:
        """Returns a redshift_connector.Connection object"""
        conn_params = self._get_conn_params()
        conn_kwargs_dejson = self.conn.extra_dejson
        conn_kwargs: Dict = {**conn_params, **conn_kwargs_dejson}
        conn: RedshiftConnection = redshift_connector.connect(**conn_kwargs)

        return conn

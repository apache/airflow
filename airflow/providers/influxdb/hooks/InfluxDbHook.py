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

"""This module allows to connect to a Neo4j database."""

from influxdb_client import InfluxDBClient
from influxdb_client.client.flux_table import FluxTable
from influxdb_client.client.write.point import Point
from typing import List, Generator, Any

from airflow.hooks.base import BaseHook
from airflow.models import Connection


class InfluxDBHook(BaseHook):
    """
    Interact with InfluxDB.

    Performs a connection to InfluxDB and retrieves client.

    :param influxdb_conn_id: Reference to :ref:`Influxdb connection id <howto/connection:influxdb>`.
    :type influxdb_conn_id: str
    """

    conn_name_attr = 'influxdb_conn_id'
    default_conn_name = 'influxdb_default'
    conn_type = 'influxdb'
    hook_name = 'Influxdb'

    def __init__(self, conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.influxdb_conn_id = conn_id
        self.connection = kwargs.pop("connection", None)
        self.client = None
        self.extras = None
        self.uri = None
        self.org_name = None

    def get_client(self, uri, token, org_name):
        return InfluxDBClient(url=uri,
                                token=token,
                                org=org_name)

    def get_conn(self) -> InfluxDBClient:
        """
        Function that initiates a new InfluxDB connection
        with token and organization name
        """
        self.connection = self.get_connection(self.influxdb_conn_id)
        self.extras = self.connection.extra_dejson.copy()

        self.uri = self.get_uri(self.connection)
        self.log.info('URI: %s', self.uri)

        if self.client is not None:
            return self.client

        token = self.connection.extra_dejson.get('token')
        self.org_name = self.connection.extra_dejson.get('org_name')

        self.client = self.get_client(self.uri, token, self.org_name)

        return self.client


    def query(self, query) -> List[FluxTable]:
        """
        Function to use the query_api
        to run the query.
        Note: The bucket name
        should be included in the query
        'from(bucket:"my-bucket") |> range(start: -10m)'

        :param query: InfluxDB query
        :return: List[FluxTable]
        """
        client = self.get_conn()

        query_api = client.query_api()
        return query_api.query(query)

    def write(self, bucket_name, point_name, tag_name, tag_value, field_name, field_value):
        """
        Writes a Point to the bucket specified.
        Example: Point("my_measurement").tag("location", "Prague").field("temperature", 25.3)
        """
        write_api = self.client.write_api()

        p = Point(point_name).tag(tag_name, tag_value).field(field_name, field_value)

        write_api.write(bucket=bucket_name, record=p)

    def create_organization(self, name):
        """
        Function to create a new organization
        """
        return self.client.organizations_api().create_organization(name=name)

    def delete_organization(self, org_id):
        """
        Function to delete organization by organization id
        """
        return self.client.organizations_api().delete_organization(org_id=org_id)

    def create_bucket(self, bucket_name, description, org_id, retention_rules=None):
        """
        Function to create a bucket for an organization

        """
        return self.client.buckets_api.create_bucket(bucket_name=bucket_name, description=description,
                                              org_id=org_id, retention_rules=None)

    def delete_bucket(self, bucket_id):
        """
        Function to delete bucket by bucket id.
        """
        return self.client.buckets_api.delete_bucket(bucket_id)

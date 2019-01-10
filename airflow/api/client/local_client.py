# -*- coding: utf-8 -*-
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

from airflow.api.client import api_client
from airflow.api.common.experimental import pool, connections
from airflow.api.common.experimental import trigger_dag
from airflow.api.common.experimental import delete_dag


class Client(api_client.Client):
    """Local API client implementation."""

    def trigger_dag(self, dag_id, run_id=None, conf=None, execution_date=None):
        dr = trigger_dag.trigger_dag(dag_id=dag_id,
                                     run_id=run_id,
                                     conf=conf,
                                     execution_date=execution_date)
        return "Created {}".format(dr)

    def delete_dag(self, dag_id):
        count = delete_dag.delete_dag(dag_id)
        return "Removed {} record(s)".format(count)

    def get_pool(self, name):
        p = pool.get_pool(name=name)
        return p.pool, p.slots, p.description

    def get_pools(self):
        return [(p.pool, p.slots, p.description) for p in pool.get_pools()]

    def create_pool(self, name, slots, description):
        p = pool.create_pool(name=name, slots=slots, description=description)
        return p.pool, p.slots, p.description

    def delete_pool(self, name):
        p = pool.delete_pool(name=name)
        return p.pool, p.slots, p.description

    def add_connection(self, conn_id,
                       conn_uri=None,
                       conn_type=None,
                       conn_host=None,
                       conn_login=None,
                       conn_password=None,
                       conn_schema=None,
                       conn_port=None,
                       conn_extra=None):
        """

        :param conn_id:
        :param conn_uri:
        :param conn_type:
        :param conn_host:
        :param conn_login:
        :param conn_password:
        :param conn_schema:
        :param conn_port:
        :param conn_extra:
        :return: The new Connection
        """
        return connections.add_connection(conn_id,
                                          conn_uri,
                                          conn_type,
                                          conn_host,
                                          conn_login,
                                          conn_password,
                                          conn_schema,
                                          conn_port,
                                          conn_extra).to_json()

    def delete_connection(self, conn_id, delete_all=False):
        """

        :param conn_id:
        :param delete_all:
        :return: the conn_id(s) of the Connection(s) that were removed
        """
        return connections.delete_connection(conn_id, delete_all)

    def list_connections(self):
        """

        :return: All Connections
        """
        return [conn.to_json() for conn in connections.list_connections()]

    def update_connection(self, conn_id,
                          conn_uri=None,
                          conn_type=None,
                          conn_host=None,
                          conn_login=None,
                          conn_password=None,
                          conn_schema=None,
                          conn_port=None,
                          conn_extra=None):
        """

        :param conn_id:
        :param conn_uri:
        :param conn_type:
        :param conn_host:
        :param conn_login:
        :param conn_password:
        :param conn_schema:
        :param conn_port:
        :param conn_extra:
        :return: The updated Connection
        """
        return connections.update_connection(conn_id,
                                             conn_uri,
                                             conn_type,
                                             conn_host,
                                             conn_login,
                                             conn_password,
                                             conn_schema,
                                             conn_port,
                                             conn_extra).to_json()

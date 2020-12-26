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
import json
from typing import Dict, Optional, Tuple

from airflow.hooks.base import BaseHook
from neo4j import GraphDatabase, Neo4jDriver, Result
from airflow.models import Connection


class Neo4jHook(BaseHook):
    """
    Interact with MySQL.

    You can specify charset in the extra field of your connection
    as ``{"charset": "utf8"}``. Also you can choose cursor as
    ``{"cursor": "SSCursor"}``. Refer to the MySQLdb.cursors for more details.

    Note: For AWS IAM authentication, use iam in the extra connection parameters
    and set it to true. Leave the password field empty. This will use the
    "aws_default" connection to get the temporary token unless you override
    in extras.
    extras example: ``{"iam":true, "aws_conn_id":"my_aws_conn"}``
    """

    conn_name_attr = 'neo4j_conn_id'
    default_conn_name = 'neo4j_default'
    conn_type = 'neo4j'
    hook_name = 'Neo4j'

    def __init__(self, conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.neo4j_conn_id = conn_id
        self.connection = None
        self.client = None

    def get_conn(self) -> Neo4jDriver:
        """

        """
        self.connection = self.get_connection(self.neo4j_conn_id)
        self.extras = self.connection.extra_dejson.copy()

        self.uri = self.get_uri(self.connection)

        if self.client is not None:
            return self.client

        self.client = GraphDatabase.driver(self.uri, auth=(self.connection.login, self.connection.password))

        return self.client

    def get_uri(self, conn: Connection) -> str:
        """

        """

        use_bolt_scheme = conn.extra_dejson.get('bolt_scheme', False)
        scheme = 'bolt' if use_bolt_scheme else 'neo4j'

        # Self signed certificates
        ssc = conn.extra_dejson.get('ssc', False)

        # Only certificates signed by CA.
        trusted_ca = conn.extra_dejson.get('trusted_ca', False)
        encryption_scheme = ''

        if ssc:
            encryption_scheme = '+ssc'
        elif trusted_ca:
            encryption_scheme = '+s'

        return '{scheme}{encryption_scheme}://{host}:{port}'.format(
            scheme=scheme,
            encryption_scheme=encryption_scheme,
            host=conn.host,
            port='7687' if conn.port is None else f':{conn.port}'
        )

    def run(self, query) -> Result:
        with self.get_conn().session() as session:
            result = session.run(query)

        return result

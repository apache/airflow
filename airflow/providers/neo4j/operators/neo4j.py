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
from typing import Dict, Iterable, Mapping, Optional, Union

from airflow.models import BaseOperator
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
from airflow.utils.decorators import apply_defaults

class Neo4jOperator(BaseOperator):
    """
    Executes sql code in a specific MySQL database

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:MySqlOperator`

    :param sql: the sql code to be executed. Can receive a str representing a
        sql statement, a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
        (templated)
    :type sql: str or list[str]
    :param mysql_conn_id: reference to a specific mysql database
    :type mysql_conn_id: str
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: dict or iterable
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :type autocommit: bool
    :param database: name of database which overwrite defined one in connection
    :type database: str
    """

    @apply_defaults
    def __init__(
        self,
        *,
        sql: str,
        neo4j_conn_id: str = 'neo4j_default',
        parameters: Optional[Union[Mapping, Iterable]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.neo4j_conn_id = neo4j_conn_id
        self.sql = sql
        self.parameters = parameters

    def execute(self, context: Dict) -> None:
        self.log.info('Executing: %s', self.sql)
        hook = Neo4jHook(conn_id=self.neo4j_conn_id)
        hook.run(self.sql)

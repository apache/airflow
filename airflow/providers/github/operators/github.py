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
from typing import Dict

from airflow.models import BaseOperator
from airflow.providers.github.hooks.github import GithubHook


class GithubOperator(BaseOperator):
    """
    Executes Github Operations

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GithubOperator`

    :param sql: the sql code to be executed. Can receive a str representing a
        sql statement
    :type sql: str
    :param github_conn_id: Reference to :ref:`Github connection id <howto/connection:github>`.
    :type github_conn_id: str
    """

    def __init__(
        self,
        *,
        github_conn_id: str = 'github_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.github_conn_id = github_conn_id

    # def execute(self, context: Dict) -> None:
    #     self.hook = GithubHook(conn_id=self.github_conn_id)
    #     self.hook.query(self.sql)

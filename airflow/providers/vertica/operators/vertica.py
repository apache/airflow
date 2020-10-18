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
from typing import Any, Dict, List, Union

from airflow.models import BaseOperator
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.utils.decorators import apply_defaults


class VerticaOperator(BaseOperator):
    """
    Executes sql code in a specific Vertica database.

    :param vertica_conn_id: reference to a specific Vertica database
    :type vertica_conn_id: str
    :param sql: the sql code to be executed. (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#b4e0ff'

    @apply_defaults
    def __init__(
        self, *, sql: Union[str, List[str]], vertica_conn_id: str = 'vertica_default', **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.vertica_conn_id = vertica_conn_id
        self.sql = sql

    def execute(self, context: Dict[Any, Any]) -> None:
        self.log.info('Executing: %s', self.sql)
        hook = VerticaHook(vertica_conn_id=self.vertica_conn_id)
        hook.run(sql=self.sql)

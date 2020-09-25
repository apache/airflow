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
from typing import Iterable, Mapping, Optional, Union

from airflow.models import BaseOperator
from airflow.providers.clickhouse.hooks.clickhouse import ClickHouseHook
from airflow.utils.decorators import apply_defaults


class ClickHouseOperator(BaseOperator):

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
        self,
        *,
        sql: str,
        clickhouse_conn_id: str = 'clickhouse_default',
        parameters: Optional[Union[Mapping, Iterable]] = None,
        database: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.clickhouse_conn_id = clickhouse_conn_id
        self.parameters = parameters
        self.database = database
        self.hook = None

    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        self.hook = ClickHouseHook(postgres_conn_id=self.clickhouse_conn_id, schema=self.database)
        self.hook.run(self.sql, parameters=self.parameters)
        for output in self.hook.conn.notices:
            self.log.info(output)

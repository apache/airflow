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
from typing import TYPE_CHECKING, Iterable, Mapping, Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.providers.apache.drill.hooks.drill import DrillHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DrillOperator(BaseOperator):
    """
    Executes the provided SQL in the identified Drill environment.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DrillOperator`

    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file.
        Template references are recognized by str ending in '.sql'
    :param drill_conn_id: id of the connection config for the target Drill
        environment
    :param parameters: (optional) the parameters to render the SQL query with.
    """

    template_fields: Sequence[str] = ('sql',)
    template_fields_renderers = {'sql': 'sql'}
    template_ext: Sequence[str] = ('.sql',)
    ui_color = '#ededed'

    def __init__(
        self,
        *,
        sql: str,
        drill_conn_id: str = 'drill_default',
        parameters: Optional[Union[Iterable, Mapping]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.drill_conn_id = drill_conn_id
        self.parameters = parameters
        self.hook: Optional[DrillHook] = None

    def execute(self, context: 'Context'):
        self.log.info('Executing: %s on %s', self.sql, self.drill_conn_id)
        self.hook = DrillHook(drill_conn_id=self.drill_conn_id)
        self.hook.run(self.sql, parameters=self.parameters, split_statements=True)

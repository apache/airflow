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
import re
from typing import Any, Optional, Tuple

from airflow.models import BaseOperator
from airflow.providers.apache.pig.hooks.pig import PigCliHook
from airflow.utils.decorators import apply_defaults


class PigOperator(BaseOperator):
    """
    Executes pig script.

    :param pig: the pig latin script to be executed. (templated)
    :type pig: str
    :param pig_cli_conn_id: reference to the Hive database
    :type pig_cli_conn_id: str
    :param pigparams_jinja_translate: when True, pig params-type templating
        ${var} gets translated into jinja-type templating {{ var }}. Note that
        you may want to use this along with the
        ``DAG(user_defined_macros=myargs)`` parameter. View the DAG
        object documentation for more details.
    :type pigparams_jinja_translate: bool
    :param pig_opts: pig options, such as: -x tez, -useHCatalog, ...
    :type pig_opts: str
    """

    template_fields = ('pig',)
    template_ext = ('.pig', '.piglatin',)
    ui_color = '#f0e4ec'

    @apply_defaults
    def __init__(
            self,
            pig: str,
            pig_cli_conn_id: str = 'pig_cli_default',
            pigparams_jinja_translate: bool = False,
            pig_opts: Optional[str] = None,
            *args: Tuple[Any, ...],
            **kwargs: Any) -> None:

        super().__init__(*args, **kwargs)
        self.pigparams_jinja_translate = pigparams_jinja_translate
        self.pig = pig
        self.pig_cli_conn_id = pig_cli_conn_id
        self.pig_opts = pig_opts
        self.hook = None

    def prepare_template(self):
        if self.pigparams_jinja_translate:
            self.pig = re.sub(
                r"(\$([a-zA-Z_][a-zA-Z0-9_]*))", r"{{ \g<2> }}", self.pig)

    def execute(self, context):
        self.log.info('Executing: %s', self.pig)
        self.hook = PigCliHook(pig_cli_conn_id=self.pig_cli_conn_id)
        self.hook.run_cli(pig=self.pig, pig_opts=self.pig_opts)

    def on_kill(self):
        self.hook.kill()

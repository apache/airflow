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

from airflow.models import BaseOperator
from airflow.providers.cloudera.hooks.cdw_hook import CdwHook
from airflow.utils.operator_helpers import context_to_airflow_vars


class CdwExecuteQueryOperator(BaseOperator):
    """
    Executes hql code in CDW. This class inherits behavior
    from HiveOperator, and instantiates a CdwHook to do the work.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CdwExecuteQueryOperator`
    """

    template_fields = ("hql", "schema", "hiveconfs")
    template_ext = (
        ".hql",
        ".sql",
    )
    ui_color = "#522a9f"
    ui_fgcolor = "#fff"

    def __init__(
        self,
        hql,
        schema="default",
        hiveconfs=None,
        hiveconf_jinja_translate=False,
        cli_conn_id="hive_cli_default",
        jdbc_driver=None,
        # new CDW args
        use_proxy_user=False,  # pylint: disable=unused-argument
        query_isolation=True,  # TODO: implement
        *args,
        **kwargs,
    ):

        super().__init__(*args, **kwargs)
        self.hql = hql
        self.schema = schema
        self.hiveconfs = hiveconfs or {}
        self.hiveconf_jinja_translate = hiveconf_jinja_translate
        self.run_as = None
        self.cli_conn_id = cli_conn_id
        self.jdbc_driver = jdbc_driver
        self.query_isolation = query_isolation
        # assigned lazily - just for consistency we can create the attribute with a
        # `None` initial value, later it will be populated by the execute method.
        # This also makes `on_kill` implementation consistent since it assumes `self.hook`
        # is defined.
        self.hook = None

    def get_hook(self):
        """Simply returns a CdwHook with the provided hive cli connection."""
        return CdwHook(
            cli_conn_id=self.cli_conn_id,
            query_isolation=self.query_isolation,
            jdbc_driver=self.jdbc_driver,
        )

    def prepare_template(self):
        if self.hiveconf_jinja_translate:
            self.hql = re.sub(r"(\$\{(hiveconf:)?([ a-zA-Z0-9_]*)\})", r"{{ \g<3> }}", self.hql)

    def execute(self, context):
        self.log.info("Executing: %s", self.hql)
        self.hook = self.get_hook()

        if self.hiveconf_jinja_translate:
            self.hiveconfs = context_to_airflow_vars(context)
        else:
            self.hiveconfs.update(context_to_airflow_vars(context))

        self.log.info("Passing HiveConf: %s", self.hiveconfs)
        self.hook.run_cli(hql=self.hql, schema=self.schema, hive_conf=self.hiveconfs)

    def dry_run(self):
        self.hook = self.get_hook()
        self.hook.test_hql(hql=self.hql)

    def on_kill(self):
        if self.hook:
            self.hook.kill()

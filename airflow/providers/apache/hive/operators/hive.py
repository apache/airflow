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
import os
import re
from typing import Any, Dict, Optional

from airflow.configuration import conf
from airflow.models import BaseOperator
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.utils import operator_helpers
from airflow.utils.decorators import apply_defaults
from airflow.utils.operator_helpers import context_to_airflow_vars


class HiveOperator(BaseOperator):
    """
    Executes hql code or hive script in a specific Hive database.

    :param hql: the hql to be executed. Note that you may also use
        a relative path from the dag file of a (template) hive
        script. (templated)
    :type hql: str
    :param hive_cli_conn_id: reference to the Hive database. (templated)
    :type hive_cli_conn_id: str
    :param hiveconfs: if defined, these key value pairs will be passed
        to hive as ``-hiveconf "key"="value"``
    :type hiveconfs: dict
    :param hiveconf_jinja_translate: when True, hiveconf-type templating
        ${var} gets translated into jinja-type templating {{ var }} and
        ${hiveconf:var} gets translated into jinja-type templating {{ var }}.
        Note that you may want to use this along with the
        ``DAG(user_defined_macros=myargs)`` parameter. View the DAG
        object documentation for more details.
    :type hiveconf_jinja_translate: bool
    :param script_begin_tag: If defined, the operator will get rid of the
        part of the script before the first occurrence of `script_begin_tag`
    :type script_begin_tag: str
    :param run_as_owner: Run HQL code as a DAG's owner.
    :type run_as_owner: bool
    :param mapred_queue: queue used by the Hadoop CapacityScheduler. (templated)
    :type  mapred_queue: str
    :param mapred_queue_priority: priority within CapacityScheduler queue.
        Possible settings include: VERY_HIGH, HIGH, NORMAL, LOW, VERY_LOW
    :type  mapred_queue_priority: str
    :param mapred_job_name: This name will appear in the jobtracker.
        This can make monitoring easier.
    :type  mapred_job_name: str
    """

    template_fields = (
        'hql',
        'schema',
        'hive_cli_conn_id',
        'mapred_queue',
        'hiveconfs',
        'mapred_job_name',
        'mapred_queue_priority',
    )
    template_ext = (
        '.hql',
        '.sql',
    )
    ui_color = '#f0e4ec'

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(
        self,
        *,
        hql: str,
        hive_cli_conn_id: str = 'hive_cli_default',
        schema: str = 'default',
        hiveconfs: Optional[Dict[Any, Any]] = None,
        hiveconf_jinja_translate: bool = False,
        script_begin_tag: Optional[str] = None,
        run_as_owner: bool = False,
        mapred_queue: Optional[str] = None,
        mapred_queue_priority: Optional[str] = None,
        mapred_job_name: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.hql = hql
        self.hive_cli_conn_id = hive_cli_conn_id
        self.schema = schema
        self.hiveconfs = hiveconfs or {}
        self.hiveconf_jinja_translate = hiveconf_jinja_translate
        self.script_begin_tag = script_begin_tag
        self.run_as = None
        if run_as_owner:
            self.run_as = self.dag.owner
        self.mapred_queue = mapred_queue
        self.mapred_queue_priority = mapred_queue_priority
        self.mapred_job_name = mapred_job_name
        self.mapred_job_name_template = conf.get(
            'hive',
            'mapred_job_name_template',
            fallback="Airflow HiveOperator task for {hostname}.{dag_id}.{task_id}.{execution_date}",
        )

        # assigned lazily - just for consistency we can create the attribute with a
        # `None` initial value, later it will be populated by the execute method.
        # This also makes `on_kill` implementation consistent since it assumes `self.hook`
        # is defined.
        self.hook: Optional[HiveCliHook] = None

    def get_hook(self) -> HiveCliHook:
        """Get Hive cli hook"""
        return HiveCliHook(
            hive_cli_conn_id=self.hive_cli_conn_id,
            run_as=self.run_as,
            mapred_queue=self.mapred_queue,
            mapred_queue_priority=self.mapred_queue_priority,
            mapred_job_name=self.mapred_job_name,
        )

    def prepare_template(self) -> None:
        if self.hiveconf_jinja_translate:
            self.hql = re.sub(r"(\$\{(hiveconf:)?([ a-zA-Z0-9_]*)\})", r"{{ \g<3> }}", self.hql)
        if self.script_begin_tag and self.script_begin_tag in self.hql:
            self.hql = "\n".join(self.hql.split(self.script_begin_tag)[1:])

    def execute(self, context: Dict[str, Any]) -> None:
        self.log.info('Executing: %s', self.hql)
        self.hook = self.get_hook()

        # set the mapred_job_name if it's not set with dag, task, execution time info
        if not self.mapred_job_name:
            ti = context['ti']
            self.hook.mapred_job_name = self.mapred_job_name_template.format(
                dag_id=ti.dag_id,
                task_id=ti.task_id,
                execution_date=ti.execution_date.isoformat(),
                hostname=ti.hostname.split('.')[0],
            )

        if self.hiveconf_jinja_translate:
            self.hiveconfs = context_to_airflow_vars(context)
        else:
            self.hiveconfs.update(context_to_airflow_vars(context))

        self.log.info('Passing HiveConf: %s', self.hiveconfs)
        self.hook.run_cli(hql=self.hql, schema=self.schema, hive_conf=self.hiveconfs)

    def dry_run(self) -> None:
        # Reset airflow environment variables to prevent
        # existing env vars from impacting behavior.
        self.clear_airflow_vars()

        self.hook = self.get_hook()
        self.hook.test_hql(hql=self.hql)

    def on_kill(self) -> None:
        if self.hook:
            self.hook.kill()

    def clear_airflow_vars(self) -> None:
        """Reset airflow environment variables to prevent existing ones from impacting behavior."""
        blank_env_vars = {
            value['env_var_format']: '' for value in operator_helpers.AIRFLOW_VAR_NAME_FORMAT_MAPPING.values()
        }
        os.environ.update(blank_env_vars)

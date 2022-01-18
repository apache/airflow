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

import functools
import logging
from typing import TYPE_CHECKING, Any, Collection, Optional, Set, Type

import jinja2
from sqlalchemy import MetaData, String
from sqlalchemy.ext.declarative import declarative_base

from airflow.configuration import conf
from airflow.templates import SandboxedEnvironment
from airflow.utils.weight_rule import WeightRule

if TYPE_CHECKING:
    from airflow.models.dag import DAG

SQL_ALCHEMY_SCHEMA = conf.get("core", "SQL_ALCHEMY_SCHEMA")

metadata = (
    None if not SQL_ALCHEMY_SCHEMA or SQL_ALCHEMY_SCHEMA.isspace() else MetaData(schema=SQL_ALCHEMY_SCHEMA)
)
Base = declarative_base(metadata=metadata)  # type: Any

ID_LEN = 250


class Operator:
    """Common interface for operators, including unmapped and mapped."""

    log: logging.Logger

    upstream_task_ids: Set[str]
    downstream_task_ids: Set[str]
    weight_rule: str
    priority_weight: int

    # For derived classes to define which fields will get jinjaified.
    template_fields: Collection[str]
    # Defines which files extensions to look for in the templated fields.
    template_ext: Collection[str]

    def get_dag(self) -> "Optional[DAG]":
        raise NotImplementedError()

    @property
    def dag_id(self) -> str:
        """Returns dag id if it has one or an adhoc + owner"""
        dag = self.get_dag()
        if dag:
            return self.dag.dag_id
        return f"adhoc_{self.owner}"

    def get_template_env(self) -> jinja2.Environment:
        """Fetch a Jinja template environment from the DAG or instantiate empty environment if no DAG."""
        dag = self.get_dag()
        if dag:
            return dag.get_template_env()
        return SandboxedEnvironment(cache_size=0)

    def prepare_template(self) -> None:
        """Hook triggered after the templated fields get replaced by their content.

        If you need your operator to alter the content of the file before the
        template is rendered, it should override this method to do so.
        """

    def resolve_template_files(self) -> None:
        """Getting the content of files for template_field / template_ext."""
        if self.template_ext:
            for field in self.template_fields:
                content = getattr(self, field, None)
                if content is None:
                    continue
                elif isinstance(content, str) and any(content.endswith(ext) for ext in self.template_ext):
                    env = self.get_template_env()
                    try:
                        setattr(self, field, env.loader.get_source(env, content)[0])  # type: ignore
                    except Exception:
                        self.log.exception("Failed to resolve template field %r", field)
                elif isinstance(content, list):
                    env = self.get_template_env()
                    for i, item in enumerate(content):
                        if isinstance(item, str) and any(item.endswith(ext) for ext in self.template_ext):
                            try:
                                content[i] = env.loader.get_source(env, item)[0]  # type: ignore
                            except Exception as e:
                                self.log.exception(e)
        self.prepare_template()

    def get_direct_relative_ids(self, upstream: bool = False) -> Set[str]:
        """Get direct relative IDs to the current task, upstream or downstream."""
        if upstream:
            return self.upstream_task_ids
        return self.downstream_task_ids

    def get_flat_relative_ids(
        self,
        upstream: bool = False,
        found_descendants: Optional[Set[str]] = None,
    ) -> Set[str]:
        """Get a flat set of relative IDs, upstream or downstream."""
        dag = self.get_dag()
        if not dag:
            return set()

        if not found_descendants:
            found_descendants = set()
        relative_ids = self.get_direct_relative_ids(upstream)

        for relative_id in relative_ids:
            if relative_id not in found_descendants:
                found_descendants.add(relative_id)
                relative_task = dag.task_dict[relative_id]
                relative_task.get_flat_relative_ids(upstream, found_descendants)

        return found_descendants

    @property
    def priority_weight_total(self) -> int:
        """
        Total priority weight for the task. It might include all upstream or downstream tasks.

        Depending on the weight rule:

        - WeightRule.ABSOLUTE - only own weight
        - WeightRule.DOWNSTREAM - adds priority weight of all downstream tasks
        - WeightRule.UPSTREAM - adds priority weight of all upstream tasks
        """
        if self.weight_rule == WeightRule.ABSOLUTE:
            return self.priority_weight
        elif self.weight_rule == WeightRule.DOWNSTREAM:
            upstream = False
        elif self.weight_rule == WeightRule.UPSTREAM:
            upstream = True
        else:
            upstream = False
        dag = self.get_dag()
        if dag is None:
            return self.priority_weight
        return self.priority_weight + sum(
            dag.task_dict[task_id].priority_weight
            for task_id in self.get_flat_relative_ids(upstream=upstream)
        )


def get_id_collation_args():
    """Get SQLAlchemy args to use for COLLATION"""
    collation = conf.get('core', 'sql_engine_collation_for_ids', fallback=None)
    if collation:
        return {'collation': collation}
    else:
        # Automatically use utf8mb3_bin collation for mysql
        # This is backwards-compatible. All our IDS are ASCII anyway so even if
        # we migrate from previously installed database with different collation and we end up mixture of
        # COLLATIONS, it's not a problem whatsoever (and we keep it small enough so that our indexes
        # for MYSQL will not exceed the maximum index size.
        #
        # See https://github.com/apache/airflow/pull/17603#issuecomment-901121618.
        #
        # We cannot use session/dialect as at this point we are trying to determine the right connection
        # parameters, so we use the connection
        conn = conf.get('core', 'sql_alchemy_conn', fallback='')
        if conn.startswith('mysql') or conn.startswith("mariadb"):
            return {'collation': 'utf8mb3_bin'}
        return {}


COLLATION_ARGS = get_id_collation_args()

StringID: Type[String] = functools.partial(String, length=ID_LEN, **COLLATION_ARGS)  # type: ignore

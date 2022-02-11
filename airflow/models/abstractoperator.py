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

import datetime
from typing import TYPE_CHECKING, Any, Callable, Collection, Dict, List, Optional, Set, Type, Union

from airflow.compat.functools import cached_property
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models.taskmixin import DAGNode
from airflow.utils.context import Context
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.weight_rule import WeightRule

if TYPE_CHECKING:
    import jinja2  # Slow import.

    from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
    from airflow.models.dag import DAG
    from airflow.models.operator import Operator

DEFAULT_OWNER = conf.get("operators", "default_owner")
DEFAULT_POOL_SLOTS = 1
DEFAULT_PRIORITY_WEIGHT = 1
DEFAULT_QUEUE = conf.get("operators", "default_queue")
DEFAULT_RETRIES = conf.getint("core", "default_task_retries", fallback=0)
DEFAULT_RETRY_DELAY = datetime.timedelta(seconds=300)
DEFAULT_WEIGHT_RULE = conf.get("core", "default_task_weight_rule", fallback=WeightRule.DOWNSTREAM)
DEFAULT_TRIGGER_RULE = TriggerRule.ALL_SUCCESS

TaskStateChangeCallback = Callable[[Context], None]


class AbstractOperator(LoggingMixin, DAGNode):
    """Common implementation for operators, including unmapped and mapped.

    This base class is more about sharing implementations, not defining a common
    interface. Unfortunately it's difficult to use this as the common base class
    for typing due to BaseOperator carrying too much historical baggage.

    The union type ``from airflow.models.operator import Operator`` is easier
    to use for typing purposes.

    :meta private:
    """

    operator_class: Union[str, Type["BaseOperator"]]

    weight_rule: str
    priority_weight: int

    # Defines the operator level extra links.
    operator_extra_links: Collection["BaseOperatorLink"]
    # For derived classes to define which fields will get jinjaified.
    template_fields: Collection[str]
    # Defines which files extensions to look for in the templated fields.
    template_ext: Collection[str]

    owner: str
    task_id: str

    def get_dag(self) -> "Optional[DAG]":
        raise NotImplementedError()

    @property
    def task_type(self) -> str:
        raise NotImplementedError()

    @property
    def inherits_from_dummy_operator(self) -> bool:
        raise NotImplementedError()

    @property
    def dag_id(self) -> str:
        """Returns dag id if it has one or an adhoc + owner"""
        dag = self.get_dag()
        if dag:
            return dag.dag_id
        return f"adhoc_{self.owner}"

    @property
    def node_id(self) -> str:
        return self.task_id

    def get_template_env(self) -> "jinja2.Environment":
        """Fetch a Jinja template environment from the DAG or instantiate empty environment if no DAG."""
        # This is imported locally since Jinja2 is heavy and we don't need it
        # for most of the functionalities. It is imported by get_template_env()
        # though, so we don't need to put this after the 'if dag' check.
        from airflow.templates import SandboxedEnvironment

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

    def get_flat_relatives(self, upstream: bool = False) -> Collection["Operator"]:
        """Get a flat list of relatives, either upstream or downstream."""
        dag = self.get_dag()
        if not dag:
            return set()
        return [dag.task_dict[task_id] for task_id in self.get_flat_relative_ids(upstream)]

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

    @cached_property
    def operator_extra_link_dict(self) -> Dict[str, Any]:
        """Returns dictionary of all extra links for the operator"""
        op_extra_links_from_plugin: Dict[str, Any] = {}
        from airflow import plugins_manager

        plugins_manager.initialize_extra_operators_links_plugins()
        if plugins_manager.operator_extra_links is None:
            raise AirflowException("Can't load operators")
        for ope in plugins_manager.operator_extra_links:
            if ope.operators and self.operator_class in ope.operators:
                op_extra_links_from_plugin.update({ope.name: ope})

        operator_extra_links_all = {link.name: link for link in self.operator_extra_links}
        # Extra links defined in Plugins overrides operator links defined in operator
        operator_extra_links_all.update(op_extra_links_from_plugin)

        return operator_extra_links_all

    @cached_property
    def global_operator_extra_link_dict(self) -> Dict[str, Any]:
        """Returns dictionary of all global extra links"""
        from airflow import plugins_manager

        plugins_manager.initialize_extra_operators_links_plugins()
        if plugins_manager.global_operator_extra_links is None:
            raise AirflowException("Can't load operators")
        return {link.name: link for link in plugins_manager.global_operator_extra_links}

    @cached_property
    def extra_links(self) -> List[str]:
        return list(set(self.operator_extra_link_dict).union(self.global_operator_extra_link_dict))

    def get_extra_links(self, dttm: datetime.datetime, link_name: str) -> Optional[Dict[str, Any]]:
        """For an operator, gets the URLs that the ``extra_links`` entry points to.

        :raise ValueError: The error message of a ValueError will be passed on through to
            the fronted to show up as a tooltip on the disabled link.
        :param dttm: The datetime parsed execution date for the URL being searched for.
        :param link_name: The name of the link we're looking for the URL for. Should be
            one of the options specified in ``extra_links``.
        """
        if link_name in self.operator_extra_link_dict:
            return self.operator_extra_link_dict[link_name].get_link(self, dttm)
        elif link_name in self.global_operator_extra_link_dict:
            return self.global_operator_extra_link_dict[link_name].get_link(self, dttm)
        return None

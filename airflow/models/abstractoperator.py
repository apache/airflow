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
import inspect
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Collection,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Type,
    Union,
)

from airflow.compat.functools import cached_property
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models.taskmixin import DAGNode
from airflow.utils.context import Context
from airflow.utils.helpers import render_template_as_native, render_template_to_string
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.weight_rule import WeightRule

TaskStateChangeCallback = Callable[[Context], None]

if TYPE_CHECKING:
    import jinja2  # Slow import.
    from sqlalchemy.orm import Session

    from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
    from airflow.models.dag import DAG
    from airflow.models.operator import Operator
    from airflow.models.taskinstance import TaskInstance

DEFAULT_OWNER: str = conf.get_mandatory_value("operators", "default_owner")
DEFAULT_POOL_SLOTS: int = 1
DEFAULT_PRIORITY_WEIGHT: int = 1
DEFAULT_QUEUE: str = conf.get_mandatory_value("operators", "default_queue")
DEFAULT_RETRIES: int = conf.getint("core", "default_task_retries", fallback=0)
DEFAULT_RETRY_DELAY: datetime.timedelta = datetime.timedelta(seconds=300)
DEFAULT_WEIGHT_RULE: WeightRule = WeightRule(
    conf.get("core", "default_task_weight_rule", fallback=WeightRule.DOWNSTREAM)
)
DEFAULT_TRIGGER_RULE: TriggerRule = TriggerRule.ALL_SUCCESS
DEFAULT_TASK_EXECUTION_TIMEOUT: Optional[datetime.timedelta] = conf.gettimedelta(
    "core", "default_task_execution_timeout"
)


class AbstractOperator(LoggingMixin, DAGNode):
    """Common implementation for operators, including unmapped and mapped.

    This base class is more about sharing implementations, not defining a common
    interface. Unfortunately it's difficult to use this as the common base class
    for typing due to BaseOperator carrying too much historical baggage.

    The union type ``from airflow.models.operator import Operator`` is easier
    to use for typing purposes.

    :meta private:
    """

    operator_class: Union[Type["BaseOperator"], Dict[str, Any]]

    weight_rule: str
    priority_weight: int

    # Defines the operator level extra links.
    operator_extra_links: Collection["BaseOperatorLink"]
    # For derived classes to define which fields will get jinjaified.
    template_fields: Collection[str]
    # Defines which files extensions to look for in the templated fields.
    template_ext: Sequence[str]

    owner: str
    task_id: str

    HIDE_ATTRS_FROM_UI: ClassVar[FrozenSet[str]] = frozenset(
        (
            'log',
            'dag',  # We show dag_id, don't need to show this too
            'node_id',  # Duplicates task_id
            'task_group',  # Doesn't have a useful repr, no point showing in UI
            'inherits_from_empty_operator',  # impl detail
            # For compatibility with TG, for operators these are just the current task, no point showing
            'roots',
            'leaves',
            # These lists are already shown via *_task_ids
            'upstream_list',
            'downstream_list',
            # Not useful, implementation detail, already shown elsewhere
            'global_operator_extra_link_dict',
            'operator_extra_link_dict',
        )
    )

    def get_dag(self) -> "Optional[DAG]":
        raise NotImplementedError()

    @property
    def task_type(self) -> str:
        raise NotImplementedError()

    @property
    def inherits_from_empty_operator(self) -> bool:
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
            return dag.get_template_env(force_sandboxed=False)
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
                            except Exception:
                                self.log.exception("Failed to get source %s", item)
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

    def get_extra_links(self, ti: "TaskInstance", link_name: str) -> Optional[str]:
        """For an operator, gets the URLs that the ``extra_links`` entry points to.

        :meta private:

        :raise ValueError: The error message of a ValueError will be passed on through to
            the fronted to show up as a tooltip on the disabled link.
        :param ti: The TaskInstance for the URL being searched for.
        :param link_name: The name of the link we're looking for the URL for. Should be
            one of the options specified in ``extra_links``.
        """
        link: Optional["BaseOperatorLink"] = self.operator_extra_link_dict.get(link_name)
        if not link:
            link = self.global_operator_extra_link_dict.get(link_name)
            if not link:
                return None
        # Check for old function signature
        parameters = inspect.signature(link.get_link).parameters
        args = [name for name, p in parameters.items() if p.kind != p.VAR_KEYWORD]
        if "ti_key" in args:
            return link.get_link(self, ti_key=ti.key)
        else:
            return link.get_link(self, ti.dag_run.logical_date)  # type: ignore[misc]
        return None

    def render_template_fields(
        self,
        context: Context,
        jinja_env: Optional["jinja2.Environment"] = None,
    ) -> Optional["BaseOperator"]:
        """Template all attributes listed in template_fields.

        If the operator is mapped, this should return the unmapped, fully
        rendered, and map-expanded operator. The mapped operator should not be
        modified.

        If the operator is not mapped, this should modify the operator in-place
        and return either *None* (for backwards compatibility) or *self*.
        """
        raise NotImplementedError()

    @provide_session
    def _do_render_template_fields(
        self,
        parent: Any,
        template_fields: Iterable[str],
        context: Context,
        jinja_env: "jinja2.Environment",
        seen_oids: Set,
        *,
        session: "Session" = NEW_SESSION,
    ) -> None:
        for attr_name in template_fields:
            try:
                value = getattr(parent, attr_name)
            except AttributeError:
                raise AttributeError(
                    f"{attr_name!r} is configured as a template field "
                    f"but {parent.task_type} does not have this attribute."
                )
            if not value:
                continue
            rendered_content = self.render_template(
                value,
                context,
                jinja_env,
                seen_oids,
            )
            setattr(parent, attr_name, rendered_content)

    def render_template(
        self,
        content: Any,
        context: Context,
        jinja_env: Optional["jinja2.Environment"] = None,
        seen_oids: Optional[Set] = None,
    ) -> Any:
        """Render a templated string.

        If *content* is a collection holding multiple templated strings, strings
        in the collection will be templated recursively.

        :param content: Content to template. Only strings can be templated (may
            be inside a collection).
        :param context: Dict with values to apply on templated content
        :param jinja_env: Jinja environment. Can be provided to avoid
            re-creating Jinja environments during recursion.
        :param seen_oids: template fields already rendered (to avoid
            *RecursionError* on circular dependencies)
        :return: Templated content
        """
        # "content" is a bad name, but we're stuck to it being public API.
        value = content
        del content

        if not jinja_env:
            jinja_env = self.get_template_env()

        from airflow.models.param import DagParam
        from airflow.models.xcom_arg import XComArg

        if isinstance(value, str):
            if any(value.endswith(ext) for ext in self.template_ext):  # A filepath.
                template = jinja_env.get_template(value)
            else:
                template = jinja_env.from_string(value)
            dag = self.get_dag()
            if dag and dag.render_template_as_native_obj:
                return render_template_as_native(template, context)
            return render_template_to_string(template, context)

        if isinstance(value, (DagParam, XComArg)):
            return value.resolve(context)

        # Fast path for common built-in collections.
        if value.__class__ is tuple:
            return tuple(self.render_template(element, context, jinja_env) for element in value)
        elif isinstance(value, tuple):  # Special case for named tuples.
            return value.__class__(*(self.render_template(el, context, jinja_env) for el in value))
        elif isinstance(value, list):
            return [self.render_template(element, context, jinja_env) for element in value]
        elif isinstance(value, dict):
            return {key: self.render_template(value, context, jinja_env) for key, value in value.items()}
        elif isinstance(value, set):
            return {self.render_template(element, context, jinja_env) for element in value}

        # More complex collections.
        if seen_oids is None:
            oids = set()
        else:
            oids = seen_oids
        self._render_nested_template_fields(value, context, jinja_env, oids)
        return value

    def _render_nested_template_fields(
        self,
        value: Any,
        context: Context,
        jinja_env: "jinja2.Environment",
        seen_oids: Set[int],
    ) -> None:
        if id(value) in seen_oids:
            return
        seen_oids.add(id(value))
        try:
            nested_template_fields = value.template_fields
        except AttributeError:
            # content has no inner template fields
            return
        self._do_render_template_fields(value, nested_template_fields, context, jinja_env, seen_oids)

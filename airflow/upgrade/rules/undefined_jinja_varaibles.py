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

from __future__ import absolute_import

import logging
import re

import jinja2
import six

from airflow import conf
from airflow.models import DagBag, TaskInstance
from airflow.upgrade.rules.base_rule import BaseRule
from airflow.utils import timezone


class UndefinedJinjaVariablesRule(BaseRule):

    title = "Jinja Template Variables cannot be undefined"

    description = """\
The default behavior for DAG's Jinja templates has changed. Now, more restrictive validation
of non-existent variables is applied - `jinja2.StrictUndefined`.

The user should do either of the following to fix this -
1. Fix the Jinja Templates by defining every variable or providing default values
2. Explicitly declare `template_undefined=jinja2.Undefined` while defining the DAG
"""

    def _check_rendered_content(self, rendered_content, seen_oids=None):
        """Replicates the logic in BaseOperator.render_template() to
        cover all the cases needed to be checked.
        """
        if isinstance(rendered_content, six.string_types):
            return set(re.findall(r"{{(.*?)}}", rendered_content))

        elif isinstance(rendered_content, (int, float, bool)):
            return set()

        elif isinstance(rendered_content, (tuple, list, set)):
            debug_error_messages = set()
            for element in rendered_content:
                debug_error_messages.update(self._check_rendered_content(element))
            return debug_error_messages

        elif isinstance(rendered_content, dict):
            debug_error_messages = set()
            for key, value in rendered_content.items():
                debug_error_messages.update(self._check_rendered_content(value))
            return debug_error_messages

        else:
            if seen_oids is None:
                seen_oids = set()
            return self._nested_check_rendered(rendered_content, seen_oids)

    def _nested_check_rendered(self, rendered_content, seen_oids):
        debug_error_messages = set()
        if id(rendered_content) not in seen_oids:
            seen_oids.add(id(rendered_content))
            nested_template_fields = rendered_content.template_fields
            for attr_name in nested_template_fields:
                nested_rendered_content = getattr(rendered_content, attr_name)

                if nested_rendered_content:
                    errors = list(
                        self._check_rendered_content(nested_rendered_content, seen_oids)
                    )
                    for i in range(len(errors)):
                        errors[i].strip()
                        errors[i] += " NestedTemplateField={}".format(attr_name)
                    debug_error_messages.update(errors)
        return debug_error_messages

    def _render_task_content(self, task, content, context):
        completed_rendering = False
        errors_while_rendering = []
        while not completed_rendering:
            # Catch errors such as {{ object.element }} where
            # object is not defined
            try:
                renderend_content = task.render_template(content, context)
                completed_rendering = True
            except Exception as e:
                undefined_variable = re.sub(" is undefined", "", str(e))
                undefined_variable = re.sub("'", "", undefined_variable)
                context[undefined_variable] = dict()
                message = "Could not find the object '{}'".format(undefined_variable)
                errors_while_rendering.append(message)
        return renderend_content, errors_while_rendering

    def iterate_over_template_fields(self, task):
        messages = {}
        task_instance = TaskInstance(task=task, execution_date=timezone.utcnow())
        context = task_instance.get_template_context()
        for attr_name in task.template_fields:
            content = getattr(task, attr_name)
            if content:
                rendered_content, errors_while_rendering = self._render_task_content(
                    task, content, context
                )
                debug_error_messages = list(
                    self._check_rendered_content(rendered_content, set())
                )
                messages[attr_name] = errors_while_rendering + debug_error_messages

        return messages

    def iterate_over_dag_tasks(self, dag):
        dag.template_undefined = jinja2.DebugUndefined
        tasks = dag.tasks
        messages = {}
        for task in tasks:
            error_messages = self.iterate_over_template_fields(task)
            messages[task.task_id] = error_messages
        return messages

    def check(self, dagbag=None):
        if not dagbag:
            logger = logging.root
            old_level = logger.level
            try:
                logger.setLevel(logging.ERROR)
                dag_folder = conf.get("core", "dags_folder")
                dagbag = DagBag(dag_folder)
            finally:
                logger.setLevel(old_level)
        dags = dagbag.dags
        messages = []
        for dag_id, dag in dags.items():
            if dag.template_undefined:
                continue
            dag_messages = self.iterate_over_dag_tasks(dag)

            for task_id, task_messages in dag_messages.items():
                for attr_name, error_messages in task_messages.items():
                    for error_message in error_messages:
                        message = (
                            "Possible UndefinedJinjaVariable -> DAG: {}, Task: {}, "
                            "Attribute: {}, Error: {}".format(
                                dag_id, task_id, attr_name, error_message.strip()
                            )
                        )
                        messages.append(message)
        return messages

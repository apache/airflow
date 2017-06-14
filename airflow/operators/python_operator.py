# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from builtins import str
import logging

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, SkipMixin
from airflow.utils.decorators import apply_defaults


class PythonOperator(BaseOperator):
    """
    Executes a Python callable

    :param python_callable: A reference to an object that is callable
    :type python_callable: python callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function
    :type op_kwargs: dict
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable
    :type op_args: list
    :param provide_context: if set to true, Airflow will pass a set of
        keyword arguments that can be used in your function. This set of
        kwargs correspond exactly to what you can use in your jinja
        templates. For this to work, you need to define `**kwargs` in your
        function header.
    :type provide_context: bool
    :param templates_dict: a dictionary where the values are templates that
        will get templated by the Airflow engine sometime between
        ``__init__`` and ``execute`` takes place and are made available
        in your callable's context after the template has been applied
    :type templates_dict: dict of str
    :param templates_exts: a list of file extensions to resolve while
        processing templated fields, for examples ``['.sql', '.hql']``
    """
    template_fields = ('templates_dict',)
    template_ext = tuple()
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(
            self,
            python_callable,
            op_args=None,
            op_kwargs=None,
            provide_context=False,
            templates_dict=None,
            templates_exts=None,
            *args, **kwargs):
        super(PythonOperator, self).__init__(*args, **kwargs)
        if not callable(python_callable):
            raise AirflowException('`python_callable` param must be callable')
        self.python_callable = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.provide_context = provide_context
        self.templates_dict = templates_dict
        if templates_exts:
            self.template_ext = templates_exts

    def execute(self, context):
        if self.provide_context:
            context.update(self.op_kwargs)
            context['templates_dict'] = self.templates_dict
            self.op_kwargs = context

        return_value = self.python_callable(*self.op_args, **self.op_kwargs)
        logging.info("Done. Returned value was: " + str(return_value))
        return return_value


class BranchPythonOperator(PythonOperator, SkipMixin):
    """
    Allows a workflow to "branch" or follow a single path following the
    execution of this task.

    It derives the PythonOperator and expects a Python function that returns
    the task_id to follow. The task_id returned should point to a task
    directly downstream from {self}. All other "branches" or
    directly downstream tasks are marked with a state of ``skipped`` so that
    these paths can't move forward. The ``skipped`` states are propageted
    downstream to allow for the DAG state to fill up and the DAG run's state
    to be inferred.

    Note that using tasks with ``depends_on_past=True`` downstream from
    ``BranchPythonOperator`` is logically unsound as ``skipped`` status
    will invariably lead to block tasks that depend on their past successes.
    ``skipped`` states propagates where all directly upstream tasks are
    ``skipped``.
    """
    def execute(self, context):
        branch = super(BranchPythonOperator, self).execute(context)
        logging.info("Following branch {}".format(branch))
        logging.info("Marking other directly downstream tasks as skipped")

        downstream_tasks = context['task'].downstream_list
        logging.debug("Downstream task_ids {}".format(downstream_tasks))

        skip_tasks = [t for t in downstream_tasks if t.task_id != branch]
        if downstream_tasks:
            self.skip(context['dag_run'], context['ti'].execution_date, skip_tasks)

        logging.info("Done.")


class ShortCircuitOperator(PythonOperator, SkipMixin):
    """
    Allows a workflow to continue only if a condition is met. Otherwise, the
    workflow "short-circuits" and downstream tasks are skipped.

    The ShortCircuitOperator is derived from the PythonOperator. It evaluates a
    condition and short-circuits the workflow if the condition is False. Any
    downstream tasks are marked with a state of "skipped". If the condition is
    True, downstream tasks proceed as normal.

    The condition is determined by the result of `python_callable`.
    """
    def execute(self, context):
        condition = super(ShortCircuitOperator, self).execute(context)
        logging.info("Condition result is {}".format(condition))

        if condition:
            logging.info('Proceeding with downstream tasks...')
            return

        logging.info('Skipping downstream tasks...')

        downstream_tasks = context['task'].get_flat_relatives(upstream=False)
        logging.debug("Downstream task_ids {}".format(downstream_tasks))

        if downstream_tasks:
            self.skip(context['dag_run'], context['ti'].execution_date, downstream_tasks)

        logging.info("Done.")

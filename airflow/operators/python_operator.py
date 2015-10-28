from builtins import str
from datetime import datetime
import logging

from airflow.models import BaseOperator, TaskInstance
from airflow.utils import apply_defaults, State
from airflow import settings


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


class BranchPythonOperator(PythonOperator):
    """
    Allows a workflow to "branch" or follow a single path following the
    execution of this task.

    It derives the PythonOperator and expects a Python function that returns
    the task_id to follow. The task_id returned should point to a task
    directely downstream from {self}. All other "branches" or
    directly downstream tasks are marked with a state of "skipped" so that
    these paths can't move forward.
    """
    def execute(self, context):
        branch = super(BranchPythonOperator, self).execute(context)
        logging.info("Following branch " + branch)
        logging.info("Marking other directly downstream tasks as skipped")
        session = settings.Session()
        for task in context['task'].downstream_list:
            if task.task_id != branch:
                ti = TaskInstance(
                    task, execution_date=context['ti'].execution_date)
                ti.state = State.SKIPPED
                ti.start_date = datetime.now()
                ti.end_date = datetime.now()
                session.merge(ti)
        session.commit()
        session.close()
        logging.info("Done.")


class ShortCircuitOperator(PythonOperator):
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
        else:
            logging.info('Skipping downstream tasks...')
            session = settings.Session()
            for task in context['task'].downstream_list:
                ti = TaskInstance(
                    task, execution_date=context['ti'].execution_date)
                ti.state = State.SKIPPED
                ti.start_date = datetime.now()
                ti.end_date = datetime.now()
                session.merge(ti)
            session.commit()
            session.close()
            logging.info("Done.")

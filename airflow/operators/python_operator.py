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
    """
    template_fields = tuple()
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(
            self,
            python_callable,
            op_args=None,
            op_kwargs=None,
            provide_context=False,
            *args, **kwargs):
        super(PythonOperator, self).__init__(*args, **kwargs)
        self.python_callable = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.provide_context = provide_context

    def execute(self, context):
        if self.provide_context:
            context.update(self.op_kwargs)
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
    directly downstream tasks are marked wit a state of "skipped" so that
    these paths can't move forward.
    """
    def execute(self, context):
        branch = super(BranchPythonOperator, self).execute(context)
        logging.info("Following branch " + branch)
        logging.info("Marking other directly downstream tasks as failed")
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

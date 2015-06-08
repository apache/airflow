import logging

from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class PythonOperator(BaseOperator):
    '''
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
        funciton header.
    :type provide_context: bool
    '''
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

from datetime import datetime
import logging

from airflow.models import BaseOperator, DagRun
from airflow.utils import apply_defaults, State
from airflow import settings


class DagRunOrder(object):
    def __init__(self, run_id=None, payload=None):
        self.run_id = run_id
        self.payload = payload


class TriggerDagRunOperator(BaseOperator):
    """
    Triggers a DAG run for a specified ``dag_id`` if a criteria is met

    :param dag_id: the dag_id to trigger
    :type dag_id: str
    :param python_callable: a reference to a python function that will be
        called while passing it the ``context`` object and a placeholder
        object ``obj`` for your callable to fill and return if you want
        a DagRun created. This ``obj`` object contains a ``run_id`` and
        ``payload`` attribute that you can modify in your function.
        The ``run_id`` should be a unique identifier for that DAG run, and
        the payload has to be a picklable object that will be made available
        to your tasks while executing that DAG run. Your function header
        should look like ``def foo(context, dag_run_obj):``
    :type python_callable: python callable
    """
    template_fields = tuple()
    template_ext = tuple()
    ui_color = '#ffefeb'
    @apply_defaults
    def __init__(
            self,
            dag_id,
            python_callable,
            *args, **kwargs):
        super(TriggerDagRunOperator, self).__init__(*args, **kwargs)
        self.python_callable = python_callable
        self.dag_id = dag_id

    def execute(self, context):
        dro = DagRunOrder(run_id='trig__' + datetime.now().isoformat())
        dro = self.python_callable(context, dro)
        if dro:
            session = settings.Session()
            dr = DagRun(
                dag_id=self.dag_id,
                run_id=dro.run_id,
                conf=dro.payload,
                external_trigger=True)
            logging.info("Creating DagRun {}".format(dr))
            session.add(dr)
            session.commit()
            session.close()
        else:
            logging.info("Criteria not met, moving on")

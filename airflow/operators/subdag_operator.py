from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.executors import DEFAULT_EXECUTOR


class SubDagOperator(BaseOperator):

    template_fields = tuple()
    ui_color = '#555'
    ui_fgcolor = '#fff'

    @apply_defaults
    def __init__(
            self,
            subdag,
            executor=DEFAULT_EXECUTOR,
            *args, **kwargs):
        """
        Yo dawg. This runs a sub dag. By convention, a sub dag's dag_id
        should be prefixed by its parent and a dot. As in `parent.child`.

        :param subdag: the DAG object to run as a subdag of the current DAG.
        :type subdag: airflow.DAG
        :param dag: the parent DAG
        :type subdag: airflow.DAG
        """
        if 'dag' not in kwargs:
            raise AirflowException("Please pass in the `dag` param")
        dag = kwargs['dag']
        super(SubDagOperator, self).__init__(*args, **kwargs)
        if dag.dag_id + '.' + kwargs['task_id'] != subdag.dag_id:
            raise AirflowException(
                "The subdag's dag_id should have the form "
                "'{{parent_dag_id}}.{{this_task_id}}'. Expected "
                "'{d}.{t}'; received '{rcvd}'.".format(
                    d=dag.dag_id, t=kwargs['task_id'], rcvd=subdag.dag_id))
        self.subdag = subdag
        self.executor = executor

    def execute(self, context):
        ed = context['execution_date']
        self.subdag.run(
            start_date=ed, end_date=ed, donot_pickle=True,
            executor=self.executor)

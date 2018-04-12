import datetime

import logging
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


class First(PythonOperator):

    def get_the_callable(self):

        def the(**kwargs):
            logging.warning("first thinks the run id is {}".format(kwargs.get('run_id')))

        return the

    def __init__(self):
        super().__init__(
            task_id=self.__class__.__name__,
            python_callable=self.get_the_callable(),
            start_date=datetime.datetime.now(),
            default_args={'provide_context': True}
        )


class Second(PythonOperator):

    def get_the_callable(self):

        def the(**kwargs):
            logging.warning("second thinks the run id is {}".format(kwargs.get('run_id')))

        return the

    def __init__(self):
        super().__init__(
            task_id=self.__class__.__name__,
            python_callable=self.get_the_callable(),
            start_date=datetime.datetime.now(),
            default_args={'provide_context': True}
        )

dag = DAG('the_dagiest_dag_of_all_the_dags')
first = First()
second = Second()
third = BashOperator(task_id='third', bash_command='echo " third {{ run_id }} "', start_date=datetime.datetime.now())
fourth = BashOperator(task_id='fourth', bash_command='echo " fourth {{ run_id }} "', start_date=datetime.datetime.now())
first.dag = dag
second.dag = dag
third.dag = dag
second.set_upstream(first)
third.set_upstream(second)
fourth.set_upstream(third)

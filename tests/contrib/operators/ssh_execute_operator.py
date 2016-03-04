import unittest
from datetime import datetime

from airflow import configuration
from airflow.settings import Session
from airflow import models, DAG
from airflow.contrib.operators.ssh_execute_operator import SSHExecuteOperator


TEST_DAG_ID = 'unit_tests'
DEFAULT_DATE = datetime(2015, 1, 1)
configuration.test_mode()


def reset(dag_id=TEST_DAG_ID):
    session = Session()
    tis = session.query(models.TaskInstance).filter_by(dag_id=dag_id)
    tis.delete()
    session.commit()
    session.close()

reset()


class SSHExecuteOperatorTest(unittest.TestCase):
    def setUp(self):
        configuration.test_mode()
        from airflow.contrib.hooks.ssh_hook import SSHHook
        hook = SSHHook()
        hook.no_host_key_check = True
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE,
            'provide_context': True
        }
        dag = DAG(TEST_DAG_ID+'test_schedule_dag_once', default_args=args)
        dag.schedule_interval = '@once'
        self.hook = hook
        self.dag = dag

    def test_simple(self):
        task = SSHExecuteOperator(
            task_id="test",
            bash_command="echo airflow",
            ssh_hook=self.hook,
            dag=self.dag,
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_with_env(self):
        task = SSHExecuteOperator(
            task_id="test",
            bash_command="echo $AIRFLOW_HOME",
            ssh_hook=self.hook,
            env={"AIRFLOW_test": "test"},
            dag=self.dag,
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)


if __name__ == '__main__':
    unittest.main()

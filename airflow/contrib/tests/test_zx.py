import unittest
from airflow.contrib import operators
from airflow import configuration, models, DAG
from datetime import datetime, time, timedelta

DEFAULT_DATE = datetime(2015, 1, 1)
DEV_NULL = '/dev/null'
TEST_DAG_ID = 'unit_tests'

class TestZX(unittest.TestCase):
    
    
    def setUp(self):
        configuration.test_mode()
        self.dagbag = models.DagBag(
        dag_folder=DEV_NULL, include_examples=True)
        self.args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=self.args)
        self.dag = dag
        
    def test_ecs_operator(self):  
        
        overrides={
         'containerOverrides': [
             {
                 'name': 'hello-world'
             }
         ]                 
        } 

        t = operators.ECSOperator(task_id='hello-world', taskDefinition="hello-world-task",cluster="default", overrides=overrides,dag=self.dag )
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)
        
        
    def test_long_running_operation(self):
        overrides={
         'containerOverrides': [
             {
                'name': 'mdm',
                'command' : ['python', '-u', 'extract_bseller.py']
             }
         ]                 
        } 
 
        op = operators.ECSOperator(task_id='extract-bseller-customers', taskDefinition="mdm-task",cluster="default", overrides=overrides,dag=self.dag )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)
        
if __name__ == '__main__':
    unittest.main()
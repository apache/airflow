import datetime as dt
from unittest import mock, TestCase
from botocore import loaders as botoloader
from airflow.configuration import conf
from airflow.executors.fargate_executor import *


class TestFargateTaskCollection(TestCase):
    def test_add(self):
        self.assertEqual(len(self.collection), 2)

        # Check basic get for first task
        self.assertEqual(self.collection.task_by_arn('001'), self.first_task)
        self.assertEqual(self.collection['001'], self.first_task)
        self.assertEqual(self.collection.task_by_key(self.first_airflow_key), self.first_task)

        # Check basic get for second task
        self.assertEqual(self.collection.task_by_arn('002'), self.second_task)
        self.assertEqual(self.collection['002'], self.second_task)
        self.assertEqual(self.collection.task_by_key(self.second_airflow_key), self.second_task)

    def test_list(self):
        # Check basic list by ARNs & airflow-task-keys
        self.assertListEqual(self.collection.get_all_arns(), ['001', '002'])
        self.assertListEqual(self.collection.get_all_task_keys(), [self.first_airflow_key, self.second_airflow_key])

    def test_pop(self):
        # pop first task & ensure that it's removed
        self.assertEqual(self.collection.pop_by_arn('001'), self.first_task)
        self.assertNotIn('001', self.collection.get_all_arns())
        self.collection.add_task(self.first_task, self.first_airflow_key)
        self.assertEqual(self.collection.pop_by_key(self.first_airflow_key), self.first_task)
        self.assertNotIn('001', self.collection.get_all_arns())

    def test_update(self):
        # update arn with new task object
        self.collection.add_task(self.first_task, self.first_airflow_key)
        self.assertEqual(self.collection['001'], self.first_task)
        updated_task = FargateTask({'taskArn': '001'})
        self.collection.update_task(updated_task)
        self.assertEqual(self.collection['001'], updated_task)

    def test_task_state(self):
        running_fargate_task = FargateTask({
            'taskArn': 'ABC',
            'containers': [{
                'lastStatus': 'PENDING',
                'exitCode': 0
            }]
        })

        success_fargate_task = FargateTask({
            'taskArn': 'ABC',
            'containers': [{
                'lastStatus': 'STOPPED',
                'exitCode': 0
            }]
        })

        failed_fargate_task = FargateTask({
            'taskArn': 'ABC',
            'containers': [{
                'lastStatus': 'STOPPED',
                'exitCode': 100
            }]
        })
        self.assertEqual(running_fargate_task.get_task_state(), State.RUNNING)
        self.assertEqual(success_fargate_task.get_task_state(), State.SUCCESS)
        self.assertEqual(failed_fargate_task.get_task_state(), State.FAILED)

    def setUp(self):
        self.collection = FargateTaskCollection()
        # Add first task
        self.first_task = FargateTask({'taskArn': '001'})
        self.first_airflow_key = mock.Mock(spec=TaskInstanceKeyType)
        self.collection.add_task(self.first_task, self.first_airflow_key)
        # Add second task
        self.second_task = FargateTask({'taskArn': '002'})
        self.second_airflow_key = mock.Mock(spec=TaskInstanceKeyType)
        self.collection.add_task(self.second_task, self.second_airflow_key)


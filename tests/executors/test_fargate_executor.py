import unittest
from unittest.mock import Mock

from airflow.executors.fargate_executor import *


class TestFargateTaskCollection(unittest.TestCase):
    def test_fargate_collection(self):
        collection = FargateTaskCollection()
        # Add first task
        first_task = FargateTask({'arn': '001'})
        first_airflow_key = Mock(spec=TaskInstanceKeyType)
        collection.add_task(first_task, first_airflow_key)
        # Add second task
        second_task = FargateTask({'arn': '002'})
        second_airflow_key = Mock(spec=TaskInstanceKeyType)
        collection.add_task(second_task, second_airflow_key)

        # Check basic get for first task
        self.assertEqual(len(collection), 2)
        self.assertEqual(collection.task_by_arn('001'), first_task)
        self.assertEqual(collection['001'], first_task)
        self.assertEqual(collection.task_by_key(first_airflow_key), first_task)

        # Check basic get for second task
        self.assertEqual(collection.task_by_arn('002'), second_task)
        self.assertEqual(collection['002'], second_task)
        self.assertEqual(collection.task_by_key(second_airflow_key), second_task)

        # Check basic list by ARNs & airflow-task-keys
        self.assertListEqual(collection.get_all_arns(), ['001', '002'])
        self.assertListEqual(collection.get_all_task_keys(), [first_airflow_key, second_airflow_key])

        # pop first task & ensure that it's removed
        self.assertEqual(collection.pop_by_arn('001'), first_task)
        self.assertNotIn('001', collection.get_all_arns())
        collection.add_task(first_task, first_airflow_key)
        self.assertEqual(collection.pop_by_key(first_airflow_key), first_task)
        self.assertNotIn('001', collection.get_all_arns())

        # update arn with new task object
        collection.add_task(first_task, first_airflow_key)
        self.assertEqual(collection['001'], first_task)
        updated_task = FargateTask({'arn': '001'})
        collection.update_task(updated_task)
        self.assertEqual(collection['001'], updated_task)


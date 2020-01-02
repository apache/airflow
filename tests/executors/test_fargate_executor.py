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


class TestFargateExecutor(TestCase):
    def test_configs(self):
        # region & cluster must be specified
        self.assertTrue(conf.has_option('fargate', 'region'))
        self.assertIsNotNone(conf.get('fargate', 'region'))

        self.assertTrue(conf.has_option('fargate', 'cluster'))
        self.assertIsNotNone(conf.get('fargate', 'cluster'))

        self.assertTrue(conf.has_option('fargate', 'execution_config_function'))
        self.assertIsNotNone(conf.get('fargate', 'execution_config_function'))

        default_func = 'airflow.executor.default_task_id_to_fargate_options_function'
        if conf.get('fargate', 'execution_config_function') == default_func:
            self.assertTrue(conf.has_option('fargate', 'task_definition'))
            self.assertIsNotNone(conf.get('fargate', 'task_definition'))

            self.assertTrue(conf.has_option('fargate', 'container_name'))
            self.assertIsNotNone(conf.get('fargate', 'container_name'))

            self.assertIsNotNone(conf.get('fargate', 'security_groups'))
            self.assertIsNotNone(conf.get('fargate', 'security_groups'))

            self.assertIsNotNone(conf.get('fargate', 'security_groups'))
            self.assertIsNotNone(conf.get('fargate', 'security_groups'))

            self.assertIsNotNone(conf.get('fargate', 'subnets'))
            self.assertIsNotNone(conf.get('fargate', 'subnets'))

            self.assertIsNotNone(conf.get('fargate', 'platform_version'))
            self.assertIsNotNone(conf.get('fargate', 'platform_version'))

            self.assertIsNotNone(conf.get('fargate', 'launch_type'))
            self.assertIsNotNone(conf.get('fargate', 'launch_type'))

            self.assertIsNotNone(conf.get('fargate', 'assign_public_ip'))
            self.assertIsNotNone(conf.get('fargate', 'assign_public_ip'))

    def test_execute(self):
        airflow_key = mock.Mock(spec=TaskInstanceKeyType)
        airflow_cmd = mock.Mock(spec=CommandType)
        self.executor.execute_async(airflow_key, airflow_cmd)

        ecs_mock = self.executor.ecs

        # ensure that run_task is called correctly as defined by Botocore docs
        ecs_mock.run_task.assert_called_once()
        self.assert_botocore_call('RunTask', *ecs_mock.run_task.call_args)

        # ensure that run_task is called correctly as defined by Botocore docs
        self.executor.ecs.run_task.assert_called_once()
        self.assert_botocore_call('RunTask', *self.executor.ecs.run_task.call_args)

        # task is stored in active worker
        self.assertEqual(len(self.executor.active_workers), 1)
        self.assertIn(self.executor.active_workers.task_by_key(airflow_key).task_arn, '001')

    @mock.patch('airflow.executors.base_executor.BaseExecutor.fail')
    def test_failed_execute_api(self, fail_mock):
        self.executor.ecs.run_task.return_value = {
            'tasks': [],
            'failures': [{
                'arn': '001',
                'reason': 'Sample Failure',
                'detail': 'UnitTest Failure - Please ignore'
            }]
        }

        airflow_key = mock.Mock(spec=TaskInstanceKeyType)
        airflow_cmd = mock.Mock(spec=CommandType)
        self.executor.execute_async(airflow_key, airflow_cmd)

        # task is not stored in active workers
        self.assertEqual(len(self.executor.active_workers), 0)
        # Task is immediately failed
        fail_mock.assert_called_once()

    @mock.patch('airflow.executors.base_executor.BaseExecutor.fail')
    @mock.patch('airflow.executors.base_executor.BaseExecutor.success')
    def test_sync(self, success_mock, fail_mock):
        after_fargate_task = self.__mock_sync()
        self.assertEqual(after_fargate_task.get_task_state(), State.SUCCESS)

        self.executor.sync()

        # ensure that run_task is called correctly as defined by Botocore docs
        self.executor.ecs.describe_tasks.assert_called_once()
        self.assert_botocore_call('DescribeTasks',
                                  *self.executor.ecs.describe_tasks.call_args)

        # task is not stored in active workers
        self.assertEqual(len(self.executor.active_workers), 0)
        # Task is immediately succeeded
        success_mock.assert_called_once()
        self.assertFalse(fail_mock.called)

    @mock.patch('airflow.executors.base_executor.BaseExecutor.fail')
    @mock.patch('airflow.executors.base_executor.BaseExecutor.success')
    def test_failed_sync(self, success_mock, fail_mock):
        after_fargate_task = self.__mock_sync()

        # set container's exit code to failure
        after_fargate_task.json['containers'][0]['exitCode'] = 100
        self.assertEqual(after_fargate_task.get_task_state(), State.FAILED)

        self.executor.sync()

        # ensure that run_task is called correctly as defined by Botocore docs
        self.executor.ecs.describe_tasks.assert_called_once()
        self.assert_botocore_call('DescribeTasks',
                                  *self.executor.ecs.describe_tasks.call_args)

        # task is not stored in active workers
        self.assertEqual(len(self.executor.active_workers), 0)
        # Task is immediately succeeded
        fail_mock.assert_called_once()
        self.assertFalse(success_mock.called)

    @mock.patch('airflow.executors.base_executor.BaseExecutor.fail')
    @mock.patch('airflow.executors.base_executor.BaseExecutor.success')
    def test_failed_sync_api(self, success_mock, fail_mock):
        self.__mock_sync()
        self.executor.ecs.describe_tasks.return_value = {
            'tasks': [],
            'failures': [{
                'arn': 'ABC',
                'reason': 'Sample Failure',
                'detail': 'UnitTest Failure - Please ignore'
            }]
        }

        # Call Sync 3 times with failures
        for check_count in range(FargateExecutor.MAX_FAILURE_CHECKS - 1):
            self.executor.sync()
            # ensure that run_task is called correctly as defined by Botocore docs
            self.assertEqual(self.executor.ecs.describe_tasks.call_count,
                             check_count + 1)
            self.assert_botocore_call('DescribeTasks',
                                      *self.executor.ecs.describe_tasks.call_args)

            # Ensure task arn is not removed from active
            self.assertIn('ABC', self.executor.active_workers.get_all_arns())

            # Task is not failed or succeeded
            self.assertFalse(fail_mock.called)
            self.assertFalse(success_mock.called)

        # Last call should fail the task
        self.executor.sync()
        self.assertNotIn('ABC', self.executor.active_workers.get_all_arns())
        self.assertTrue(fail_mock.called)
        self.assertFalse(success_mock.called)

    def assert_botocore_call(self, method_name, args, kwargs):
        """Asserts that a given method-call adheres to Botocore's API docs"""
        # Boto3 doesn't like args
        self.assertEqual(len(args), 0)
        # Now check kwargs, recursively
        input_shape_name = self.ecs_model['operations'][method_name]['input']['shape']
        input_shape = self.ecs_model['shapes'][input_shape_name]
        self.__assert_botocore_shape(input_shape, kwargs)

    def __assert_botocore_shape(self, input_shape, input_value):
        """Asserts that a given value adheres to Botocore's API docs"""
        if 'required' in input_shape:
            for required_key in input_shape['required']:
                self.assertIn(required_key, input_value.keys())

        if 'type' in input_shape:
            if input_shape['type'] == 'boolean':
                self.assertIsInstance(input_value, bool)
            elif input_shape['type'] == 'double':
                self.assertIsInstance(input_value, float)
            elif input_shape['type'] == 'integer':
                self.assertIsInstance(input_value, int)
            elif input_shape['type'] == 'list':
                self.assertIsInstance(input_value, list)
            elif input_shape['type'] == 'long':
                self.assertIsInstance(input_value, int)
            elif input_shape['type'] == 'map':
                self.assertIsInstance(input_value, dict)
            elif input_shape['type'] == 'string':
                self.assertIsInstance(input_value, str)
            elif input_shape['type'] == 'timestamp':
                self.assertIsInstance(input_value, (dt.datetime, dt.date))
            elif input_shape['type'] == 'structure':
                if 'member' in input_shape:
                    member_name = input_shape['member']['shape']
                    member_shape = self.ecs_model['shapes'][member_name]
                    if member_name in input_value:
                        member_value = input_value[member_name]
                        self.__assert_botocore_shape(member_shape, member_value)
                elif 'members' in input_shape:
                    for member_name in input_shape['members']:
                        shape_name = input_shape['members'][member_name]['shape']
                        member_shape = self.ecs_model['shapes'][shape_name]
                        if member_name in input_value:
                            member_value = input_value[member_name]
                            self.__assert_botocore_shape(member_shape, member_value)

        if 'enum' in input_shape:
            self.assertIn(input_value, input_shape['enum'])
        if 'min' in input_shape:
            self.assertGreaterEqual(input_value, input_shape['min'])
        if 'max' in input_shape:
            self.assertLessEqual(input_value, input_shape['max'])
        if 'pattern' in input_shape:
            self.assertRegex(input_value, input_shape['pattern'])

    def setUp(self) -> None:
        loader = botoloader.Loader()
        self.ecs_model = loader.load_service_model('ecs', 'service-2')
        self.__set_mocked_executor()

    def __set_mocked_executor(self):
        """Mock ECS such that there's nothing wrong with anything"""
        executor = FargateExecutor()
        executor.start()

        # replace boto3 ecs client with mock
        ecs_mock = mock.Mock()
        run_task_ret_val = {
            'tasks': [{'taskArn': '001'}],
            'failures': []
        }
        ecs_mock.run_task.return_value = run_task_ret_val
        executor.ecs = ecs_mock

        self.executor = executor

    def __mock_sync(self):
        """Mock ECS such that there's nothing wrong with anything"""

        # create running fargate instance
        before_fargate_task = mock.Mock(spec=FargateTask)
        before_fargate_task.task_arn = 'ABC'
        before_fargate_task.api_failure_count = 0
        before_fargate_task.get_task_state.return_value = State.RUNNING

        after_fargate_task = FargateTask({
            'taskArn': 'ABC',
            'containers': [{
                'lastStatus': 'STOPPED',
                'exitCode': 0
            }]
        })

        airflow_key = mock.Mock(spec=TaskInstanceKeyType)
        self.executor.active_workers.add_task(before_fargate_task, airflow_key)

        self.executor.ecs.describe_tasks.return_value = {
            'tasks': [after_fargate_task.json],
            'failures': []
        }
        return after_fargate_task



    @mock.patch('airflow.executors.fargate_executor.FargateExecutor.sync')
    @mock.patch('airflow.executors.base_executor.BaseExecutor.trigger_tasks')
    @mock.patch('airflow.stats.Stats.gauge')
    def test_gauge_executor_metrics(self, mock_stats_gauge, mock_trigger_tasks, mock_sync):
        executor = FargateExecutor()
        executor.heartbeat()
        calls = [
            mock.call('executor.open_slots', mock.ANY),
            mock.call('executor.queued_tasks', mock.ANY),
            mock.call('executor.running_tasks', mock.ANY)
        ]
        mock_stats_gauge.assert_has_calls(calls)

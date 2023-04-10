import datetime as dt
import os
from unittest import TestCase, mock

from airflow.providers.amazon.aws.executors.batch_executor import (
    AwsBatchExecutor, BatchJobDetailSchema, BatchJob, BatchJobCollection
)
from airflow.utils.state import State

from .botocore_helper import get_botocore_model, assert_botocore_call


def set_conf():
    os.environ['AIRFLOW__BATCH__REGION'] = 'us-west-1'
    os.environ['AIRFLOW__BATCH__JOB_NAME'] = 'some-job-name'
    os.environ['AIRFLOW__BATCH__JOB_QUEUE'] = 'some-job-queue'
    os.environ['AIRFLOW__BATCH__JOB_DEFINITION'] = 'some-job-def'


def unset_conf():
    for x in os.environ:
        if x.startswith('AIRFLOW__BATCH__'):
            os.environ.pop(x)


class TestBatchCollection(TestCase):
    """Tests EcsTaskCollection Class"""
    def test_get_and_add(self):
        """Test add_task, task_by_arn, cmd_by_key"""
        self.assertEqual(len(self.collection), 2)

    def test_list(self):
        """Test get_all_arns() and get_all_task_keys()"""
        # Check basic list by ARNs & airflow-task-keys
        self.assertListEqual(self.collection.get_all_jobs(), [self.first_job_id, self.second_job_id])

    def test_pop(self):
        """Test pop_by_key()"""
        # pop first task & ensure that it's removed
        self.assertEqual(self.collection.pop_by_id(self.first_job_id), self.first_airflow_key)
        self.assertEqual(len(self.collection), 1)
        self.assertListEqual(self.collection.get_all_jobs(), [self.second_job_id])

    def setUp(self):
        """
        Create a ECS Task Collection and add 2 airflow tasks. Populates self.collection,
        self.first/second_task, self.first/second_airflow_key, and self.first/second_airflow_cmd.
        """
        self.collection = BatchJobCollection()
        # Add first task
        self.first_job_id = '001'
        self.first_airflow_key = mock.Mock(spec=tuple)
        self.collection.add_job(self.first_job_id, self.first_airflow_key)
        # Add second task
        self.second_job_id = '002'
        self.second_airflow_key = mock.Mock(spec=tuple)
        self.collection.add_job(self.second_job_id, self.second_airflow_key)


class TestBatchJob(TestCase):
    """Tests the EcsFargateTask DTO"""
    def test_queued_tasks(self):
        """Tasks that are pending launch identified as 'queued'"""
        for status in self.all_statuses:
            if status not in (self.success, self.failed, self.running):
                job = BatchJob('id', status)
                self.assertNotIn(job.get_job_state(), (State.RUNNING, State.FAILED, State.SUCCESS))

    def test_running_jobs(self):
        """Tasks that have been launched are identified as 'running'"""
        assert self.running in self.all_statuses, 'A core assumption in the Batch Executor has changed. ' \
                                                  'What happened to the list of statuses or the running state?'
        running_job = BatchJob('AAA', self.running)
        self.assertEqual(running_job.get_job_state(), State.RUNNING)

    def test_success_jobs(self):
        """Tasks that have been launched are identified as 'running'"""
        assert self.success in self.all_statuses, 'A core assumption in the Batch Executor has changed. ' \
                                                  'What happened to the list of statuses or the running state?'
        success_job = BatchJob('BBB', self.success)
        self.assertEqual(success_job.get_job_state(), State.SUCCESS)

    def test_failed_jobs(self):
        """Tasks that have been launched are identified as 'running'"""
        assert self.failed in self.all_statuses, 'A core assumption in the Batch Executor has changed. ' \
                                                 'What happened to the list of statuses or the running state?'
        running_job = BatchJob('CCC', self.failed)
        self.assertEqual(running_job.get_job_state(), State.FAILED)

    def setUp(self):
        batch_model = get_botocore_model('batch')
        self.all_statuses = batch_model['shapes']['JobStatus']['enum']
        self.running = 'RUNNING'
        self.success = 'SUCCEEDED'
        self.failed = 'FAILED'

    @classmethod
    def setUpClass(cls) -> None:
        set_conf()

    @classmethod
    def tearDownClass(cls) -> None:
        unset_conf()


class TestAwsBatchExecutor(TestCase):
    """Tests the AWS Batch Executor itself"""

    def test_execute(self):
        """Test execution from end-to-end"""
        airflow_key = mock.Mock(spec=tuple)
        airflow_cmd = mock.Mock(spec=list)
        executor_config = mock.Mock(spec=dict)

        self.executor.batch.submit_job.return_value = {
            'jobId': 'ABC',
            'jobName': 'some-job-name'
        }

        self.executor.execute_async(airflow_key, airflow_cmd)

        # ensure that run_task is called correctly as defined by Botocore docs
        self.executor.batch.submit_job.assert_called_once()
        self.assert_botocore_call('SubmitJob', *self.executor.batch.submit_job.call_args)

        # task is stored in active worker
        self.assertEqual(1, len(self.executor.active_workers))

    @mock.patch('airflow.executors.base_executor.BaseExecutor.fail')
    @mock.patch('airflow.executors.base_executor.BaseExecutor.success')
    def test_sync(self, success_mock, fail_mock):
        """Test synch from end-to-end. Mocks a successful job & makes sure it's removed"""
        after_sync_response = self.__mock_sync()

        # sanity check that container's status code is mocked to success
        loaded_batch_job = BatchJobDetailSchema().load(after_sync_response)
        self.assertEqual(State.SUCCESS, loaded_batch_job.get_job_state())

        self.executor.sync()

        # ensure that run_task is called correctly as defined by Botocore docs
        self.executor.batch.describe_jobs.assert_called_once()
        self.assert_botocore_call('DescribeJobs', *self.executor.batch.describe_jobs.call_args)

        # task is not stored in active workers
        self.assertEqual(0, len(self.executor.active_workers))
        # Task is immediately succeeded
        success_mock.assert_called_once()
        self.assertFalse(fail_mock.called)

    @mock.patch('airflow.executors.base_executor.BaseExecutor.fail')
    @mock.patch('airflow.executors.base_executor.BaseExecutor.success')
    def test_failed_sync(self, success_mock, fail_mock):
        """Test failure states"""
        after_sync_response = self.__mock_sync()

        # set container's status code to failure & sanity-check
        after_sync_response['status'] = 'FAILED'
        self.assertEqual(State.FAILED, BatchJobDetailSchema().load(after_sync_response).get_job_state())
        self.executor.sync()

        # ensure that run_task is called correctly as defined by Botocore docs
        self.executor.batch.describe_jobs.assert_called_once()
        self.assert_botocore_call('DescribeJobs', *self.executor.batch.describe_jobs.call_args)

        # task is not stored in active workers
        self.assertEqual(len(self.executor.active_workers), 0)
        # Task is immediately succeeded
        fail_mock.assert_called_once()
        self.assertFalse(success_mock.called)

    def test_terminate(self):
        """Test that executor can shut everything down; forcing all tasks to unnaturally exit"""
        mocked_job_json = self.__mock_sync()
        mocked_job_json['status'] = 'FAILED'
        self.assertEqual(State.FAILED, BatchJobDetailSchema().load(mocked_job_json).get_job_state())

        self.executor.terminate()

        self.assertTrue(self.executor.batch.terminate_job.called)
        self.assert_botocore_call('TerminateJob', *self.executor.batch.terminate_job.call_args)

    def test_end(self):
        """The end() function should call sync 3 times, and the task should fail on the 3rd call"""
        sync_call_count = 0
        sync_func = self.executor.sync

        def sync_mock():
            """This is to count the number of times sync is called. On the 3rd time, mock the job to fail"""
            nonlocal sync_call_count
            if sync_call_count >= 2:
                mocked_job_json['status'] = 'FAILED'
            sync_func()
            sync_call_count += 1

        self.executor.sync = sync_mock
        mocked_job_json = self.__mock_sync()
        mocked_job_json['status'] = 'RUNNING'
        self.executor.end(heartbeat_interval=0)
        self.assertEqual(3, sync_call_count)
        self.executor.sync = sync_func

    def assert_botocore_call(self, method_name, args, kwargs):
        assert_botocore_call(self.batch_model, method_name, args, kwargs)

    def setUp(self) -> None:
        """Creates Botocore Loader (used for asserting botocore calls) and a mocked ecs client"""
        self.batch_model = get_botocore_model('batch')
        self.__set_mocked_executor()

    @classmethod
    def setUpClass(cls) -> None:
        set_conf()

    @classmethod
    def tearDownClass(cls) -> None:
        unset_conf()

    def __set_mocked_executor(self):
        """Mock ECS such that there's nothing wrong with anything"""
        from airflow.configuration import conf
        executor = AwsBatchExecutor()
        executor.start()

        # replace boto3 ecs client with mock
        batch_mock = mock.Mock(spec=executor.batch)
        submit_job_ret_val = {
            'jobName': conf.get('batch', 'job_name'),
            'jobId': 'ABC'
        }
        batch_mock.submit_job.return_value = submit_job_ret_val
        executor.batch = batch_mock

        self.executor = executor

    def __mock_sync(self):
        """Mock ECS such that there's nothing wrong with anything"""
        # create running fargate instance
        before_batch_job = mock.Mock(spec=BatchJob)
        before_batch_job.job_id = 'ABC'
        before_batch_job.get_job_state.return_value = State.RUNNING

        airflow_key = mock.Mock(spec=tuple)
        self.executor.active_workers.add_job('ABC', airflow_key)

        after_batch_job = {
            'jobName': 'some-job-queue',
            'jobId': 'ABC',
            'jobQueue': 'some-job-queue',
            'status': 'SUCCEEDED',
            'createdAt': dt.datetime.now().timestamp(),
            'jobDefinition': 'some-job-def',
        }
        self.executor.batch.describe_jobs.return_value = {
            'jobs': [after_batch_job]
        }
        return after_batch_job

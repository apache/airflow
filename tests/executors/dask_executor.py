# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import logging
import time
import unittest

from airflow import configuration
from airflow.models import DAG, DagBag, TaskInstance, State
from airflow.jobs import BackfillJob
from airflow.operators.python_operator import PythonOperator

try:
    from airflow.executors import DaskExecutor
    from distributed import LocalCluster
    SKIP_DASK = False
except ImportError:
    logging.error('Dask unavailable, skipping DaskExecutor tests')
    SKIP_DASK = True

if 'sqlite' in configuration.get('core', 'sql_alchemy_conn'):
    logging.error('sqlite does not support concurrent access')
    SKIP_DASK = True

DEFAULT_DATE = datetime.datetime(2017, 1, 1)


class DaskExecutorTest(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag(include_examples=True)

    @unittest.skipIf(SKIP_DASK, 'Dask unsupported by this configuration')
    def test_dask_executor_functions(self):
        cluster = LocalCluster(nanny=False)

        executor = DaskExecutor(cluster_address=cluster.scheduler_address)

        success_command = 'echo 1'
        fail_command = 'exit 1'

        executor.execute_async(key='success', command=success_command)
        executor.execute_async(key='fail', command=fail_command)

        success_future = next(
            k for k, v in executor.futures.items() if v == 'success')
        fail_future = next(
            k for k, v in executor.futures.items() if v == 'fail')

        # wait for the futures to execute, with a timeout
        timeout = datetime.datetime.now() + datetime.timedelta(seconds=0.5)
        while not (success_future.done() and fail_future.done()):
            if datetime.datetime.now() > timeout:
                raise ValueError(
                    'The futures should have finished; there is probably '
                    'an error communciating with the Dask cluster.')

        # both tasks should have finished
        self.assertTrue(success_future.done())
        self.assertTrue(fail_future.done())

        # check task exceptions
        self.assertTrue(success_future.exception() is None)
        self.assertTrue(fail_future.exception() is not None)

        # tell the executor to shut down
        executor.end()
        self.assertTrue(len(executor.futures) == 0)

        cluster.close()

    @unittest.skipIf(SKIP_DASK, 'Dask unsupported by this configuration')
    def test_submit_task_instance_to_dask_cluster(self):
        """
        Test that the DaskExecutor properly submits tasks to the cluster
        """
        cluster = LocalCluster(nanny=False)

        executor = DaskExecutor(cluster_address=cluster.scheduler_address)

        args = dict(
            start_date=DEFAULT_DATE
        )

        def fail():
            raise ValueError('Intentional failure.')

        with DAG('test-dag', default_args=args) as dag:
            # queue should be allowed, but ignored
            success_operator = PythonOperator(
                task_id='success',
                python_callable=lambda: True,
                queue='queue')

            fail_operator = PythonOperator(
                task_id='fail',
                python_callable=fail)

        success_ti = TaskInstance(
            success_operator,
            execution_date=DEFAULT_DATE)

        fail_ti = TaskInstance(
            fail_operator,
            execution_date=DEFAULT_DATE)

        # queue the tasks
        executor.queue_task_instance(success_ti)
        executor.queue_task_instance(fail_ti)

        # the tasks haven't been submitted to the cluster yet
        self.assertTrue(len(executor.futures) == 0)
        # after the heartbeat, they have been submitted
        executor.heartbeat()
        self.assertTrue(len(executor.futures) == 2)

        # wait a reasonable amount of time for the tasks to complete
        for _ in range(2):
            time.sleep(0.25)
            executor.heartbeat()

        # check that the futures were completed
        if len(executor.futures) == 2:
            raise ValueError('Failed to reach cluster before timeout.')
        self.assertTrue(len(executor.futures) == 0)

        # check that the taskinstances were updated
        success_ti.refresh_from_db()
        self.assertTrue(success_ti.state == State.SUCCESS)
        fail_ti.refresh_from_db()
        self.assertTrue(fail_ti.state == State.FAILED)

        cluster.close()


    @unittest.skipIf(SKIP_DASK, 'Dask unsupported by this configuration')
    def test_backfill_integration(self):
        """
        Test that DaskExecutor can be used to backfill example dags
        """
        cluster = LocalCluster(nanny=False)

        dags = [
            dag for dag in self.dagbag.dags.values()
            if dag.dag_id in [
                'example_bash_operator',
                # 'example_python_operator',
            ]
        ]

        for dag in dags:
            dag.clear(
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE)

        for i, dag in enumerate(sorted(dags, key=lambda d: d.dag_id)):
            job = BackfillJob(
                dag=dag,
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE,
                ignore_first_depends_on_past=True,
                executor=DaskExecutor(
                    cluster_address=cluster.scheduler_address))
            job.run()

        cluster.close()

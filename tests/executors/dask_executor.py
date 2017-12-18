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

import unittest

from airflow import configuration
from airflow.models import DagBag
from airflow.jobs import BackfillJob
from airflow.utils import timezone

from datetime import timedelta

try:
    from airflow.executors.dask_executor import DaskExecutor
    from distributed import LocalCluster
    SKIP_DASK = False
except ImportError:
    SKIP_DASK = True

if 'sqlite' in configuration.get('core', 'sql_alchemy_conn'):
    SKIP_DASK = True

# Always skip due to issues on python 3 issues
SKIP_DASK = True

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class DaskExecutorTest(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag(include_examples=True)
        self.cluster = LocalCluster()

    @unittest.skipIf(SKIP_DASK, 'Dask unsupported by this configuration')
    def test_dask_executor_functions(self):
        executor = DaskExecutor(cluster_address=self.cluster.scheduler_address)

        # start the executor
        executor.start()

        success_command = 'echo 1'
        fail_command = 'exit 1'

        executor.execute_async(key='success', command=success_command)
        executor.execute_async(key='fail', command=fail_command)

        success_future = next(
            k for k, v in executor.futures.items() if v == 'success')
        fail_future = next(
            k for k, v in executor.futures.items() if v == 'fail')

        # wait for the futures to execute, with a timeout
        timeout = timezone.utcnow() + timedelta(seconds=30)
        while not (success_future.done() and fail_future.done()):
            if timezone.utcnow() > timeout:
                raise ValueError(
                    'The futures should have finished; there is probably '
                    'an error communciating with the Dask cluster.')

        # both tasks should have finished
        self.assertTrue(success_future.done())
        self.assertTrue(fail_future.done())

        # check task exceptions
        self.assertTrue(success_future.exception() is None)
        self.assertTrue(fail_future.exception() is not None)

    @unittest.skipIf(SKIP_DASK, 'Dask unsupported by this configuration')
    def test_backfill_integration(self):
        """
        Test that DaskExecutor can be used to backfill example dags
        """
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
                    cluster_address=self.cluster.scheduler_address))
            job.run()

    def tearDown(self):
        self.cluster.close(timeout=5)

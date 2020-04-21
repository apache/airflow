#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import logging
import sys

import pandas as pd

from airflow import settings
from airflow.configuration import conf
from airflow.jobs.scheduler_job import SchedulerJob
from airflow.models import DagBag, DagModel, DagRun, TaskInstance
from airflow.utils import timezone
from airflow.utils.state import State

SUBDIR = 'scripts/perf/dags'
DAG_IDS = ['perf_dag_1', 'perf_dag_2']
MAX_RUNTIME_SECS = 6


class SchedulerMetricsJob(SchedulerJob):
    """
    This class extends SchedulerJob to instrument the execution performance of
    task instances contained in each DAG. We want to know if any DAG
    is starved of resources, and this will be reflected in the stats printed
    out at the end of the test run. The following metrics will be instrumented
    for each task instance (dag_id, task_id, execution_date) tuple:

    1. Queuing delay - time taken from starting the executor to the task
       instance to be added to the executor queue.
    2. Start delay - time taken from starting the executor to the task instance
       to start execution.
    3. Land time - time taken from starting the executor to task instance
       completion.
    4. Duration - time taken for executing the task instance.

    The DAGs implement bash operators that call the system wait command. This
    is representative of typical operators run on Airflow - queries that are
    run on remote systems and spend the majority of their time on I/O wait.

    To Run:
        $ python scripts/perf/scheduler_ops_metrics.py [timeout]

    You can specify timeout in seconds as an optional parameter.
    Its default value is 6 seconds.
    """
    __mapper_args__ = {
        'polymorphic_identity': 'SchedulerMetricsJob'
    }

    def __init__(self, dag_ids, subdir, max_runtime_secs):
        self.max_runtime_secs = max_runtime_secs
        super().__init__(dag_ids=dag_ids, subdir=subdir)

    def print_stats(self):
        """
        Print operational metrics for the scheduler test.
        """
        session = settings.Session()
        TI = TaskInstance
        tis = (
            session
            .query(TI)
            .filter(TI.dag_id.in_(DAG_IDS))
            .all()
        )
        successful_tis = [x for x in tis if x.state == State.SUCCESS]
        ti_perf = [(ti.dag_id, ti.task_id, ti.execution_date,
                    (ti.queued_dttm - self.start_date).total_seconds(),
                    (ti.start_date - self.start_date).total_seconds(),
                    (ti.end_date - self.start_date).total_seconds(),
                    ti.duration) for ti in successful_tis]
        ti_perf_df = pd.DataFrame(ti_perf, columns=['dag_id', 'task_id',
                                                    'execution_date',
                                                    'queue_delay',
                                                    'start_delay', 'land_time',
                                                    'duration'])

        print('Performance Results')
        print('###################')
        for dag_id in DAG_IDS:
            print('DAG {}'.format(dag_id))
            print(ti_perf_df[ti_perf_df['dag_id'] == dag_id])
        print('###################')
        if len(tis) > len(successful_tis):
            print("WARNING!! The following task instances haven't completed")
            print(pd.DataFrame([(ti.dag_id, ti.task_id, ti.execution_date, ti.state)
                  for ti in filter(lambda x: x.state != State.SUCCESS, tis)],
                  columns=['dag_id', 'task_id', 'execution_date', 'state']))

        session.commit()

    def heartbeat(self):
        """
        Override the scheduler heartbeat to determine when the test is complete
        """
        super().heartbeat()
        session = settings.Session()
        # Get all the relevant task instances
        TI = TaskInstance
        successful_tis = (
            session
            .query(TI)
            .filter(TI.dag_id.in_(DAG_IDS))
            .filter(TI.state.in_([State.SUCCESS]))
            .all()
        )
        session.commit()

        dagbag = DagBag(SUBDIR)
        dags = [dagbag.dags[dag_id] for dag_id in DAG_IDS]
        # the tasks in perf_dag_1 and per_dag_2 have a daily schedule interval.
        num_task_instances = sum([(timezone.utcnow() - task.start_date).days
                                 for dag in dags for task in dag.tasks])

        if (len(successful_tis) == num_task_instances or
                (timezone.utcnow() - self.start_date).total_seconds() >
                self.max_runtime_secs):
            if len(successful_tis) == num_task_instances:
                self.log.info("All tasks processed! Printing stats.")
            else:
                self.log.info("Test timeout reached. Printing available stats.")
            self.print_stats()
            set_dags_paused_state(True)
            sys.exit()


def clear_dag_runs():
    """
    Remove any existing DAG runs for the perf test DAGs.
    """
    session = settings.Session()
    drs = session.query(DagRun).filter(
        DagRun.dag_id.in_(DAG_IDS),
    ).all()
    for dr in drs:
        logging.info('Deleting DagRun :: %s', dr)
        session.delete(dr)


def clear_dag_task_instances():
    """
    Remove any existing task instances for the perf test DAGs.
    """
    session = settings.Session()
    TI = TaskInstance
    tis = (
        session
        .query(TI)
        .filter(TI.dag_id.in_(DAG_IDS))
        .all()
    )
    for ti in tis:
        logging.info('Deleting TaskInstance :: %s', ti)
        session.delete(ti)
    session.commit()


def set_dags_paused_state(is_paused):
    """
    Toggle the pause state of the DAGs in the test.
    """
    session = settings.Session()
    dag_models = session.query(DagModel).filter(
        DagModel.dag_id.in_(DAG_IDS))
    for dag_model in dag_models:
        logging.info('Setting DAG :: %s is_paused=%s', dag_model, is_paused)
        dag_model.is_paused = is_paused
    session.commit()


def main():
    '''
    Run the scheduler metrics jobs after loading the test configuration and
    clearing old instances of dags and tasks
    '''
    max_runtime_secs = MAX_RUNTIME_SECS
    if len(sys.argv) > 1:
        try:
            max_runtime_secs = int(sys.argv[1])
            if max_runtime_secs < 1:
                raise ValueError
        except ValueError:
            logging.error('Specify a positive integer for timeout.')
            sys.exit(1)

    conf.load_test_config()

    set_dags_paused_state(False)
    clear_dag_runs()
    clear_dag_task_instances()

    job = SchedulerMetricsJob(
        dag_ids=DAG_IDS,
        subdir=SUBDIR,
        max_runtime_secs=max_runtime_secs)

    job.run()


if __name__ == "__main__":
    main()

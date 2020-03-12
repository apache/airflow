#!/usr/bin/env python3
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

import gc
import os
import statistics
import time

import click

MAX_DAG_RUNS_ALLOWED = 1


class ShortCircutExecutorMixin:
    def __init__(self, stop_when_these_completed):
        super().__init__()
        self.reset(stop_when_these_completed)

    def reset(self, stop_when_these_completed):
        self.stop_when_these_completed = {
            # We want to store the dag run here, but we don't have them until they are created.
            key: None for key in stop_when_these_completed
        }

    def change_state(self, key, state):
        from airflow.utils.state import State
        super().change_state(key, state)

        dag_id, task_id, execution_date, __ = key
        run_key = (dag_id, execution_date.timestamp())
        if run_key in self.stop_when_these_completed:

            if self.stop_when_these_completed[run_key] is None:
                import airflow.models

                # We are interested in this run, but don't yet have the record for it.
                run = airflow.models.DagRun.find(dag_id=dag_id, execution_date=execution_date)[0]
                self.stop_when_these_completed[run_key] = run
            else:
                run = self.stop_when_these_completed[run_key]

            if run and all(t.state == State.SUCCESS for t in run.get_task_instances()):
                self.stop_when_these_completed.pop(run_key)

                if not self.stop_when_these_completed:
                    self.log.warning("STOPPING SCHEDULER -- all runs complete")
                    self.scheduler_job.processor_agent._done = True
                else:
                    self.log.warning("WAITING ON %d RUNS", len(self.stop_when_these_completed))


def get_executor_under_test():
    try:
        # Run against master and 1.10.x releases
        from tests.test_utils.mock_executor import MockExecutor
    except ImportError:
        from tests.executors.test_executor import TestExecutor as MockExecutor

    # from airflow.executors.local_executor import LocalExecutor

    # Change this to try other executors
    Executor = MockExecutor

    class ShortCircutExecutor(ShortCircutExecutorMixin, Executor):
        pass

    return ShortCircutExecutor


def reset_dag(dag, session):
    import airflow.models

    DR = airflow.models.DagRun
    DM = airflow.models.DagModel
    TI = airflow.models.TaskInstance
    TF = airflow.models.TaskFail
    dag_id = dag.dag_id

    session.query(DM).filter(DM.dag_id == dag_id).update({'is_paused': False})
    session.query(DR).filter(DR.dag_id == dag_id).delete()
    session.query(TI).filter(TI.dag_id == dag_id).delete()
    session.query(TF).filter(TF.dag_id == dag_id).delete()


def pause_all_dags(session):
    from airflow.models.dag import DagModel
    session.query(DagModel).update({'is_paused': True})


def create_dag_runs(dag, num_runs, session):
    from airflow.models.dagrun import DagRun
    from airflow.utils import timezone
    from airflow.utils.state import State

    next_run_date = dag.normalize_schedule(dag.start_date or min(t.start_date for t in dag.tasks))

    for _ in range(num_runs):
        dag.create_dagrun(
            run_id=DagRun.ID_PREFIX + next_run_date.isoformat(),
            execution_date=next_run_date,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
            external_trigger=False,
            session=session,
        )
        next_run_date = dag.following_schedule(next_run_date)


@click.command()
@click.option('--num-runs', default=1, help='number of DagRun, to run for each DAG')
@click.option('--repeat', default=3, help='number of times to run test, to reduce variance')
@click.option('--pre-create-dag-runs', is_flag=True, default=False,
              help='''Pre-create the dag runs and stop the scheduler creating more.

        Warning: this makes the scheduler do (slightly) less work so may skew your numbers. Use sparingly!
        ''')
@click.argument('dag_ids', required=True, nargs=-1)
def main(num_runs, repeat, pre_create_dag_runs, dag_ids):
    """
    This script can be used to measure the total "scheduler overhead" of Airflow.

    By overhead we mean if the tasks executed instantly as soon as they are
    executed (i.e. they do nothing) how quickly could we schedule them.

    It will monitor the task completion of the Mock/stub executor (no actual
    tasks are run) and after the required number of dag runs for all the
    specified dags have completed all their tasks, it will cleanly shut down
    the scheduler.

    The dags you run with need to have an early enough start_date to create the
    desired number of runs.

    Care should be taken that other limits (DAG concurrency, pool size etc) are
    not the bottleneck. This script doesn't help you in that regard.

    It is recommended to repeat the test at least 3 times (`--repeat=3`, the
    default) so that you can get somewhat-accurate variance on the reported
    timing numbers, but this can be disabled for longer runs if needed.
    """

    # Turn on unit test mode so that we don't do any sleep() in the scheduler
    # loop - not needed on master, but this script can run against older
    # releases too!
    os.environ['AIRFLOW__CORE_UNIT_TEST_MODE'] = 'True'

    os.environ['AIRFLOW__CORE__DAG_CONCURRENCY'] = '500'

    # Set this so that dags can dynamically configure their end_date
    os.environ['AIRFLOW_BENCHMARK_MAX_DAG_RUNS'] = str(num_runs)

    if pre_create_dag_runs:
        os.environ['AIRFLOW__SCHEDULER__USE_JOB_SCHEDULER'] = 'False'

    from airflow.jobs.scheduler_job import SchedulerJob
    from airflow.models.dagbag import DagBag
    from airflow.utils import db

    dagbag = DagBag()

    dags = []
    dag_run_keys = []

    with db.create_session() as session:
        pause_all_dags(session)
        for dag_id in dag_ids:
            dag = dagbag.get_dag(dag_id)
            dag.sync_to_db(session=session)
            dags.append(dag)
            reset_dag(dag, session)

            next_run_date = dag.normalize_schedule(dag.start_date or min(t.start_date for t in dag.tasks))

            # Compute the execution_dates we want to wait on. We only "need" the last
            # one, but having them all means we get a more useful "count down" in the
            # log
            dag_run_keys.append((dag.dag_id, next_run_date.timestamp()))

            for _ in range(num_runs - 1):
                next_run_date = dag.following_schedule(next_run_date)
                dag_run_keys.append((dag.dag_id, next_run_date.timestamp()))

            end_date = dag.end_date or dag.default_args.get('end_date')
            if end_date != next_run_date:
                exit(f"DAG {dag_id} has incorrect end_date ({end_date}) for number of runs! It should be"
                     f" {next_run_date}")

            if pre_create_dag_runs:
                create_dag_runs(dag, num_runs, session)

    ShortCircutExecutor = get_executor_under_test()

    executor = ShortCircutExecutor(stop_when_these_completed=dag_run_keys)
    scheduler_job = SchedulerJob(dag_ids=dag_ids, do_pickle=False, executor=executor)
    executor.scheduler_job = scheduler_job

    total_tasks = sum(len(dag.tasks) for dag in dags)

    if 'PYSPY' in os.environ:
        pid = str(os.getpid())
        filename = os.environ.get('PYSPY_O', 'flame-' + pid + '.html')
        os.spawnlp(os.P_NOWAIT, 'sudo', 'sudo', 'py-spy', 'record', '-o', filename, '-p', pid, '--idle')

    times = []

    # Need a lambda to refer to the _latest_ value fo scheduler_job, not just
    # the initial one
    code_to_test = lambda: scheduler_job.run()

    for n in range(repeat):
        gc.disable()
        start = time.perf_counter()

        code_to_test()
        times.append(time.perf_counter() - start)
        gc.enable()
        print("Run %d time: %.5f" % (n + 1, times[-1]))

        if n + 1 != repeat:
            with db.create_session() as session:
                for dag in dags:
                    reset_dag(dag, session)

            executor.reset(dag_run_keys)
            scheduler_job = SchedulerJob(dag_ids=dag_ids, do_pickle=False, executor=executor)
            executor.scheduler_job = scheduler_job

    print()
    print()
    msg = "Time for %d dag runs of %d dags with %d total tasks: %.4fs"

    if len(times) > 1:
        print((msg + " (±%.3fs)") % (
            num_runs,
            len(dags),
            total_tasks,
            statistics.mean(times),
            statistics.stdev(times)
        ))
    else:
        print(msg % (num_runs, len(dags), total_tasks, times[0]))

    print()
    print()


if __name__ == "__main__":
    main()

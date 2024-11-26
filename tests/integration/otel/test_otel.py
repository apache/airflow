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
from __future__ import annotations

import logging
import os
import signal
import subprocess
import time

import pendulum
import pytest

from airflow.executors import executor_loader
from airflow.executors.executor_utils import ExecutorName
from airflow.models import DAG, DagBag, DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance
from airflow.traces.otel_tracer import CTX_PROP_SUFFIX
from airflow.utils.session import create_session
from airflow.utils.span_status import SpanStatus
from airflow.utils.state import State

from tests.integration.otel.test_utils import (
    assert_parent_children_spans,
    assert_parent_children_spans_for_non_root,
    assert_span_name_belongs_to_root_span,
    assert_span_not_in_children_spans,
    dump_airflow_metadata_db,
    extract_spans_from_output,
    get_parent_child_dict,
)

log = logging.getLogger("test_otel.TestOtelIntegration")


def unpause_trigger_dag_and_get_run_id(dag_id: str) -> str:
    unpause_command = ["airflow", "dags", "unpause", dag_id]

    # Unpause the dag using the cli.
    subprocess.run(unpause_command, check=True, env=os.environ.copy())

    execution_date = pendulum.now("UTC")
    run_id = f"manual__{execution_date.isoformat()}"

    trigger_command = [
        "airflow",
        "dags",
        "trigger",
        dag_id,
        "--run-id",
        run_id,
        "--exec-date",
        execution_date.isoformat(),
    ]

    # Trigger the dag using the cli.
    subprocess.run(trigger_command, check=True, env=os.environ.copy())

    return run_id


def wait_for_dag_run_and_check_span_status(dag_id: str, run_id: str, max_wait_time: int, span_status: str):
    # max_wait_time, is the timeout for the DAG run to complete. The value is in seconds.
    start_time = time.time()

    while time.time() - start_time < max_wait_time:
        with create_session() as session:
            dag_run = (
                session.query(DagRun)
                .filter(
                    DagRun.dag_id == dag_id,
                    DagRun.run_id == run_id,
                )
                .first()
            )

            if dag_run is None:
                time.sleep(5)
                continue

            dag_run_state = dag_run.state
            log.info("DAG Run state: %s.", dag_run_state)

            dag_run_span_status = dag_run.span_status
            log.info("DAG Run span status: %s.", dag_run_span_status)

            if dag_run_state in [State.SUCCESS, State.FAILED]:
                break

    assert (
        dag_run_state == State.SUCCESS
    ), f"Dag run did not complete successfully. Final state: {dag_run_state}."

    assert (
        dag_run_span_status == span_status
    ), f"Dag run span status isn't {span_status} as expected.Actual status: {dag_run_span_status}."


def check_spans_with_continuance(output: str, dag: DAG):
    # Get a list of lines from the captured output.
    output_lines = output.splitlines()

    # Filter the output, create a json obj for each span and then store them into dictionaries.
    # One dictionary with only the root spans, and one with all the captured spans (root and otherwise).
    root_span_dict, span_dict = extract_spans_from_output(output_lines)
    # Generate a dictionary with parent child relationships.
    # This is done by comparing the span_id of each root span with the parent_id of each non-root span.
    parent_child_dict = get_parent_child_dict(root_span_dict, span_dict)

    # The span hierarchy for dag 'otel_test_dag_with_pause' is
    # dag span
    #   |_ task_1 span
    #   |_ scheduler_exited span
    #   |_ new_scheduler span
    #   |_ dag span (continued)
    #       |_ task_1 span (continued)
    #           |_ sub_span_1
    #               |_ sub_span_2
    #                   |_ sub_span_3
    #           |_ sub_span_4
    #       |_ task_2 span

    dag_id = dag.dag_id

    task_instance_ids = dag.task_ids
    task1_id = task_instance_ids[0]
    task2_id = task_instance_ids[1]

    dag_root_span_name = f"{dag_id}{CTX_PROP_SUFFIX}"

    dag_root_span_children_names = [
        f"{task1_id}{CTX_PROP_SUFFIX}",
        "current_scheduler_exited",
        "new_scheduler",
        f"{dag_id}_continued{CTX_PROP_SUFFIX}",
    ]

    dag_continued_span_children_names = [
        f"{task1_id}_continued{CTX_PROP_SUFFIX}",
        f"{task2_id}{CTX_PROP_SUFFIX}",
    ]

    task1_span_children_names = [
        f"{task1_id}_sub_span1{CTX_PROP_SUFFIX}",
        f"{task1_id}_sub_span4{CTX_PROP_SUFFIX}",
    ]

    # Single element lists.
    task1_sub_span1_children_span_names = [f"{task1_id}_sub_span2{CTX_PROP_SUFFIX}"]
    task1_sub_span2_children_span_names = [f"{task1_id}_sub_span3{CTX_PROP_SUFFIX}"]

    assert_span_name_belongs_to_root_span(
        root_span_dict=root_span_dict, span_name=dag_root_span_name, should_succeed=True
    )

    # Check direct children of the root span.
    assert_parent_children_spans(
        parent_child_dict=parent_child_dict,
        root_span_dict=root_span_dict,
        parent_name=dag_root_span_name,
        children_names=dag_root_span_children_names,
    )

    # Use a span name that exists, but it's not a direct child.
    assert_span_not_in_children_spans(
        parent_child_dict=parent_child_dict,
        root_span_dict=root_span_dict,
        span_dict=span_dict,
        parent_name=dag_root_span_name,
        child_name=f"{task1_id}_continued{CTX_PROP_SUFFIX}",
        span_exists=True,
    )

    # Use a span name that doesn't exist at all.
    assert_span_not_in_children_spans(
        parent_child_dict=parent_child_dict,
        root_span_dict=root_span_dict,
        span_dict=span_dict,
        parent_name=dag_root_span_name,
        child_name=f"{task1_id}_non_existent",
        span_exists=False,
    )

    # Check children of the continued dag span.
    assert_parent_children_spans_for_non_root(
        span_dict=span_dict,
        parent_name=f"{dag_id}_continued{CTX_PROP_SUFFIX}",
        children_names=dag_continued_span_children_names,
    )

    # Check children of the continued task1 span.
    assert_parent_children_spans_for_non_root(
        span_dict=span_dict,
        parent_name=f"{task1_id}_continued{CTX_PROP_SUFFIX}",
        children_names=task1_span_children_names,
    )

    # Check children of task1 sub span1.
    assert_parent_children_spans_for_non_root(
        span_dict=span_dict,
        parent_name=f"{task1_id}_sub_span1{CTX_PROP_SUFFIX}",
        children_names=task1_sub_span1_children_span_names,
    )

    # Check children of task1 sub span2.
    assert_parent_children_spans_for_non_root(
        span_dict=span_dict,
        parent_name=f"{task1_id}_sub_span2{CTX_PROP_SUFFIX}",
        children_names=task1_sub_span2_children_span_names,
    )


def check_spans_without_continuance(output: str, dag: DAG):
    # Get a list of lines from the captured output.
    output_lines = output.splitlines()

    # Filter the output, create a json obj for each span and then store them into dictionaries.
    # One dictionary with only the root spans, and one with all the captured spans (root and otherwise).
    root_span_dict, span_dict = extract_spans_from_output(output_lines)
    # Generate a dictionary with parent child relationships.
    # This is done by comparing the span_id of each root span with the parent_id of each non-root span.
    parent_child_dict = get_parent_child_dict(root_span_dict, span_dict)

    # Any spans generated under a task, are children of the task span.
    # The span hierarchy for dag 'otel_test_dag' is
    # dag span
    #   |_ task_1 span
    #       |_ sub_span_1
    #           |_ sub_span_2
    #               |_ sub_span_3
    #       |_ sub_span_4
    #   |_ task_2 span

    dag_id = dag.dag_id

    task_instance_ids = dag.task_ids
    task1_id = task_instance_ids[0]
    task2_id = task_instance_ids[1]

    dag_root_span_name = f"{dag_id}{CTX_PROP_SUFFIX}"

    dag_root_span_children_names = [
        f"{task1_id}{CTX_PROP_SUFFIX}",
        f"{task2_id}{CTX_PROP_SUFFIX}",
    ]

    task1_span_children_names = [
        f"{task1_id}_sub_span1{CTX_PROP_SUFFIX}",
        f"{task1_id}_sub_span4{CTX_PROP_SUFFIX}",
    ]

    # Single element lists.
    task1_sub_span1_children_span_names = [f"{task1_id}_sub_span2{CTX_PROP_SUFFIX}"]
    task1_sub_span2_children_span_names = [f"{task1_id}_sub_span3{CTX_PROP_SUFFIX}"]

    assert_span_name_belongs_to_root_span(
        root_span_dict=root_span_dict, span_name=dag_root_span_name, should_succeed=True
    )

    # Check direct children of the root span.
    assert_parent_children_spans(
        parent_child_dict=parent_child_dict,
        root_span_dict=root_span_dict,
        parent_name=dag_root_span_name,
        children_names=dag_root_span_children_names,
    )

    # Use a span name that exists, but it's not a direct child.
    assert_span_not_in_children_spans(
        parent_child_dict=parent_child_dict,
        root_span_dict=root_span_dict,
        span_dict=span_dict,
        parent_name=dag_root_span_name,
        child_name=f"{task1_id}_sub_span1{CTX_PROP_SUFFIX}",
        span_exists=True,
    )

    # Use a span name that doesn't exist at all.
    assert_span_not_in_children_spans(
        parent_child_dict=parent_child_dict,
        root_span_dict=root_span_dict,
        span_dict=span_dict,
        parent_name=dag_root_span_name,
        child_name=f"{task1_id}_non_existent",
        span_exists=False,
    )

    # Check children of the task1 span.
    assert_parent_children_spans_for_non_root(
        span_dict=span_dict,
        parent_name=f"{task1_id}{CTX_PROP_SUFFIX}",
        children_names=task1_span_children_names,
    )

    # Check children of task1 sub span1.
    assert_parent_children_spans_for_non_root(
        span_dict=span_dict,
        parent_name=f"{task1_id}_sub_span1{CTX_PROP_SUFFIX}",
        children_names=task1_sub_span1_children_span_names,
    )

    # Check children of task1 sub span2.
    assert_parent_children_spans_for_non_root(
        span_dict=span_dict,
        parent_name=f"{task1_id}_sub_span2{CTX_PROP_SUFFIX}",
        children_names=task1_sub_span2_children_span_names,
    )


@pytest.mark.integration("redis")
@pytest.mark.backend("postgres")
class TestOtelIntegration:
    """
    This test is using a ConsoleSpanExporter so that it can capture
    the spans from the stdout and run assertions on them.

    It can also be used with otel and jaeger for manual testing.
    To export the spans to otel and visualize them with jaeger,
    - start breeze with '--integration otel'
    - run on the shell 'export use_otel=true'
    - run the test
    - check 'http://localhost:36686/'

    To get span dumps on the stdout, run 'export log_level=debug'.
    """

    test_dir = os.path.dirname(os.path.abspath(__file__))
    dag_folder = os.path.join(test_dir, "dags")

    use_otel = os.getenv("use_otel", default="false")
    log_level = os.getenv("log_level", default="none")

    celery_command_args = [
        "celery",
        "--app",
        "airflow.providers.celery.executors.celery_executor.app",
        "worker",
        "--concurrency",
        "1",
        "--loglevel",
        "INFO",
    ]

    scheduler_command_args = [
        "airflow",
        "scheduler",
    ]

    @classmethod
    def setup_class(cls):
        os.environ["AIRFLOW__TRACES__OTEL_ON"] = "True"
        os.environ["AIRFLOW__TRACES__OTEL_HOST"] = "breeze-otel-collector"
        os.environ["AIRFLOW__TRACES__OTEL_PORT"] = "4318"
        if cls.use_otel != "true":
            os.environ["AIRFLOW__TRACES__OTEL_DEBUGGING_ON"] = "True"

        os.environ["AIRFLOW__TRACES__OTEL_USE_CONTEXT_PROPAGATION"] = "True"

        os.environ["AIRFLOW__SCHEDULER__STANDALONE_DAG_PROCESSOR"] = "False"
        os.environ["AIRFLOW__SCHEDULER__PROCESSOR_POLL_INTERVAL"] = "2"

        os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = f"{cls.dag_folder}"

        os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
        os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "False"

        if cls.log_level == "debug":
            log.setLevel(logging.DEBUG)

    @pytest.fixture
    def celery_worker_env_vars(self, monkeypatch):
        os.environ["AIRFLOW__CORE__EXECUTOR"] = "CeleryExecutor"
        executor_name = ExecutorName(
            module_path="airflow.providers.celery.executors.celery_executor.CeleryExecutor",
            alias="CeleryExecutor",
        )
        monkeypatch.setattr(executor_loader, "_alias_to_executors", {"CeleryExecutor": executor_name})

    def test_same_scheduler_processing_the_entire_dag(
        self, monkeypatch, celery_worker_env_vars, capfd, session
    ):
        """
        Test that a DAG runs successfully and exports the correct spans.
        Integration with a scheduler, a celery worker, a postgres db and a redis broker.
        """
        celery_worker_process = None
        scheduler_process = None
        try:
            # Start the processes here and not as fixtures or in a common setup,
            # so that the test can capture their output.
            celery_worker_process, scheduler_process = self.start_worker_and_scheduler1()

            dag_id = "otel_test_dag"
            dag = self.serialize_and_get_dag(dag_id=dag_id)

            run_id = unpause_trigger_dag_and_get_run_id(dag_id=dag_id)

            wait_for_dag_run_and_check_span_status(
                dag_id=dag_id, run_id=run_id, max_wait_time=90, span_status=SpanStatus.ENDED
            )

            if logging.root.level == logging.DEBUG:
                with create_session() as session:
                    dump_airflow_metadata_db(session)
        finally:
            # Terminate the processes.
            celery_worker_process.terminate()
            celery_worker_process.wait()

            scheduler_process.terminate()
            scheduler_process.wait()

        out, err = capfd.readouterr()
        log.debug("out-start --\n%s\n-- out-end", out)
        log.debug("err-start --\n%s\n-- err-end", err)

        if self.use_otel != "true":
            # Dag run should have succeeded. Test the spans from the output.
            check_spans_without_continuance(output=out, dag=dag)

    def test_another_scheduler_executing_the_second_task_and_the_rest_of_the_dag(
        self, monkeypatch, celery_worker_env_vars, capfd, session
    ):
        """
        Similar to test_dag_spans_with_context_propagation but this test will start with one scheduler,
        and during the dag execution, it will stop the process and start a new one.

        A txt file will be used for signaling the test and the dag in order to make sure that
        the 1st scheduler is stopped while the first task is executing and that
        the 2nd scheduler picks up the task and dag processing.
        The steps will be
        - The dag starts running, creates the file with a signal word and waits until the word is changed.
        - The test checks if the file exist, stops the scheduler, starts a new scheduler and updates the file.
        - The dag gets the update and continues until the task is finished.
        At this point, the second scheduler should handle the rest of the dag processing.
        """

        celery_worker_process = None
        scheduler_process_1 = None
        scheduler_process_2 = None
        try:
            # Start the processes here and not as fixtures or in a common setup,
            # so that the test can capture their output.
            celery_worker_process, scheduler_process_1 = self.start_worker_and_scheduler1()

            dag_id = "otel_test_dag"
            dag = self.serialize_and_get_dag(dag_id=dag_id)

            run_id = unpause_trigger_dag_and_get_run_id(dag_id=dag_id)

            with create_session() as session:
                tis: list[TaskInstance] = dag.get_task_instances(session=session)

            task_1 = tis[0]

            while True:
                with create_session() as session:
                    ti = (
                        session.query(TaskInstance)
                        .filter(
                            TaskInstance.task_id == task_1.task_id,
                            TaskInstance.run_id == task_1.run_id,
                        )
                        .first()
                    )

                    if ti is None:
                        continue

                    # Wait until the task has been finished.
                    if ti.state in State.finished:
                        break

            with capfd.disabled():
                # When we pause the scheduler1 thread, capfd keeps trying to read the
                # file descriptors for the process and ends up freezing the test.
                # There won't be any exported spans from the following code,
                # so not capturing the output, doesn't make a difference.
                scheduler_process_1.send_signal(signal.SIGSTOP)

                scheduler_process_2 = subprocess.Popen(
                    self.scheduler_command_args,
                    env=os.environ.copy(),
                    stdout=None,
                    stderr=None,
                )

                with create_session() as session:
                    dag_run = (
                        session.query(DagRun)
                        .filter(
                            DagRun.dag_id == dag_id,
                            DagRun.run_id == run_id,
                        )
                        .first()
                    )

                    assert (
                        dag_run.state == State.RUNNING
                    ), f"Dag Run state isn't RUNNING. State: {dag_run.state}"
                    assert (
                        dag_run.span_status == SpanStatus.ACTIVE
                    ), f"Dag Run span_status isn't ACTIVE. Span_status: {dag_run.span_status}"

                # Wait for scheduler2 to be up and running.
                time.sleep(10)

                wait_for_dag_run_and_check_span_status(
                    dag_id=dag_id, run_id=run_id, max_wait_time=120, span_status=SpanStatus.SHOULD_END
                )

                if logging.root.level == logging.DEBUG:
                    with create_session() as session:
                        dump_airflow_metadata_db(session)

                scheduler_process_1.send_signal(signal.SIGCONT)
                scheduler_process_2.terminate()

            # Wait for the scheduler to start again and continue running.
            time.sleep(10)

            wait_for_dag_run_and_check_span_status(
                dag_id=dag_id, run_id=run_id, max_wait_time=30, span_status=SpanStatus.ENDED
            )

        finally:
            # Terminate the processes.
            celery_worker_process.terminate()
            celery_worker_process.wait()

            scheduler_process_1.terminate()
            scheduler_process_1.wait()

            # scheduler_process_2.terminate()
            scheduler_process_2.wait()

        out, err = capfd.readouterr()
        log.debug("out-start --\n%s\n-- out-end", out)
        log.debug("err-start --\n%s\n-- err-end", err)

        if self.use_otel != "true":
            # Dag run should have succeeded. Test the spans in the output.
            check_spans_without_continuance(output=out, dag=dag)

    def test_scheduler_change_in_the_middle_of_first_task_execution_until_the_end(
        self, monkeypatch, celery_worker_env_vars, capfd, session
    ):
        """
        Similar to test_dag_spans_with_context_propagation but this test will start with one scheduler,
        and during the dag execution, it will stop the process and start a new one.

        A txt file will be used for signaling the test and the dag in order to make sure that
        the 1st scheduler is stopped while the first task is executing and that
        the 2nd scheduler picks up the task and dag processing.
        The steps will be
        - The dag starts running, creates the file with a signal word and waits until the word is changed.
        - The test checks if the file exist, stops the scheduler, starts a new scheduler and updates the file.
        - The dag gets the update and continues until the task is finished.
        At this point, the second scheduler should handle the rest of the dag processing.
        """

        celery_worker_process = None
        scheduler_process_1 = None
        scheduler_process_2 = None
        try:
            # Start the processes here and not as fixtures or in a common setup,
            # so that the test can capture their output.
            celery_worker_process, scheduler_process_1 = self.start_worker_and_scheduler1()

            dag_id = "otel_test_dag"
            dag = self.serialize_and_get_dag(dag_id=dag_id)

            run_id = unpause_trigger_dag_and_get_run_id(dag_id=dag_id)

            with create_session() as session:
                tis: list[TaskInstance] = dag.get_task_instances(session=session)

            task_1 = tis[0]

            while True:
                with create_session() as session:
                    ti = (
                        session.query(TaskInstance)
                        .filter(
                            TaskInstance.task_id == task_1.task_id,
                            TaskInstance.run_id == task_1.run_id,
                        )
                        .first()
                    )

                    if ti is None:
                        continue

                    # Wait until the task has been finished.
                    if ti.state in State.finished:
                        break

            with capfd.disabled():
                # When we pause the scheduler1 thread, capfd keeps trying to read the
                # file descriptors for the process and ends up freezing the test.
                # There won't be any exported spans from the following code,
                # so not capturing the output, doesn't make a difference.
                scheduler_process_1.send_signal(signal.SIGSTOP)

                scheduler_process_2 = subprocess.Popen(
                    self.scheduler_command_args,
                    env=os.environ.copy(),
                    stdout=None,
                    stderr=None,
                )

                with create_session() as session:
                    dag_run = (
                        session.query(DagRun)
                        .filter(
                            DagRun.dag_id == dag_id,
                            DagRun.run_id == run_id,
                        )
                        .first()
                    )

                    assert (
                        dag_run.state == State.RUNNING
                    ), f"Dag Run state isn't RUNNING. State: {dag_run.state}"
                    assert (
                        dag_run.span_status == SpanStatus.ACTIVE
                    ), f"Dag Run span_status isn't ACTIVE. Span_status: {dag_run.span_status}"

                # Wait for scheduler2 to be up and running.
                time.sleep(10)

                wait_for_dag_run_and_check_span_status(
                    dag_id=dag_id, run_id=run_id, max_wait_time=120, span_status=SpanStatus.SHOULD_END
                )

                if logging.root.level == logging.DEBUG:
                    with create_session() as session:
                        dump_airflow_metadata_db(session)

                scheduler_process_1.send_signal(signal.SIGCONT)
                scheduler_process_2.terminate()

            # Wait for the scheduler to start again and continue running.
            time.sleep(10)

            wait_for_dag_run_and_check_span_status(
                dag_id=dag_id, run_id=run_id, max_wait_time=30, span_status=SpanStatus.ENDED
            )
        finally:
            # Terminate the processes.
            celery_worker_process.terminate()
            celery_worker_process.wait()

            scheduler_process_1.terminate()
            scheduler_process_1.wait()

            # scheduler_process_2.terminate()
            scheduler_process_2.wait()

        out, err = capfd.readouterr()
        log.debug("out-start --\n%s\n-- out-end", out)
        log.debug("err-start --\n%s\n-- err-end", err)

        if self.use_otel != "true":
            # Dag run should have succeeded. Test the spans in the output.
            check_spans_without_continuance(output=out, dag=dag)

    def test_scheduler_exits_gracefully_in_the_middle_of_the_first_task(
        self, monkeypatch, celery_worker_env_vars, capfd, session
    ):
        """
        Similar to test_dag_spans_with_context_propagation but this test will start with one scheduler,
        and during the dag execution, it will stop the process and start a new one.

        A txt file will be used for signaling the test and the dag in order to make sure that
        the 1st scheduler is stopped while the first task is executing and that
        the 2nd scheduler picks up the task and dag processing.
        The steps will be
        - The dag starts running, creates the file with a signal word and waits until the word is changed.
        - The test checks if the file exist, stops the scheduler, starts a new scheduler and updates the file.
        - The dag gets the update and continues until the task is finished.
        At this point, the second scheduler should handle the rest of the dag processing.
        """

        celery_worker_process = None
        scheduler_process_1 = None
        scheduler_process_2 = None
        try:
            # Start the processes here and not as fixtures or in a common setup,
            # so that the test can capture their output.
            celery_worker_process, scheduler_process_1 = self.start_worker_and_scheduler1()

            dag_id = "otel_test_dag_with_pause"
            dag = self.serialize_and_get_dag(dag_id=dag_id)

            run_id = unpause_trigger_dag_and_get_run_id(dag_id=dag_id)

            # Control file path.
            control_file = os.path.join(self.dag_folder, "dag_control.txt")

            while True:
                try:
                    with open(control_file) as file:
                        # If it reaches inside the block, then the file exists and the test can read it.
                        break
                except FileNotFoundError:
                    print("Control file not found. Waiting...")
                    time.sleep(1)
                    continue

            # Since, we are past the loop, then the file exists and the dag has been paused.
            # Terminate scheduler1 and start scheduler2.
            scheduler_process_1.terminate()

            scheduler_process_2 = subprocess.Popen(
                self.scheduler_command_args,
                env=os.environ.copy(),
                stdout=None,
                stderr=None,
            )

            # Wait for scheduler2 to be up and running.
            time.sleep(10)

            # Rewrite the file to unpause the dag.
            with open(control_file, "w") as file:
                file.write("continue")

            wait_for_dag_run_and_check_span_status(
                dag_id=dag_id, run_id=run_id, max_wait_time=120, span_status=SpanStatus.ENDED
            )

            if logging.root.level == logging.DEBUG:
                with create_session() as session:
                    dump_airflow_metadata_db(session)

        finally:
            # Terminate the processes.
            celery_worker_process.terminate()
            celery_worker_process.wait()

            scheduler_process_1.wait()

            scheduler_process_2.terminate()
            scheduler_process_2.wait()

        out, err = capfd.readouterr()
        log.debug("out-start --\n%s\n-- out-end", out)
        log.debug("err-start --\n%s\n-- err-end", err)

        if self.use_otel != "true":
            # Dag run should have succeeded. Test the spans in the output.
            check_spans_with_continuance(output=out, dag=dag)

    def test_scheduler_exits_forcefully_in_the_middle_of_the_first_task(
        self, monkeypatch, celery_worker_env_vars, capfd, session
    ):
        """
        This test will start with one scheduler, and during the dag execution,
        it will stop the process and start a new one.

        A txt file will be used for signaling the test and the dag in order to make sure that
        the 1st scheduler is stopped while the first task is executing and that
        the 2nd scheduler picks up the task and dag processing.
        The steps will be
        - The dag starts running, creates the file with a signal word and waits until the word is changed.
        - The test checks if the file exist, stops the scheduler, starts a new scheduler and updates the file.
        - The dag gets the update and continues until the task is finished.
        At this point, the second scheduler should handle the rest of the dag processing.
        """

        celery_worker_process = None
        scheduler_process_1 = None
        scheduler_process_2 = None
        try:
            # Start the processes here and not as fixtures or in a common setup,
            # so that the test can capture their output.
            celery_worker_process, scheduler_process_1 = self.start_worker_and_scheduler1()

            dag_id = "otel_test_dag_with_pause"
            dag = self.serialize_and_get_dag(dag_id=dag_id)

            run_id = unpause_trigger_dag_and_get_run_id(dag_id=dag_id)

            # Control file path.
            control_file = os.path.join(self.dag_folder, "dag_control.txt")

            while True:
                try:
                    with open(control_file) as file:
                        # If it reaches inside the block, then the file exists and the test can read it.
                        break
                except FileNotFoundError:
                    print("Control file not found. Waiting...")
                    time.sleep(1)
                    continue

            # Since, we are past the loop, then the file exists and the dag has been paused.
            # Terminate scheduler1 and start scheduler2.
            scheduler_process_1.terminate()

            scheduler_process_2 = subprocess.Popen(
                self.scheduler_command_args,
                env=os.environ.copy(),
                stdout=None,
                stderr=None,
            )

            # Wait for scheduler2 to be up and running.
            time.sleep(10)

            # Rewrite the file to unpause the dag.
            with open(control_file, "w") as file:
                file.write("continue")

            wait_for_dag_run_and_check_span_status(
                dag_id=dag_id, run_id=run_id, max_wait_time=120, span_status=SpanStatus.ENDED
            )

            if logging.root.level == logging.DEBUG:
                with create_session() as session:
                    dump_airflow_metadata_db(session)

        finally:
            # Terminate the processes.
            celery_worker_process.terminate()
            celery_worker_process.wait()

            scheduler_process_1.wait()

            scheduler_process_2.terminate()
            scheduler_process_2.wait()

        out, err = capfd.readouterr()
        log.debug("out-start --\n%s\n-- out-end", out)
        log.debug("err-start --\n%s\n-- err-end", err)

        if self.use_otel != "true":
            # Dag run should have succeeded. Test the spans in the output.
            check_spans_with_continuance(output=out, dag=dag)

    def start_worker_and_scheduler1(self):
        celery_worker_process = subprocess.Popen(
            self.celery_command_args,
            env=os.environ.copy(),
            stdout=None,
            stderr=None,
        )

        scheduler_process = subprocess.Popen(
            self.scheduler_command_args,
            env=os.environ.copy(),
            stdout=None,
            stderr=None,
        )

        # Wait to ensure both processes have started.
        time.sleep(10)

        return celery_worker_process, scheduler_process

    def serialize_and_get_dag(self, dag_id: str) -> DAG:
        # Load DAGs from the dag directory.
        dag_bag = DagBag(dag_folder=self.dag_folder, include_examples=False)

        dag = dag_bag.get_dag(dag_id)

        assert dag is not None, f"DAG with ID {dag_id} not found."

        with create_session() as session:
            # Sync the DAG to the database.
            dag.sync_to_db(session=session)
            # Manually serialize the dag and write it to the db to avoid a db error.
            SerializedDagModel.write_dag(dag, session=session)
            session.commit()

        return dag

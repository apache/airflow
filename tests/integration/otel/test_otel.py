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
import subprocess
import time

import pendulum
import pytest

from airflow.executors import executor_loader
from airflow.executors.executor_utils import ExecutorName
from airflow.models import DagBag, DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.traces.otel_tracer import CTX_PROP_SUFFIX
from airflow.utils.session import create_session
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


@pytest.mark.integration("redis")
@pytest.mark.backend("postgres")
class TestOtelIntegration:
    test_dir = os.path.dirname(os.path.abspath(__file__))
    dag_folder = os.path.join(test_dir, "dags")

    @classmethod
    def setup_class(cls):
        os.environ["AIRFLOW__TRACES__OTEL_ON"] = "True"
        os.environ["AIRFLOW__TRACES__OTEL_HOST"] = "localhost"
        os.environ["AIRFLOW__TRACES__OTEL_PORT"] = "4318"
        os.environ["AIRFLOW__TRACES__OTEL_DEBUGGING_ON"] = "True"
        os.environ["AIRFLOW__TRACES__OTEL_TASK_LOG_EVENT"] = "True"
        os.environ["AIRFLOW__TRACES__OTEL_USE_CONTEXT_PROPAGATION"] = "True"

        os.environ["AIRFLOW__SCHEDULER__STANDALONE_DAG_PROCESSOR"] = "False"
        os.environ["AIRFLOW__SCHEDULER__PROCESSOR_POLL_INTERVAL"] = "2"

        os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = f"{cls.dag_folder}"

        os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
        os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "False"

    @pytest.fixture
    def celery_worker_env_vars(self, monkeypatch):
        os.environ["AIRFLOW__CORE__EXECUTOR"] = "CeleryExecutor"
        executor_name = ExecutorName(
            module_path="airflow.providers.celery.executors.celery_executor.CeleryExecutor",
            alias="CeleryExecutor",
        )
        monkeypatch.setattr(executor_loader, "_alias_to_executors", {"CeleryExecutor": executor_name})

    def test_dag_spans_with_context_propagation(self, monkeypatch, celery_worker_env_vars, capfd, session):
        """
        Test that a DAG runs successfully and exports the correct spans.
        Integration with a scheduler, a celery worker, a postgres db and a redis broker.
        """
        # Uncomment to enable debug mode and get span and db dumps on the output.
        # log.setLevel(logging.DEBUG)

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

        celery_worker_process = None
        scheduler_process = None
        try:
            # Start the processes here and not as fixtures, so that the test can capture their output.
            celery_worker_process = subprocess.Popen(
                celery_command_args,
                env=os.environ.copy(),
                stdout=None,
                stderr=None,
            )

            scheduler_process = subprocess.Popen(
                scheduler_command_args,
                env=os.environ.copy(),
                stdout=None,
                stderr=None,
            )

            # Wait to ensure both processes have started.
            time.sleep(10)

            execution_date = pendulum.now("UTC")

            # Load DAGs from the dag directory.
            dag_bag = DagBag(dag_folder=self.dag_folder, include_examples=False)

            dag_id = "otel_test_dag"
            dag = dag_bag.get_dag(dag_id)

            assert dag is not None, f"DAG with ID {dag_id} not found."

            with create_session() as session:
                # Sync the DAG to the database.
                dag.sync_to_db(session=session)
                # Manually serialize the dag and write it to the db to avoid a db error.
                SerializedDagModel.write_dag(dag, session=session)
                session.commit()

            unpause_command = ["airflow", "dags", "unpause", dag_id]

            # Unpause the dag using the cli.
            subprocess.run(unpause_command, check=True, env=os.environ.copy())

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

            # Wait timeout for the DAG run to complete.
            max_wait_time = 60  # seconds
            start_time = time.time()

            dag_run_state = None

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

                    if dag_run_state in [State.SUCCESS, State.FAILED]:
                        break

                time.sleep(5)

            if logging.root.level == logging.DEBUG:
                with create_session() as session:
                    dump_airflow_metadata_db(session)

            assert (
                dag_run_state == State.SUCCESS
            ), f"Dag run did not complete successfully. Final state: {dag_run_state}."
        finally:
            # Terminate the processes.
            celery_worker_process.terminate()
            celery_worker_process.wait()

            scheduler_process.terminate()
            scheduler_process.wait()

        out, err = capfd.readouterr()
        log.debug("out-start --\n%s\n-- out-end", out)
        log.debug("err-start --\n%s\n-- err-end", err)

        # Dag run should have succeeded. Test the spans in the output.
        # Get a list of lines from the captured output.
        output_lines = out.splitlines()

        # Filter the output, create a json obj for each span and then store them into dictionaries.
        # One dictionary with only the root spans, and one with all the captured spans (root and otherwise).
        root_span_dict, span_dict = extract_spans_from_output(output_lines)
        # Generate a dictionary with parent child relationships.
        # This is done by comparing the span_id of each root span with the parent_id of each non-root span.
        parent_child_dict = get_parent_child_dict(root_span_dict, span_dict)

        dag_span_name = str(dag_id + CTX_PROP_SUFFIX)
        assert_span_name_belongs_to_root_span(
            root_span_dict=root_span_dict, span_name=dag_span_name, should_succeed=True
        )

        non_existent_dag_span_name = str(dag_id + CTX_PROP_SUFFIX + "fail")
        assert_span_name_belongs_to_root_span(
            root_span_dict=root_span_dict, span_name=non_existent_dag_span_name, should_succeed=False
        )

        dag_children_span_names = []
        task_instance_ids = dag.task_ids

        for task_id in task_instance_ids:
            dag_children_span_names.append(f"{task_id}{CTX_PROP_SUFFIX}")

        first_task_id = task_instance_ids[0]

        assert_parent_children_spans(
            parent_child_dict=parent_child_dict,
            root_span_dict=root_span_dict,
            parent_name=dag_span_name,
            children_names=dag_children_span_names,
        )

        assert_span_not_in_children_spans(
            parent_child_dict=parent_child_dict,
            root_span_dict=root_span_dict,
            span_dict=span_dict,
            parent_name=dag_span_name,
            child_name=first_task_id,
            span_exists=True,
        )

        assert_span_not_in_children_spans(
            parent_child_dict=parent_child_dict,
            root_span_dict=root_span_dict,
            span_dict=span_dict,
            parent_name=dag_span_name,
            child_name=f"{first_task_id}_fail",
            span_exists=False,
        )

        # Any spans generated under a task, are children of the task span.
        # The span hierarchy for dag 'test_dag' is
        # dag span
        #   |_ task_1 span
        #       |_ sub_span_1
        #           |_ sub_span_2
        #               |_ sub_span_3
        #       |_ sub_span_4
        #   |_ task_2 span

        first_task_children_span_names = [
            f"{first_task_id}_sub_span1{CTX_PROP_SUFFIX}",
            f"{first_task_id}_sub_span4{CTX_PROP_SUFFIX}",
        ]
        assert_parent_children_spans_for_non_root(
            span_dict=span_dict,
            parent_name=f"{first_task_id}{CTX_PROP_SUFFIX}",
            children_names=first_task_children_span_names,
        )

        # Single element list.
        sub_span1_children_span_names = [f"{first_task_id}_sub_span2{CTX_PROP_SUFFIX}"]
        assert_parent_children_spans_for_non_root(
            span_dict=span_dict,
            parent_name=f"{first_task_id}_sub_span1{CTX_PROP_SUFFIX}",
            children_names=sub_span1_children_span_names,
        )

        sub_span2_children_span_names = [f"{first_task_id}_sub_span3{CTX_PROP_SUFFIX}"]
        assert_parent_children_spans_for_non_root(
            span_dict=span_dict,
            parent_name=f"{first_task_id}_sub_span2{CTX_PROP_SUFFIX}",
            children_names=sub_span2_children_span_names,
        )

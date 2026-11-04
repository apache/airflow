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

from datetime import timedelta
from unittest import mock

import modal
import pytest

from airflow.providers.modal.decorators.modal import _ModalDecoratedOperator
from airflow.providers.modal.exceptions import ModalSandboxError
from airflow.providers.modal.hooks.modal import ModalHook
from airflow.providers.modal.triggers.modal import ModalSandboxTrigger
from airflow.providers.modal.utils.python_sandbox import ENTRYPOINT, send_payload
from airflow.sdk import DAG, task
from airflow.sdk.api.datamodels._generated import TaskInstanceState
from airflow.sdk.execution_time.comms import DeferTask

from unit.modal.local_sandbox import LocalSandbox


@pytest.fixture
def sandboxes(tmp_path):
    """
    Patch ``ModalHook.create_sandbox`` to run each sandbox locally; yields the sandboxes created.

    ``get_sandbox`` returns the sandbox with the requested id, as reattaching does on Modal.
    """
    created: list[LocalSandbox] = []

    def create_sandbox(self, *entrypoint, app_name, create_app_if_missing=False, **sandbox_kwargs):
        sandbox = LocalSandbox(*entrypoint, tmp_path=tmp_path, timeout=sandbox_kwargs.get("timeout"))
        sandbox.create_kwargs = {
            "app_name": app_name,
            "create_app_if_missing": create_app_if_missing,
            **sandbox_kwargs,
        }
        created.append(sandbox)
        return sandbox

    def get_sandbox(self, sandbox_id):
        return next(sandbox for sandbox in created if sandbox.object_id == sandbox_id)

    with (
        mock.patch.object(ModalHook, "create_sandbox", autospec=True, side_effect=create_sandbox),
        mock.patch.object(ModalHook, "get_sandbox", autospec=True, side_effect=get_sandbox),
    ):
        yield created


def _operator(xcom_arg) -> _ModalDecoratedOperator:
    operator = xcom_arg.operator
    assert isinstance(operator, _ModalDecoratedOperator)
    return operator


class TestModalTask:
    def test_return_value_reaches_xcom(self, run_task, sandboxes):
        with DAG(dag_id="modal_dag"):

            @task.modal(image=modal.Image.debian_slim(python_version="3.12"))
            def summarize(values: list[int], scale: int) -> dict:
                import statistics

                return {"mean": statistics.mean(values) * scale}

            operator = _operator(summarize([1, 2, 3], scale=10))

        run_task(operator)

        assert run_task.state == TaskInstanceState.SUCCESS, run_task.error
        assert run_task.xcom.get(key="return_value") == {"mean": 20}

    def test_multiple_outputs_unrolls_the_returned_dict(self, run_task, sandboxes):
        with DAG(dag_id="modal_dag"):

            @task.modal(multiple_outputs=True)
            def split() -> dict[str, int]:
                return {"rows": 3, "bytes": 1024}

            operator = _operator(split())

        run_task(operator)

        assert run_task.state == TaskInstanceState.SUCCESS, run_task.error
        assert run_task.xcom.get(key="rows") == 3
        assert run_task.xcom.get(key="bytes") == 1024

    def test_function_error_fails_the_task_with_its_exit_code(self, run_task, sandboxes):
        with DAG(dag_id="modal_dag"):

            @task.modal
            def fail():
                raise ValueError("boom")

            operator = _operator(fail())

        run_task(operator)

        assert run_task.state == TaskInstanceState.FAILED
        assert isinstance(run_task.error, ModalSandboxError)
        assert run_task.error.returncode == 1
        assert run_task.error.sandbox_id == "sb-local"

    def test_function_output_goes_to_the_task_log(self, run_task, sandboxes):
        with DAG(dag_id="modal_dag"):

            @task.modal
            def chatty():
                print("loading the model")
                return "done"

            operator = _operator(chatty())

        with mock.patch.object(type(operator), "log", new_callable=mock.PropertyMock) as log:
            run_task(operator)

        logged = [call.args for call in log.return_value.info.call_args_list]
        output_lines = [args for args in logged if args[0] == "%s"]
        assert output_lines == [("%s", "loading the model")], "only the function's output is logged"

    def test_sandbox_gets_the_task_options_and_identifying_tags(self, run_task, sandboxes):
        image = modal.Image.debian_slim(python_version="3.12").pip_install("pydantic<2")
        secret = modal.Secret.from_dict({"API_KEY": "x"})
        volume = mock.sentinel.volume
        with DAG(dag_id="modal_dag"):

            @task.modal(
                image=image,
                gpu="H100",
                cpu=(2.0, 4.0),
                memory=8192,
                timeout=3600,
                secrets=[secret],
                volumes={"/data": volume},
                app_name="ml-jobs",
            )
            def infer():
                return None

            operator = _operator(infer())

        run_task(operator, run_id="manual__2026-11-04T10:00:00+00:00", try_number=2)

        (sandbox,) = sandboxes
        assert sandbox.entrypoint == ENTRYPOINT
        assert sandbox.create_kwargs == {
            "app_name": "ml-jobs",
            "create_app_if_missing": True,
            "image": image,
            "gpu": "H100",
            "cpu": (2.0, 4.0),
            "memory": 8192,
            "timeout": 3600,
            "secrets": [secret],
            "volumes": {"/data": volume},
            "tags": {
                "airflow_dag_id": "modal_dag",
                "airflow_task_id": "infer",
                "airflow_run_id": "manual__2026-11-04T10:00:00+00:00",
                "airflow_map_index": "-1",
                "airflow_try_number": "2",
            },
        }

    def test_secret_and_volume_names_resolve_through_the_connection_at_run_time(self, run_task, sandboxes):
        named_secret = modal.Secret.from_dict({"HF_TOKEN": "x"})
        named_volume = modal.Volume.from_name("weights")
        inline_secret = modal.Secret.from_dict({"API_KEY": "y"})
        with DAG(dag_id="modal_dag"):

            @task.modal(
                modal_conn_id="modal_gpu",
                secrets=["huggingface", inline_secret],
                volumes={"/weights": "weights"},
            )
            def infer():
                return None

            operator = _operator(infer())

        with (
            mock.patch.object(
                ModalHook, "get_secret", autospec=True, return_value=named_secret
            ) as get_secret,
            mock.patch.object(
                ModalHook, "get_volume", autospec=True, return_value=named_volume
            ) as get_volume,
        ):
            run_task(operator)

        (sandbox,) = sandboxes
        assert sandbox.create_kwargs["secrets"] == [named_secret, inline_secret]
        assert sandbox.create_kwargs["volumes"] == {"/weights": named_volume}
        (hook, name), _ = get_secret.call_args
        assert (hook.modal_conn_id, name) == ("modal_gpu", "huggingface")
        (hook, name), _ = get_volume.call_args
        assert (hook.modal_conn_id, name) == ("modal_gpu", "weights")

    def test_unset_options_are_left_to_modal_and_image_defaults_to_debian_slim(self, run_task, sandboxes):
        with DAG(dag_id="modal_dag"):

            @task.modal
            def noop():
                return None

            operator = _operator(noop())

        with mock.patch.object(modal.Image, "debian_slim", autospec=True) as debian_slim:
            run_task(operator)

        (sandbox,) = sandboxes
        assert set(sandbox.create_kwargs) == {"app_name", "create_app_if_missing", "image", "tags"}
        assert sandbox.create_kwargs["app_name"] == "airflow"
        assert sandbox.create_kwargs["image"] is debian_slim.return_value

    def test_a_single_secret_name_is_one_secret(self, run_task, sandboxes):
        secret = modal.Secret.from_dict({"HF_TOKEN": "x"})
        with DAG(dag_id="modal_dag"):

            @task.modal(secrets="huggingface")
            def infer():
                return None

            operator = _operator(infer())

        with mock.patch.object(ModalHook, "get_secret", autospec=True, return_value=secret) as get_secret:
            run_task(operator)

        (sandbox,) = sandboxes
        assert sandbox.create_kwargs["secrets"] == [secret]
        assert get_secret.call_args.args[1:] == ("huggingface",)

    @pytest.mark.parametrize(
        ("timeout", "execution_timeout", "sandbox_timeout"),
        [
            pytest.param(3600, timedelta(seconds=30), 30, id="execution-timeout-caps-timeout"),
            pytest.param(None, timedelta(seconds=30.5), 31, id="execution-timeout-alone"),
            pytest.param(10, timedelta(seconds=30), 10, id="shorter-timeout-wins"),
            pytest.param(10, None, 10, id="timeout-alone"),
        ],
    )
    def test_execution_timeout_caps_the_sandbox_timeout(
        self, run_task, sandboxes, timeout, execution_timeout, sandbox_timeout
    ):
        with DAG(dag_id="modal_dag"):

            @task.modal(timeout=timeout, execution_timeout=execution_timeout)
            def noop():
                return None

            operator = _operator(noop())

        run_task(operator)

        (sandbox,) = sandboxes
        assert sandbox.create_kwargs["timeout"] == sandbox_timeout


class TestFailures:
    def test_timeout_fails_the_task_with_exit_code_124(self, run_task, sandboxes):
        with DAG(dag_id="modal_dag"):

            @task.modal(timeout=1)
            def nap():
                import time

                time.sleep(30)

            operator = _operator(nap())

        run_task(operator)

        assert run_task.state == TaskInstanceState.FAILED
        assert isinstance(run_task.error, ModalSandboxError)
        assert run_task.error.returncode == 124
        assert "reached its timeout (1s)" in str(run_task.error)

    def test_losing_the_output_stream_keeps_waiting_for_the_result(self, run_task, sandboxes, monkeypatch):
        def broken_stream(self):
            raise modal.exception.ConnectionError("stream reset")

        monkeypatch.setattr(LocalSandbox, "stderr", property(broken_stream))
        with DAG(dag_id="modal_dag"):

            @task.modal
            def train():
                return {"loss": 0.1}

            operator = _operator(train())

        run_task(operator)

        assert run_task.state == TaskInstanceState.SUCCESS, run_task.error
        assert run_task.xcom.get(key="return_value") == {"loss": 0.1}
        (sandbox,) = sandboxes
        assert not sandbox.terminated

    def test_losing_track_of_the_sandbox_terminates_it(self, run_task, sandboxes, monkeypatch):
        def lost(self, raise_on_termination=True):
            raise modal.exception.ConnectionError("connection lost")

        monkeypatch.setattr(LocalSandbox, "wait", lost)
        with DAG(dag_id="modal_dag"):

            @task.modal
            def train():
                return None

            operator = _operator(train())

        run_task(operator)

        assert run_task.state == TaskInstanceState.FAILED
        assert isinstance(run_task.error, modal.exception.ConnectionError)
        (sandbox,) = sandboxes
        assert sandbox.terminated


class TestDeferrable:
    def test_defers_to_a_trigger_for_the_started_sandbox(self, run_task, sandboxes):
        with DAG(dag_id="modal_dag"):

            @task.modal(deferrable=True, modal_conn_id="modal_gpu", execution_timeout=timedelta(hours=2))
            def train():
                return {"loss": 0.1}

            operator = _operator(train())

        run_task(operator)

        assert run_task.state == TaskInstanceState.DEFERRED
        assert isinstance(run_task.msg, DeferTask)
        classpath, kwargs = ModalSandboxTrigger(sandbox_id="sb-local", modal_conn_id="modal_gpu").serialize()
        assert run_task.msg.classpath == classpath
        assert run_task.msg.trigger_kwargs == kwargs
        assert run_task.msg.next_method == "execute_complete"
        assert run_task.msg.trigger_timeout == timedelta(hours=2)
        (sandbox,) = sandboxes
        assert not sandbox.terminated, "deferring must leave the sandbox running for the trigger"

    def test_execute_complete_reads_the_return_value_from_the_sandbox(self, sandboxes):
        with DAG(dag_id="modal_dag"):

            @task.modal(deferrable=True)
            def train():
                return {"loss": 0.1}

            operator = _operator(train())
        sandbox = ModalHook().create_sandbox(*ENTRYPOINT, app_name="airflow")
        send_payload(sandbox, operator._build_payload())

        result = operator.execute_complete(context={}, event={"sandbox_id": "sb-local", "returncode": 0})

        assert result == {"loss": 0.1}

    @pytest.mark.parametrize(
        ("returncode", "message"),
        [
            pytest.param(124, r"reached its timeout \(300s, Modal's default\)", id="timeout"),
            pytest.param(137, "was terminated", id="terminated"),
            pytest.param(1, "failed in Modal sandbox sb-1 with exit code 1", id="function-error"),
        ],
    )
    def test_execute_complete_raises_for_a_failed_sandbox(self, returncode, message):
        with DAG(dag_id="modal_dag"):

            @task.modal(deferrable=True)
            def train():
                return None

            operator = _operator(train())

        with pytest.raises(ModalSandboxError, match=message) as exc_info:
            operator.execute_complete(context={}, event={"sandbox_id": "sb-1", "returncode": returncode})

        assert exc_info.value.returncode == returncode


class TestOnKill:
    def test_terminates_the_running_sandbox(self, tmp_path):
        with DAG(dag_id="modal_dag"):

            @task.modal
            def train():
                return None

            operator = _operator(train())
        sandbox = LocalSandbox(*ENTRYPOINT, tmp_path=tmp_path)
        operator._sandbox = sandbox

        operator.on_kill()

        assert sandbox.terminated

    def test_a_failed_terminate_is_logged_not_raised(self, tmp_path, monkeypatch):
        def gone(self):
            raise modal.exception.NotFoundError("sandbox gone")

        monkeypatch.setattr(LocalSandbox, "terminate", gone)
        with DAG(dag_id="modal_dag"):

            @task.modal
            def train():
                return None

            operator = _operator(train())
        operator._sandbox = LocalSandbox(*ENTRYPOINT, tmp_path=tmp_path)

        with mock.patch.object(type(operator), "log", new_callable=mock.PropertyMock) as log:
            operator.on_kill()

        assert "Failed to terminate" in log.return_value.warning.call_args.args[0]

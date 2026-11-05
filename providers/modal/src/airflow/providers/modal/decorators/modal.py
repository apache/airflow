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
"""``@task.modal``: run a Python function in a Modal sandbox, in an image without Airflow."""

from __future__ import annotations

import math
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

import modal

from airflow.providers.common.compat.sdk import (
    BaseOperator,
    DecoratedOperator,
    conf,
    task_decorator_factory,
)
from airflow.providers.modal.exceptions import ModalSandboxError
from airflow.providers.modal.hooks.modal import ModalHook
from airflow.providers.modal.triggers.modal import ModalSandboxTrigger
from airflow.providers.modal.utils.python_sandbox import (
    ENTRYPOINT,
    TERMINATED_RETURNCODE,
    TIMEOUT_RETURNCODE,
    build_payload,
    decode_result,
    send_payload,
    wait_for_exit,
)

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context, TaskDecorator


class _ModalSandboxOperator(BaseOperator):
    """
    Run a prepared Python payload in a Modal sandbox and return the function's return value.

    The base half of ``@task.modal``. ``DecoratedOperator.execute`` hands off to the next
    ``execute`` in the MRO, so the Modal half lives here, under it, as ``DockerOperator`` does
    for ``@task.docker``. The subclass supplies the payload through :meth:`_build_payload`.
    """

    template_fields: Sequence[str] = ("modal_conn_id",)

    def __init__(
        self,
        *,
        image: modal.Image | None = None,
        gpu: str | None = None,
        cpu: float | tuple[float, float] | None = None,
        memory: int | tuple[int, int] | None = None,
        timeout: int | None = None,
        secrets: Sequence[modal.Secret | str] | str | None = None,
        volumes: dict[str, modal.Volume | str] | None = None,
        modal_conn_id: str | None = ModalHook.default_conn_name,
        app_name: str = "airflow",
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.image = image
        self.gpu = gpu
        self.cpu = cpu
        self.memory = memory
        self.timeout = timeout
        self.secrets = secrets
        self.volumes = volumes
        self.modal_conn_id = modal_conn_id
        self.app_name = app_name
        self.deferrable = deferrable
        self._sandbox: modal.Sandbox | None = None

    def _build_payload(self) -> str:
        raise NotImplementedError

    def _sandbox_options(self, hook: ModalHook) -> dict[str, Any]:
        """``Sandbox.create`` keyword arguments, leaving out the unset ones so Modal's defaults apply."""
        options: dict[str, Any] = {
            "gpu": self.gpu,
            "cpu": self.cpu,
            "memory": self.memory,
            "timeout": self._sandbox_timeout(),
        }
        options = {key: value for key, value in options.items() if value is not None}
        # Names resolve here, through the hook, so they use the connection's credentials and
        # environment, and a Dag file that names them makes no Modal call when it is parsed.
        if self.secrets is not None:
            secrets = [self.secrets] if isinstance(self.secrets, str) else self.secrets
            options["secrets"] = [
                hook.get_secret(secret) if isinstance(secret, str) else secret for secret in secrets
            ]
        if self.volumes is not None:
            options["volumes"] = {
                path: hook.get_volume(volume) if isinstance(volume, str) else volume
                for path, volume in self.volumes.items()
            }
        # Built when the task runs, so debian_slim() takes the Python version of the worker,
        # not of whichever process imported this module.
        options["image"] = self.image or modal.Image.debian_slim()
        return options

    def _sandbox_timeout(self) -> int | None:
        """
        Modal's timeout for the sandbox: the shorter of ``timeout`` and ``execution_timeout``.

        Either one alone is used as is; with neither, Modal's 300-second default applies.

        The cap is what stops a deferred task's sandbox when ``execution_timeout`` runs out: the
        triggerer gives up on the trigger without calling ``on_kill``, so only Modal can stop it.
        """
        if self.execution_timeout is None:
            return self.timeout
        cap = math.ceil(self.execution_timeout.total_seconds())
        return cap if self.timeout is None else min(self.timeout, cap)

    def _sandbox_tags(self, context: Context) -> dict[str, str]:
        """Tags that find the sandbox from a task instance in the Modal dashboard, and back."""
        ti = context["ti"]
        return {
            "airflow_dag_id": ti.dag_id,
            "airflow_task_id": ti.task_id,
            "airflow_run_id": ti.run_id,
            "airflow_map_index": str(ti.map_index),
            "airflow_try_number": str(ti.try_number),
        }

    def execute(self, context: Context) -> Any:
        payload = self._build_payload()
        hook = ModalHook(modal_conn_id=self.modal_conn_id)
        sandbox = hook.create_sandbox(
            *ENTRYPOINT,
            app_name=self.app_name,
            create_app_if_missing=True,
            tags=self._sandbox_tags(context),
            **self._sandbox_options(hook),
        )
        self._sandbox = sandbox
        self.log.info("Started Modal sandbox %s in app %r.", sandbox.object_id, self.app_name)
        # Deferring raises TaskDeferred, a BaseException, so it passes through and leaves the
        # sandbox running for the trigger. Any other error would leave a sandbox nobody follows,
        # and a retry would start a second one beside it.
        try:
            send_payload(sandbox, payload)
            if self.deferrable:
                self.defer(
                    trigger=ModalSandboxTrigger(
                        sandbox_id=sandbox.object_id, modal_conn_id=self.modal_conn_id
                    ),
                    method_name="execute_complete",
                    timeout=self.execution_timeout,
                )
            self._log_output(sandbox)
            wait_for_exit(sandbox)
        except Exception:
            self.on_kill()
            raise
        self._raise_for_returncode(sandbox.object_id, sandbox.returncode)
        return decode_result(sandbox.stdout.read())

    def _log_output(self, sandbox: modal.Sandbox) -> None:
        try:
            for line in sandbox.stderr:
                self.log.info("%s", line.rstrip("\n"))
        except Exception as e:
            # Modal's reader raises once its retries for stream errors run out. Losing the
            # output is no reason to stop a function that is still running.
            self.log.warning(
                "Lost the output of Modal sandbox %s (%s); waiting for it to exit.", sandbox.object_id, e
            )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> Any:
        """Read the return value once the trigger reports that the sandbox exited."""
        sandbox_id = event["sandbox_id"]
        self._raise_for_returncode(sandbox_id, event["returncode"])
        sandbox = ModalHook(modal_conn_id=self.modal_conn_id).get_sandbox(sandbox_id)
        return decode_result(sandbox.stdout.read())

    def _raise_for_returncode(self, sandbox_id: str, returncode: int | None) -> None:
        if returncode == 0:
            return
        if returncode == TIMEOUT_RETURNCODE:
            timeout = self._sandbox_timeout()
            limit = f"{timeout}s" if timeout is not None else "300s, Modal's default"
            message = (
                f"Modal sandbox {sandbox_id} reached its timeout ({limit}) before the function "
                "returned. Raise the decorator's timeout, or the task's execution_timeout if that "
                "is shorter."
            )
        elif returncode == TERMINATED_RETURNCODE:
            message = (
                f"Modal sandbox {sandbox_id} was terminated before the function returned, for "
                "example because it ran out of memory under a memory limit, or from the Modal "
                "dashboard."
            )
        else:
            message = (
                f"The function failed in Modal sandbox {sandbox_id} with exit code {returncode}. "
                "Its traceback is in the log above."
            )
        raise ModalSandboxError(message, sandbox_id=sandbox_id, returncode=returncode)

    def on_kill(self) -> None:
        if self._sandbox is None:
            return
        self.log.info("Terminating Modal sandbox %s.", self._sandbox.object_id)
        try:
            self._sandbox.terminate()
        except Exception as e:
            # execute() also calls this while another error propagates; a failed terminate must
            # not replace that error.
            self.log.warning("Failed to terminate Modal sandbox %s: %s", self._sandbox.object_id, e)


class _ModalDecoratedOperator(DecoratedOperator, _ModalSandboxOperator):
    """
    Run the decorated function's body in a Modal sandbox.

    The function's source is extracted and sent to the sandbox, as ``@task.docker`` and
    ``@task.kubernetes`` do, so the body must be self-contained: import what it needs inside
    the function, and take every input as an argument. Airflow is not needed
    in the image, and there is no task context, connection or hook inside the function. Pass
    credentials in as Modal secrets.

    Arguments and the return value travel as pickles, so they must load on the other side:
    use plain Python values, not objects from packages that only one side has installed.

    Its arguments are documented on :func:`modal_task`.
    """

    custom_operator_name = "@task.modal"

    template_fields: Sequence[str] = (*DecoratedOperator.template_fields, "modal_conn_id")

    # Modal objects hold SDK state that is not meant to be deep-copied, and are never mutated here.
    shallow_copy_attrs: Sequence[str] = ("python_callable", "image", "secrets", "volumes")

    @property
    def xcom_push(self) -> bool:
        """Read by ``DecoratedOperator.execute`` on Airflow 3.0, which passes it to ``_handle_output``."""
        return self.do_xcom_push

    @xcom_push.setter
    def xcom_push(self, value: bool) -> None:
        self.do_xcom_push = value

    def _build_payload(self) -> str:
        return build_payload(
            source=self.get_python_source(),
            function_name=self.python_callable.__name__,
            args=self.op_args,
            kwargs=dict(self.op_kwargs),
        )


def modal_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    **kwargs: Any,
) -> TaskDecorator:
    """
    Wrap a function into a task that runs its body in a Modal sandbox, as ``@task.modal``.

    Takes the arguments below as keywords, plus any ``BaseOperator`` argument.

    :param python_callable: Function to decorate.
    :param multiple_outputs: If set, a returned dict is unrolled to multiple XCom values, one
        per key. When unset, it is inferred from the function's return annotation.
    :param image: The ``modal.Image`` to run in. Defaults to ``modal.Image.debian_slim()`` built
        for the worker's Python version. Any image with Python 3.8 or later on its ``PATH`` as
        ``python`` works; the function's dependencies go here, not on the worker.
    :param gpu: GPU to request, for example ``"H100"`` or ``"A10G:2"``.
    :param cpu: CPU cores to reserve, or a ``(reservation, limit)`` pair to also cap usage.
    :param memory: Memory to reserve in MiB, or a ``(reservation, limit)`` pair to also cap it.
    :param timeout: Seconds the sandbox may run before Modal stops it. Modal's default is 300.
        When both this and the task's ``execution_timeout`` are set, the shorter wins; when only
        ``execution_timeout`` is set, the sandbox gets that.
    :param secrets: Modal secrets whose keys become environment variables inside the sandbox,
        as a name, or a list of names or ``modal.Secret`` objects. Names are looked up when the
        task runs, in the connection's environment.
    :param volumes: Mount points mapped to Modal volumes, as names or ``modal.Volume`` objects.
        Names are looked up when the task runs, in the connection's environment.
    :param modal_conn_id: :ref:`Modal connection id <howto/connection:modal>`.
    :param app_name: Modal app the sandboxes are created under, ``airflow`` by default; created
        on first use.
    :param deferrable: Wait for the sandbox on the triggerer instead of holding a worker slot.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_ModalDecoratedOperator,
        **kwargs,
    )

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
"""
Resume a `Strands Agents <https://strandsagents.com/>`__ agent where it stopped when Airflow retries its task.

.. note:: Experimental, as Strands' checkpointing is; see :mod:`strands.experimental.checkpoint`.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any, Protocol

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException, get_current_context

try:
    from strands.session import SnapshotSessionManager
    from strands.types.session import encode_bytes_values
except ImportError as e:
    raise AirflowOptionalProviderFeatureException(e)

from airflow.providers.common.ai.batch.state import compute_identity_key
from airflow.providers.common.ai.utils.coroutines import run_coroutine_sync
from airflow.providers.common.ai.utils.task_logger import get_task_logger
from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_3_PLUS

if AIRFLOW_V_3_3_PLUS:
    from airflow.providers.common.ai.durable.strands_storage import TaskStateStoreStorage

if TYPE_CHECKING:
    from strands import Agent
    from strands.agent import AgentResult
    from strands.session import SessionManager
    from strands.storage import Storage
    from strands.types.agent import AgentInput

__all__ = ["AgentFactory", "invoke_durably"]

log = get_task_logger()

# The key in ``agent.state`` that holds where the agent paused and for which prompt.
_CHECKPOINT_STATE_KEY = "airflow_checkpoint"


class AgentFactory(Protocol):
    """
    Builds the agent :func:`invoke_durably` runs, wiring in the two arguments it is called with.

    ``functools.partial(Agent, model=model, tools=[...])`` is one.
    """

    def __call__(self, *, session_manager: SessionManager, checkpointing: bool) -> Agent: ...


def invoke_durably(
    agent_factory: AgentFactory,
    prompt: AgentInput,
    *,
    session_id: str | None = None,
    storage: Storage | None = None,
) -> AgentResult:
    """
    Run a Strands agent so that a retry of the task resumes it from its last checkpoint.

    Call it inside a task. The agent is built with Strands' ``checkpointing=True`` and a
    ``SnapshotSessionManager``, so it pauses at every boundary of a cycle that calls tools:
    once the model has asked for tools, and once they have run. At each pause the
    conversation is saved, together with where the agent paused, before the agent carries
    on. When the task fails and Airflow retries it, the agent is rebuilt from the last
    pause, so the model calls and tool calls of the cycles before it do not run again.
    Once the agent stops for any other reason, such as its final answer, an interrupt or
    a cancellation, what was saved is deleted, so clearing the task later starts the
    agent over. :ref:`howto/frameworks:strands-durable` describes what a retry runs again.

    Pass a function that builds the agent, such as ``functools.partial(Agent, ...)``:

    .. code-block:: python

        from functools import partial

        from strands import Agent

        from airflow.providers.common.ai.durable.strands import invoke_durably
        from airflow.providers.common.ai.tools.strands import AirflowTools
        from airflow.sdk import task


        @task(retries=3)
        def research(question: str) -> str:
            agent = partial(Agent, model=model, plugins=[AirflowTools(warehouse)])
            return str(invoke_durably(agent, question))

    :param agent_factory: Builds the agent, given the ``session_manager`` and
        ``checkpointing`` arguments to pass to ``Agent``. It is called once per try, or
        twice when the saved state is from a different prompt.
    :param prompt: The prompt for the agent; ignored when resuming.
    :param session_id: Names the agent's saved state. Defaults to an id derived from the
        task instance (Dag id, run id, task id and map index), the same on every try. Pass
        a distinct one for each agent when a task runs more than one; it must stay the same
        across tries, and when ``storage`` is shared across Dag runs it must differ between
        them.
    :param storage: Where the agent's state is saved. Defaults to the task instance's task
        state store, on Airflow 3.3 and later; on older versions pass a Strands storage such
        as ``S3Storage``.
    :return: The result of the agent's last invocation. Its metrics cover every invocation
        of this try, one per resume in ``metrics.agent_invocations``, and none of the
        earlier tries.
    :raises ValueError: If the agent's model keeps the conversation server-side
        (``model.stateful``), since Strands drops a restored conversation for it.
    """
    context = get_current_context()
    if storage is None:
        if not AIRFLOW_V_3_3_PLUS:
            raise AirflowOptionalProviderFeatureException(
                "invoke_durably() saves to the task state store, which needs Airflow 3.3 or later. "
                "On older versions pass a Strands storage, such as strands.storage.S3Storage, as storage=."
            )
        storage = TaskStateStoreStorage(context["task_state_store"])
    if session_id is None:
        ti = context["ti"]
        # A hash: short (task state store keys are at most 512 characters) and free of the
        # characters Strands refuses in a session id, whatever the Dag, run and task ids hold.
        session_id = compute_identity_key(
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            run_id=ti.run_id,
            map_index=ti.map_index if ti.map_index is not None else -1,
        )
    prompt_fingerprint = _fingerprint(prompt)

    session_manager, agent = _build(agent_factory, session_id, storage)
    saved = agent.state.get(_CHECKPOINT_STATE_KEY)
    request: AgentInput | dict[str, Any]
    if isinstance(saved, dict) and saved.get("prompt") == prompt_fingerprint:
        checkpoint = saved["checkpoint"]
        log.info(
            "Resuming the Strands agent from its last checkpoint",
            session_id=session_id,
            position=checkpoint["position"],
            cycle=checkpoint["cycle_index"],
        )
        request = _resume(checkpoint)
    else:
        if saved is not None:
            # The agent was restored from an earlier try with a different prompt, so its
            # conversation cannot be resumed. Start over from an empty one.
            log.warning("The saved Strands agent state is for a different prompt; starting over")
            run_coroutine_sync(session_manager.delete_session())
            session_manager, agent = _build(agent_factory, session_id, storage)
        request = prompt

    # Strands types the prompt as AgentInput, which leaves out its checkpointResume block.
    result = agent(request)  # type: ignore[arg-type]
    while result.checkpoint is not None:
        checkpoint = result.checkpoint.to_dict()
        # In the snapshot itself, so the conversation and the pause it belongs to are saved
        # in one write and can never disagree.
        agent.state.set(_CHECKPOINT_STATE_KEY, {"checkpoint": checkpoint, "prompt": prompt_fingerprint})
        run_coroutine_sync(session_manager.save_snapshot(agent, is_latest=True))
        log.debug("Saved a Strands checkpoint", session_id=session_id, **checkpoint)
        result = agent(_resume(checkpoint))  # type: ignore[arg-type]

    try:
        run_coroutine_sync(session_manager.delete_session())
    except Exception:
        # The agent has finished; a failed delete must not fail the task. What is left is
        # removed with the Dag run, and a retry resumes from the last pause.
        log.warning("Could not delete the Strands agent's saved state", session_id=session_id, exc_info=True)
    return result


def _build(
    agent_factory: AgentFactory, session_id: str, storage: Storage
) -> tuple[SnapshotSessionManager, Agent]:
    # "trigger" with no trigger: Strands saves on its own only to redact or trim the
    # conversation it holds, not when an invocation ends or fails, so the snapshot stays
    # at the last pause.
    session_manager = SnapshotSessionManager(session_id, storage=storage, save_latest_on="trigger")
    agent = agent_factory(session_manager=session_manager, checkpointing=True)
    if agent.model.stateful:
        raise ValueError(
            "invoke_durably() cannot resume an agent whose model keeps the conversation "
            "server-side (model.stateful): Strands drops a restored conversation for it."
        )
    return session_manager, agent


def _fingerprint(prompt: AgentInput) -> str:
    # Bytes, such as an image in a content block, encoded the way Strands saves them.
    return hashlib.sha256(json.dumps(encode_bytes_values(prompt), sort_keys=True).encode()).hexdigest()


def _resume(checkpoint: dict[str, Any]) -> dict[str, Any]:
    return {"checkpointResume": {"checkpoint": checkpoint}}

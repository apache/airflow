 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

.. _message-history:

Multi-turn sessions and message history
=======================================

By default each agent run is a cold, single-turn conversation. To carry a
conversation across runs -- a chat or iterative agent where "and the third one?"
must resolve against an earlier answer -- pass ``message_history``.

When ``message_history`` is set, the operator seeds the run with those prior
turns and, after the run, pushes the full updated transcript
(``result.all_messages()``) to XCom under the key ``message_history``. The next
run reads it back to resume the conversation. ``None`` (the default) keeps the
single-turn behavior unchanged.

To record the transcript of a single-turn run, set ``emit_message_history=True``.
The operator starts the run with no prior turns and pushes ``result.all_messages()``
to the same ``message_history`` XCom key, so the transcript is available to
downstream tasks for inspection, evaluation or storage. ``emit_message_history`` defaults
to ``True`` when ``message_history`` is set; pass ``False`` to keep a seeded run from
pushing it.

The operator does **not** decide *where* a session is stored -- that keying is
deployment-specific. The pattern is three tasks: load the prior transcript for
the session, run the agent, store the updated transcript. The example keys a
JSON file in object storage by ``session_id`` (use ``s3://`` / ``gs://`` in a
deployment); the first run starts from an empty ``"[]"``.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent.py
    :language: python
    :start-after: [START howto_agent_session]
    :end-before: [END howto_agent_session]

``message_history`` accepts a list of pydantic-ai ``ModelMessage`` objects or
their JSON form (``str`` / ``bytes``), so the value emitted to XCom feeds
straight back in on the next run. When pulling it via a template, pass
``default='[]'`` (as above) so the first run -- which has no XCom yet -- starts a
fresh session instead of trying to parse the string ``"None"``.

.. note::

    ``message_history`` and ``emit_message_history`` cannot be combined with
    ``enable_hitl_review`` -- the
    operator raises at construction. The post-review (human-approved) transcript
    is not recoverable today, so emitting the pre-review transcript would
    silently drop the reviewed turns.

Message format, and limiting session size
-----------------------------------------

The transcript is stored as pydantic-ai's JSON message format. It is
**cumulative**: each turn appends to it, so it grows for the life of the
session. The operator does not trim it itself, but ``capabilities=`` is passed
to the pydantic-ai ``Agent``, so the ``ProcessHistory``
capability from pydantic-ai's documentation works on ``AgentOperator`` as is:

.. code-block:: python

    from pydantic_ai.capabilities import ProcessHistory
    from pydantic_ai.messages import ModelMessage, ModelRequest, RetryPromptPart, ToolReturnPart


    def keep_recent(messages: list[ModelMessage]) -> list[ModelMessage]:
        tail = messages[-10:]
        # Start at a user turn. A tail that begins at a request holding only tool
        # returns or retries (mid-run) loses the user's question.
        for i, message in enumerate(tail):
            if isinstance(message, ModelRequest) and not any(
                isinstance(part, (ToolReturnPart, RetryPromptPart)) for part in message.parts
            ):
                return tail[i:]
        return messages


    AgentOperator(
        task_id="chat",
        ...,
        capabilities=[ProcessHistory(keep_recent)],
    )

pydantic-ai writes the processed list back into the run state, and
``result.all_messages()`` returns that list. With a history processor set, the
transcript pushed to XCom is therefore the **trimmed** one, and turns the
processor drops are no longer part of the stored session. Alternatively, read the
``message_history`` XCom in a downstream task, trim it, store it in your own
session store, and load it from there on the next run; or configure an
object-storage XCom backend for long sessions.

The operator has no built-in cross-session memory. To let an agent use facts
from earlier sessions, store them yourself, load them in an upstream task (the
same load, run, store pattern as above), and put them in the prompt or the
instructions. To let the agent keep its own notes across Dag runs instead, see
:doc:`memory`.

Related pydantic-ai documentation
---------------------------------

The transcript is a pydantic-ai message list, so pydantic-ai's own
documentation applies to it. The
`message history page <https://pydantic.dev/docs/ai/core-concepts/message-history/>`__
covers accessing messages from a result, storing and loading them as JSON,
persisting sessions, editing existing messages, and processing the history
before it reaches the model (for example keeping only recent messages or
summarizing old ones).

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

The transcript is **cumulative**: each turn appends to it, so it grows for the
life of the session. For long sessions, configure an object-storage XCom backend
or trim older turns before the next run rather than feeding the whole history
back unbounded.

.. note::

    ``message_history`` cannot be combined with ``enable_hitl_review`` -- the
    operator raises at construction. The post-review (human-approved) transcript
    is not recoverable today, so emitting the pre-review transcript would
    silently drop the reviewed turns.

What this is, and what it is not
--------------------------------

``message_history`` is a hand-off of one conversation transcript from one run to
the next. When you pass ``message_history``, the operator reads that transcript,
and after the run it writes the full updated transcript to XCom under
``message_history`` as pydantic-ai's JSON message format. Where that transcript
lives between runs and which session key it is stored under are decisions for
your Dag. The operator does not trim the transcript.

Use it to resume a multi-turn conversation across task runs. If you need an
agent to recall facts across unrelated sessions, note that the operator has no
built-in cross-session memory.

Related pydantic-ai documentation
---------------------------------

The transcript is a pydantic-ai message list, so pydantic-ai's own
documentation applies to it. The
`message history page <https://pydantic.dev/docs/ai/core-concepts/message-history/>`__
covers accessing messages from a result, storing and loading them as JSON,
persisting sessions, editing existing messages, and processing the history
before it reaches the model (for example keeping only recent messages or
summarizing old ones). That page describes pydantic-ai's own ``Agent`` API, not
the operator's parameters; this page lists what ``AgentOperator`` itself does
with ``message_history``.

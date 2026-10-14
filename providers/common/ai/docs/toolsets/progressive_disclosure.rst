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

.. _howto/toolset:progressive-disclosure:

Progressive tool disclosure
===========================

Every tool an agent can call costs input tokens on every model request: its name,
description and argument schema are sent each time, whether the model uses the tool or
not. ``.defer_loading()`` hides a toolset's tools until the model searches for them, so
an agent can carry a wide toolset and pay mostly for the tools a run looks for. It is a
Pydantic AI method that every toolset has, including the ones this provider ships. It
pays off on a toolset with many tools, and does not on one with a handful, such as
``SQLToolset`` (see :ref:`progressive-disclosure-when-not`). A tool no run needs is better
left out of the toolset altogether, with ``allowed_methods`` or ``.filtered()``; defer the
ones only some runs need.

Hide a toolset's tools until the model needs them
-------------------------------------------------

This agent can call nine read-only ``S3Hook`` methods on one bucket, and the model sees
none of them until it searches:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_progressive_tool_disclosure.py
    :language: python
    :dedent: 4
    :start-after: [START howto_toolset_defer_loading]
    :end-before: [END howto_toolset_defer_loading]

How the model finds a hidden tool depends on the model:

- On models with native tool search, the deferred tools are sent to the provider with
  ``defer_loading`` set, and the provider keeps their definitions out of the model's
  context and searches them itself. Pydantic AI uses native search for Claude Sonnet 4.5,
  Opus 4.5 and Haiku 4.5 and the later Claude families it lists, and for GPT-5.4 and later
  through the OpenAI Responses API; its
  `tool search guide <https://pydantic.dev/docs/ai/tools-toolsets/tools-advanced/#tool-search>`__
  has the details.
- On every other model, the hidden tools are left out of the request. The model is
  offered one ``search_tools`` tool that matches keywords against tool names and
  descriptions, and the tools it finds are sent with the next request.

Pydantic AI picks the path from a list of model name prefixes in the installed version, so
a model family that version does not list gets the local search. For example, Pydantic AI
2.48 lists ``claude-haiku-4-5`` but no Haiku 5 model, so ``claude-haiku-5-5`` takes the
local path.

Either way, a tool the model has found stays available while the search that found it
is in the run's message history.

Keep the common tools visible
-----------------------------

A search costs a model request. When most runs use the same few tools, pass the names of
the tools to hide, and leave the rest in the request from the start:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_progressive_tool_disclosure.py
    :language: python
    :dedent: 4
    :start-after: [START howto_toolset_defer_loading_some]
    :end-before: [END howto_toolset_defer_loading_some]

The names are the tool names the model sees, so they include ``tool_prefix``. A name that
matches no tool is ignored without an error, so a misspelled name leaves that tool visible.

What it saves
-------------

These counts are the tokens in the tool definitions of the first model request, counted
with ``tiktoken``'s ``o200k_base`` encoding on a model without native tool search. Each
model provider counts tokens its own way, so its numbers differ somewhat.

.. list-table::
   :header-rows: 1
   :widths: 52 16 16 16

   * - Toolset
     - All tools sent
     - ``.defer_loading()``
     - ``s3_list_keys`` and ``s3_read_key`` visible
   * - The nine ``S3Hook`` methods above
     - 1,059
     - 163
     - 694
   * - 25 ``S3Hook`` methods, writes included
     - 4,609
     - 163
     - 761

The 163 tokens are ``search_tools`` itself, so the saving grows with the number of tools
hidden. On a model with native tool search there is no ``search_tools``, and the
provider keeps the deferred tools out of the model's context until they are found.

What a run looks like
---------------------

The first example, run on ``claude-haiku-5-5`` (which took the local search), searched
once, then called ``s3_list_keys`` once and ``s3_read_key`` twice. This is the
``AgentOperator`` part of its task log, with timestamps and logger names trimmed:

.. code-block:: text

    ::group::Tool call: s3_list_keys
    Tool s3_list_keys returned in 0.20s
    ::endgroup::
    ::group::Tool call: s3_read_key
    Tool s3_read_key returned in 0.08s
    ::endgroup::
    ::group::Tool call: s3_read_key
    Tool s3_read_key returned in 0.01s
    ::endgroup::
    ::group::LLM run complete: model=claude-haiku-5-5, requests=4, tool_calls=4, input_tokens=6683, output_tokens=474, total_tokens=7157
    LLM prompt cache: cache_read_tokens=3894, cache_write_tokens=2779
    Tool call sequence: search_tools -> s3_list_keys -> s3_read_key -> s3_read_key
    ::endgroup::

``search_tools`` has no ``Tool call`` group of its own, because tool call logging does not
cover tools another capability adds. It shows only in the sequence at the end of the run,
and it is one of the four tool calls counted.

The second example, with ``s3_list_keys`` and ``s3_read_key`` visible from the start, made
the same three S3 calls without a search, in one model request fewer:

.. code-block:: text

    ::group::LLM run complete: model=claude-haiku-5-5, requests=3, tool_calls=3, input_tokens=4633, output_tokens=394, total_tokens=5027
    LLM prompt cache: cache_read_tokens=2880, cache_write_tokens=1745
    Tool call sequence: s3_list_keys -> s3_read_key -> s3_read_key
    ::endgroup::

The local search matches each keyword on its own, so one search can reveal more tools than
the model goes on to use. In a run where the model searched for ``list keys read key``,
the search revealed six of the nine S3 tools, and every request after it carried all six.

What stays the same
-------------------

A tool the model found is the same tool, called the same way:

- Its result passes through Airflow's secret masker before the model sees it, as every
  tool result does (see :doc:`../agent_security`).
- Its calls are logged like any other tool's (see :doc:`logging`).
- With ``durable=True``, a retry replays the model requests and tool calls that came
  after a search; only the step that failed runs again (see :doc:`../durable_execution`).
- A templated connection ID on the toolset is rendered for each task instance, as it is
  without ``.defer_loading()``.

.. _progressive-disclosure-when-not:

When it does not help
---------------------

``SQLToolset`` has four tools whatever the database holds: 488 tokens of tool
definitions, against 163 for ``search_tools`` in their place. Hiding them saves 325
tokens a request and costs a search request in nearly every run, since the model needs
the tools to answer anything. On a warehouse with hundreds of tables, ``list_tables``
output grows with the table count; limit it with ``allowed_tables`` (see
:ref:`sql-toolset-restricted`).

``AgentSkillsToolset`` has four tools, and its list of skills goes into the agent's
instructions, which ``.defer_loading()`` does not touch. For 30 skills that list was
2,129 tokens against 907 for the tools. The toolset already loads each skill's body only
when the model asks for it.

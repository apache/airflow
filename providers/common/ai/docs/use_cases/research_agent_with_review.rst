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

Research agent with human review
================================

Someone has a question that needs a knowledge base, a dataset and a web search to answer,
and the right sequence of lookups depends on the question. This Dag runs a LangChain ReAct
agent that decides for itself which tools to call and in what order, then hands the raw
findings to a separate formatting step and to a reviewer. Airflow puts a person in front
of the agent to edit the question, exposes the findings and tool calls as XCom, makes
formatting its own retryable task, and holds the report for approval.

This is the shape for teams with existing LangChain tools. The agent loop is LangChain's;
the connection, the formatting call and the review gates are Airflow's.

What this demonstrates
----------------------

* :doc:`../hooks/langchain` -- ``LangChainHook`` supplies the chat model and embeddings
  from one Airflow connection.
* :doc:`../operators/llm` -- ``LLMOperator`` formats the findings with a Jinja template
  that reads the agent's XCom.
* :doc:`apache-airflow-providers-standard:operators/hitl` -- ``HITLEntryOperator`` to edit
  the question, ``ApprovalOperator`` to sign off the report.
* :doc:`../toolsets/langchain` -- the reverse direction, giving a LangChain agent tools
  built from Airflow connections.

Run it
------

1. Install the LangChain extra and the packages the tools use:

   .. code-block:: bash

       pip install "apache-airflow-providers-common-ai[langchain]" \
           langchain-openai langchain-text-splitters langchain-community faiss-cpu

2. Create a ``langchain`` connection named ``langchain_default`` with your API key in the
   password field (see :doc:`../connections/langchain`).

3. Optionally put documents under ``DOCS_PATH`` and the survey CSV at ``SURVEY_CSV_PATH``;
   without them the Dag writes sample pages so the tools have something to search.

4. Trigger the Dag:

   .. code-block:: bash

       airflow dags test example_langchain_tool_agent

The run pauses at ``prompt_review`` and ``report_approval``; answer from Required Actions
in the UI (see the note on :doc:`index`). The ``run_research_agent`` log shows every tool
call, and its XCom keeps the findings and call list.

The Dag
-------

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_langchain_tool_agent.py
    :language: python
    :start-after: [START example_langchain_tool_agent]
    :end-before: [END example_langchain_tool_agent]

The tools (knowledge-base search, survey query, a stubbed web search, a clock) are ordinary
LangChain ``@tool`` functions built in ``_build_tools``.

Adapting it
-----------

* Replace ``_build_tools`` with your own LangChain tools. Anything from the LangChain
  ecosystem works unchanged.
* To run the same shape on pydantic-ai instead, use :doc:`../operators/agent` with
  :doc:`../toolsets/index`; the surrounding review and formatting tasks stay as they are.
* For routine questions, drop ``prompt_review`` and take the question from a Dag param;
  keep ``report_approval``.

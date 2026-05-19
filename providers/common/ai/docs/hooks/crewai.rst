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

.. _howto/hook:crewai:

``CrewAIHook``
==============

Use :class:`~airflow.providers.common.ai.hooks.crewai.CrewAIHook`
to bridge Airflow connections to CrewAI model constructors.  The hook
extracts credentials from an Airflow connection and returns a configured
``crewai.LLM`` instance that can be passed directly to CrewAI agents.

The hook reuses the ``pydanticai`` connection type, so users configure a
single connection for PydanticAI operators, LlamaIndex tasks, LangChain
tasks, and CrewAI tasks.

.. seealso::
    :ref:`Connection configuration <howto/connection:pydanticai>`

Basic Usage
-----------

Use the hook in a ``@task`` function to get a configured LLM for CrewAI
agents:

.. code-block:: python

    from airflow.providers.common.ai.hooks.crewai import CrewAIHook

    @task
    def run_crew(topic: str) -> str:
        hook = CrewAIHook(llm_conn_id="pydanticai_default", llm_model="openai/gpt-4o")
        llm = hook.get_llm()

        from crewai import Agent, Crew, Task

        researcher = Agent(
            role="Researcher",
            goal=f"Research {topic}",
            backstory="You are an expert researcher.",
            llm=llm,
        )
        research_task = Task(
            description=f"Research the latest developments in {topic}.",
            expected_output="A summary of key findings.",
            agent=researcher,
        )
        crew = Crew(agents=[researcher], tasks=[research_task])
        result = crew.kickoff()
        return result.raw

Connection Configuration
------------------------

The hook reads credentials from the Airflow connection:

- **password** -- API key (passed as ``api_key`` to ``crewai.LLM``)
- **host** -- Base URL (passed as ``base_url``; optional, for custom
  endpoints)

Parameters
----------

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Parameter
     - Default
     - Description
   * - ``llm_conn_id``
     - ``pydanticai_default``
     - Airflow connection ID for the LLM provider.
   * - ``llm_model``
     - ``None``
     - Model name in LiteLLM format (e.g. ``openai/gpt-4o``).
       Required for ``get_llm()``.

Dependencies
------------

Install the ``crewai`` extra to use this hook::

    pip install apache-airflow-providers-common-ai[crewai]

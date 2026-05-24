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
"""Minimal example DAG showing CrewAIHook usage.

A single ``@task`` instantiates :class:`CrewAIHook`, asks for a configured
``crewai.LLM``, wires it into a one-agent ``Crew``, and returns the result.
For a richer multi-agent end-to-end demo (HITL review, mapped tickers,
report synthesis), see ``example_crewai_stock_analysis.py``.
"""

from __future__ import annotations

import datetime

from airflow.providers.common.ai.hooks.crewai import CrewAIHook
from airflow.providers.common.compat.sdk import dag, task


# [START howto_hook_crewai_basic]
@dag(schedule=None)
def example_crewai_hook():
    @task(execution_timeout=datetime.timedelta(minutes=10))
    def summarize_topic(topic: str) -> str:
        from crewai import Agent, Crew, Task

        hook = CrewAIHook(
            llm_conn_id="crewai_default",
            llm_model="openai/gpt-4o",
        )
        llm = hook.get_llm()

        researcher = Agent(
            role="Researcher",
            goal=f"Produce a concise summary of {topic}",
            backstory="You are an expert researcher who writes clearly.",
            llm=llm,
        )
        research_task = Task(
            description=f"Summarize the current state of {topic} in 3 bullet points.",
            expected_output="A 3-bullet summary in plain text.",
            agent=researcher,
        )
        crew = Crew(agents=[researcher], tasks=[research_task])
        return str(crew.kickoff().raw)

    summarize_topic("Apache Airflow's common-ai provider")


# [END howto_hook_crewai_basic]

example_crewai_hook()

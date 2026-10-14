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
"""Quickstart example: a first ``@task.llm`` Dag with a downstream task that uses the answer."""

from __future__ import annotations

# [START howto_quickstart_llm]
from airflow.sdk import dag, task

RELEASE_NOTES = """
Changes since the last release:
- Tasks can now carry a retry policy that decides whether a failure is worth retrying.
- Heartbeat writes are batched, which cut metadata database load by about a third in testing.
- Fixed a crash when two tasks in one Dag file shared a task id.
- Dropped support for Python 3.9.
"""


@dag(schedule=None, tags=["example"])
def quickstart_llm():
    @task.llm(llm_conn_id="pydanticai_default", system_prompt="You write release announcements. Be concise.")
    def summarize(notes: str):
        return f"Summarize these release notes in two sentences for a team status update:\n{notes}"

    @task
    def publish(summary: str) -> dict[str, str | int]:
        print(f"Release summary: {summary}")
        return {"summary": summary, "characters": len(summary)}

    publish(summarize(RELEASE_NOTES))


quickstart_llm()
# [END howto_quickstart_llm]

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

Use Tuning Engines as a governed AI endpoint
============================================

Airflow DAGs can call an OpenAI-compatible AI gateway from tasks while Airflow
continues to own scheduling, retries, backfills, task state, and operational
visibility. Tuning Engines can provide model routing, policy checks, approval
decisions, usage attribution, and gateway traces for those calls.

Configuration
-------------

Store the Tuning Engines inference key with your normal Airflow secrets backend
or environment management:

.. code-block:: bash

    export TE_INFERENCE_KEY=sk-te-your-inference-key
    export TE_MODEL=auto

TaskFlow DAG example
--------------------

.. code-block:: python

    from __future__ import annotations

    import os
    import uuid
    from datetime import datetime

    import httpx
    from airflow.decorators import dag, task


    def new_id(prefix: str) -> str:
        return f"{prefix}_{uuid.uuid4().hex}"


    @dag(
        dag_id="tuning_engines_governed_ai",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        tags=["ai", "tuning-engines"],
    )
    def tuning_engines_governed_ai():
        @task(retries=2)
        def governed_model_call(prompt: str) -> dict:
            run_id = new_id("airflow")
            request_id = new_id("req")
            response = httpx.post(
                "https://api.tuningengines.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {os.environ['TE_INFERENCE_KEY']}",
                    "Content-Type": "application/json",
                    "X-TE-Run-ID": run_id,
                    "X-TE-Request-ID": request_id,
                },
                timeout=60,
                json={
                    "model": os.getenv("TE_MODEL", "auto"),
                    "messages": [{"role": "user", "content": prompt}],
                    "metadata": {
                        "run_id": run_id,
                        "request_id": request_id,
                        "runtime": "airflow",
                        "event_type": "model.call",
                    },
                },
            )
            response.raise_for_status()
            return response.json()

        governed_model_call("Summarize why governed AI DAGs need retries.")


    tuning_engines_governed_ai()

Use the same ``run_id`` on any trace or state-reference calls you emit from the
DAG so Tuning Engines can correlate gateway activity with the Airflow run and
task logs.

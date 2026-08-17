<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# SandboxToolset system tests

Both tests exercise the complete toolset boundary with a deterministic pydantic-ai model:

1. an Airflow task runs a deterministic pydantic-ai agent;
2. the agent calls every sandbox tool;
3. the selected backend creates a sandbox;
4. later calls read the file written by the first call, proving per-run persistence; and
5. the agent run tears the sandbox down.

## Docker Sandboxes

Install the `sbx` CLI on every worker that can run this Dag and initialize its network policy once:

```console
sbx policy init deny-all
```

The worker must also be able to pull the default `python:3.12-slim` template. Run the test in an
Airflow system-test environment whose task process has access to the host `sbx` installation:

```console
pytest --system providers/common/ai/tests/system/common/ai/example_sandbox_toolset_sbx.py
```

## Ascii Box

Install the Ascii Box extra and export a short-lived API key into the task process:

```console
pip install "apache-airflow-providers-common-ai[sandbox-ascii-box]"
export BOX_API_KEY="..."
pytest --system providers/common/ai/tests/system/common/ai/example_sandbox_toolset_ascii_box.py
```

The test requests open egress (`SandboxSpec(block_network=False)`), because Ascii Box
cannot enforce a deny-all network policy, and a 15-minute server-side TTL.

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

# Databricks agent system test fixture

This deterministic agent uses the real `DurableAgentServer` and returns a fixed greeting, the received input and the session ID. It requires no model endpoint or external tools. A short delay in its handler exercises background invocation and polling.

The app uses the server's default in-process Runtime Store. Run it on one app instance; the test does not cover persistence or recovery across app restarts.

## Deploy the app

From the Airflow checkout, upload this directory and deploy it as a Databricks App using an authenticated Databricks CLI profile:

```bash
databricks workspace import-dir \
  providers/databricks/tests/system/databricks/resources/agent_server \
  /Workspace/Shared/airflow-system-tests/agent-server --profile YOUR_PROFILE
databricks apps create airflow-system-test-agent --profile YOUR_PROFILE
databricks apps deploy airflow-system-test-agent \
  --source-code-path /Workspace/Shared/airflow-system-tests/agent-server \
  --profile YOUR_PROFILE
```

Wait until compute is `ACTIVE` and app status is `RUNNING`. Grant the caller service principal workspace access and `CAN_USE` on the app.

## Run the system test

Create a private Airflow connections JSON file at `files/databricks-agent-connections.json` containing a `databricks_oauth` connection: connection type `databricks`, workspace URL in `host`, OAuth client ID in `login`, client secret in `password`, and `{"service_principal_oauth": true}` in extras.

Create `files/databricks-agent-system-test.env` with the deployed app's base URL and connection settings. Breeze mounts `files/` at `/files`:

```bash
DATABRICKS_AGENT_APP_URL=https://YOUR_APP_HOST
DATABRICKS_AGENT_CONN_ID=databricks_oauth
DATABRICKS_AGENT_CONN_FILE=/files/databricks-agent-connections.json
```

Run:

```bash
SYSTEM_TESTS_ENV_ID=your-unique-id \
BREEZE_INIT_COMMAND='set -a; . /files/databricks-agent-system-test.env; set +a' \
breeze testing system-tests \
  --backend sqlite \
  --forward-credentials \
  --test-timeout 2400 \
  providers/databricks/tests/system/databricks/example_databricks_agent.py \
  -q
```

The test invokes the existing app in normal and deferrable modes and checks its output and session ID. It does not create or delete the deployment.

## Clean up

Stop app compute after the test:

```bash
databricks apps stop airflow-system-test-agent --profile YOUR_PROFILE
```

Delete the app when it is no longer needed with `databricks apps delete airflow-system-test-agent --profile YOUR_PROFILE`.

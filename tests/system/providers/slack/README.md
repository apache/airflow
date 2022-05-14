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

# Slack provider system tests

## Tests structure

All Slack-related system tests are located inside this subdirectory of system tests which is
`tests/system/providers/slack/`. At this point, the file structre is flat but it can change once
more and more tests are added.

Each test file is as self-contained DAG (one DAG per file) and potentally each test may require some additional
resources which should be placed in `resources` directory found on the same level as tests.

Each test file should start with prefix `example_*`. If there
is anything more needed for the test to be executed, it should be documented in the docstrings.

## Initial configuration

To be able to run Slack system test(s) you need to define AIRFLOW_CONN_SLACK_DEFAULT and SYSTEM_TESTS_ENV_ID variables.

Example how these variables could be confiured are provided below:

```commandline
export AIRFLOW_CONN_SLACK_DEFAULT='slack://:xoxb-ABCDABCDABCDABCD@'
export SYSTEM_TESTS_ENV_ID="abcd"
```
Once you confiure these variables you can run a slack system test in the following way

```commandline
pytest --system=slack tests/system/providers/slack/example_slack.py

```

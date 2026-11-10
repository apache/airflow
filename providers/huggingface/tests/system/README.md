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

# Hugging Face system tests

Run the DAG system tests with pytest from the repository root in an Airflow
system-test environment:

```bash
pytest tests/system/huggingface
```

The read-only test needs no Hugging Face credentials. The publishing test
publishes temporary private datasets under the ``airflow-hf`` organization by
default, so configure an Airflow connection named ``huggingface_default`` with
a write-scoped Hub token before running it. Set ``HF_TEST_HUB_ORG`` or
``HF_TEST_HUB_CONN_ID`` to override the defaults. The publishing DAG also
checks retry reuse of a successful Hub commit on Airflow 3.3 or later; that
branch is omitted on earlier versions.

The system-test DAGs use ``get_test_run(dag)`` so pytest can execute their task
graphs as Airflow system tests. Repository ID and run ID environment overrides
are documented in the provider docs.

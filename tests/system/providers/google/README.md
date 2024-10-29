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

# Google provider system tests

## Tests structure

All Google-related system tests are located inside this subdirectory of system tests which is
`tests/system/providers/google/`. They are grouped in directories by the related service name, e.g. all BigQuery
tests are stored inside `tests/system/providers/google/cloud/bigquery/` directory. In each directory you will find test files
as self-contained Dags (one DAG per file). Each test may require some additional resources which should be placed in
`resources` directory found on the same level as tests. Each test file should start with prefix `example_*`. If there
is anything more needed for the test to be executed, it should be documented in the docstrings.

Example files structure:

```
tests/system/providers/google
├── bigquery
│   ├── resources
│   │   ├── example_bigquery_query.sql
│   │   └── us-states.csv
│   ├── example_bigquery_queries.py
│   ├── example_bigquery_operations.py
.   .
│   └── example_bigquery_*.py
├── dataflow
├── gcs
.
└── *
```

## Initial configuration

Each test requires some environment variables. Check how to set them up on your operating system, but on UNIX-based
OSes this should work:

```commandline
export NAME_OF_ENV_VAR=value
```

To confirm that it is set up correctly, run `echo $NAME_OF_ENV_VAR` which will display its value.

### Required environment variables

- `SYSTEM_TESTS_GCP_PROJECT` - GCP project name that will be used to run system tests (this can be checked on the UI
  dashboard of the GCP or by running `gcloud config list`).

- `SYSTEM_TESTS_ENV_ID` - environment ID that is unique across different executions of system tests (if they
  are run in parallel). The need for this variable comes from the possibility, that the tests may be run on various
  versions of Airflow at the same time using CI environment. If this is the case, the value of this variable ensures
  that all resources that are created during the tests, will not interfere with other resources in the same project,
  that can be created at the same time.

  If you run your tests in parallel, make sure to set this variable with randomized generated value, preferably no
  longer than 8 characters (which should be fine to avoid collisions). Otherwise, please put whatever value you want,
  but use only lowercase letters A-Z and digits 0-9.

  The value of this environment variable is commonly attached to other variables like bucket names so it needs to
  follow the same guidelines as bucket names, which can be found
  [here](https://cloud.google.com/storage/docs/naming-buckets#requirements).

### GCP Project setup

System tests are executed in a working environment, in this case a GCP project that is deposited with credits. Such
project is prepared and dedicated for the exclusive use of the Airflow Community, especially for the CI integrated
system tests. If you want to run the tests manually, you need to have your own GCP project and configure it properly.

Tests need to know which GCP project to use to execute them. If your environment is not already configured to use
particular GCP project (like Airflow running in GCP Composer) you need to configure it using
[gcloud CLI](https://cloud.google.com/sdk/gcloud). Use your project ID to execute this command:

```commandline
gcloud config set project <project-id>
```

Keep in mind that some additional commands may be required.


## Settings for specific tests

Some tests may require extra setup. If this is the case, the steps should be documented inside the docstring of
related test file.

## Dashboard

To check the status of the system tests against the head revision of Apache Airflow, please refer to this [dashboard](https://storage.googleapis.com/providers-dashboard-html/dashboard.html).

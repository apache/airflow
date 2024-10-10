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

# Amazon provider system tests

## Tests structure

All AWS-related system tests are located inside `tests/system/providers/amazon/aws/`.
In this directory you will find test files in the form of Example DAGs, one DAG per file.
Each test should be self-contained but in the case where additional resources are required,
they can be found in the `resources` directory on the same level as tests or noted in the
test's docstring.  Each test file should start with prefix `example_*`.

Example directory structure:

```
tests/system/providers/amazon/aws
├── resources
│   ├── example_athena_data.csv
│   └── example_sagemaker_constants.py
├── example_athena.py
├── example_batch.py
.
├── example_step_functions.py
└── *
```

## Initial configuration

Each test may require some environment variables. Check how to set them up on your
operating system, but on UNIX-based operating systems `export NAME_OF_ENV_VAR=value`
should work.  To confirm that it is set up correctly, run `echo $NAME_OF_ENV_VAR`
which will display its value.

When manually running tests using pytest, you can define them inline with the command.
For example:

```commandline
NAME_OF_ENV_VAR=value pytest --system amazon tests/system/providers/amazon/aws/example_test.py
```

### Required environment variables

- `SYSTEM_TESTS_ENV_ID` - AWS System Tests use `SystemTestContextBuilder` to generate
and export this value if one does not exist.

An environment ID is a unique value across multiple/different executions of a system
test.  It is required because the CI environment may run the tests on various versions
of Airflow in parallel.  If this is the case, the value of this variable ensures that
resources that are created during the test execution do not interfere with each other.

The value is used as part of the name for resources which have different requirements.
For example, an S3 bucket name can not use underscores whereas an Athena table name can
not use hyphens.  In order to minimize conflicts, this variable should be a randomized
value using only lowercase letters A-Z and digits 0-9, and start with a letter.

## Settings for specific tests

Amazon system test files are designed to be as self-contained as possible.  They will contain
any sample data and configuration values which are required, and they will create and tear
down any required infrastructure.  Some tests will require an IAM Role ARN or other external
variables that can not be created by the test itself, and the requirements for those values
should be documented inside the docstring of the test file.

### Contributing

Please see [CONTRIBUTING.md](CONTRIBUTING.md) for community guidelines and best practices for
writing AWS System Tests for Apache Airflow.

## Dashboard

You can see the latest health status of all AWS-related system tests in
[this dashboard](https://aws-mwaa.github.io/open-source/system-tests/dashboard.html). In this dashboard are listed all
AWS-related system tests and some statistics about the last executions, such as: number of invocations, number of
successes, number of failures and average duration. You can also see the status (succeed/failed) of the last 10
executions for each system test.

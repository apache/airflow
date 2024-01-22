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

# Airflow System Tests

- [How to run system tests](#how_to_run)
  - [Running via Airflow](#run_via_airflow)
  - [Running via Pytest](#run_via_pytest)
  - [Running via Airflow CLI](#run_via_airflow_cli)
- [How to write system tests](#how_to_write)

System tests verify the correctness of Airflow Operators by running them in DAGs and allowing to communicate with
external services. A system test tries to look as close to a regular DAG as possible, and it generally checks the
"happy path" (a scenario featuring no errors) ensuring that the Operator works as expected.

The purpose of these tests is to:

- assure high quality of providers and their integration with Airflow core,
- avoid regression in providers when doing changes to the Airflow,
- autogenerate documentation for Operators from code,
- provide runnable example DAGs with use cases for different Operators,
- serve both as examples and test files.

> This is the new design of system tests which temporarily exists along with the old one documented at
> [testing.rst](../../contribution-docs/testing.rst) and soon will completely replace it. The new design is based on the
> [AIP-47](https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-47+New+design+of+Airflow+System+Tests).
> Please use it and write any new system tests according to this documentation.

## How to run system tests <a name="how_to_run"></a>

There are multiple ways of running system tests. Each system test is a self-contained DAG, so it can be run as any
other DAG. Some tests may require access to external services, enabled APIs or specific permissions. Make sure to
prepare your  environment correctly, depending on the system tests you want to run - some may require additional
configuration which should be documented by the relevant providers in their subdirectory
`tests/system/providers/<provider_name>/README.md`.

### Running via Airflow <a name="run_via_airflow"></a>

If you have a working Airflow environment with a scheduler and a webserver, you can import system test files into
your Airflow instance and they will be automatically triggered. If the setup of the environment is correct
(depending on the type of tests you want to run), they should be executed without any issues. The instructions on
how to set up the environment is documented in each provider's system tests directory. Make sure that all resource
required by the tests are also imported.

### Running via Pytest <a name="run_via_pytest"></a>

Running system tests with pytest is the easiest with Breeze. Thanks to it, you don't need to bother about setting up
the correct environment, that is able to execute the tests.
You can either run them using your IDE (if you have installed plugin/widget supporting pytest) or using the following
example of command:

```commandline
# pytest --system [provider_name] [path_to_test(s)]
pytest --system google tests/system/providers/google/cloud/bigquery/example_bigquery_queries.py
```

You can specify several `--system` flags if you want to execute tests for several providers:

```commandline
pytest --system google --system aws tests/system
```

### Running via Airflow CLI <a name="run_via_airflow_cli"></a>

It is possible to run system tests using Airflow CLI. To execute a specific system test, you need to provide
`dag_id` of the test to be run, `execution_date` (preferably the one from the past) and a `-S/--subdir` option
followed by the path where the tests are stored (the command by default looks into `$AIRFLOW_HOME/dags`):

```commandline
# airflow dags test -S [path_to_tests] [dag_id] [execution date]
airflow dags test -S tests/system bigquery_dataset 2022-01-01
```

> Some additional setup may be required to use Airflow CLI. Please refer
> [here](https://airflow.apache.org/docs/apache-airflow/stable/usage-cli.html) for a documentation.


## How to write system tests <a name="how_to_write"></a>

If you are going to implement new system tests, it is recommended to familiarize with the content of the
[AIP-47](https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-47+New+design+of+Airflow+System+Tests). There are many changes in comparison to the old design documented at
[testing.rst](../../contribution-docs/testing.rst), so you need to be aware of them and be compliant with
the new design.

To make it easier to migrate old system tests or write new ones, we
documented the whole **process of migration in details** (which can be found
[here](https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-47+New+design+of+Airflow+System+Tests#AIP47NewdesignofAirflowSystemTests-Processofmigrationindetails))
and also prepared an example of a test (located just below the migration details).

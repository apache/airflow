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


# Package apache-airflow-backport-providers-databricks

Release: 2020.05.19

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-class-summary)
    - [Operators](#operators)
        - [Moved operators](#moved-operators)
    - [Hooks](#hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 2020.05.19](#release-20200519)

## Backport package

This is a backport providers package for `databricks` provider. All classes for this provider package
are in `airflow.providers.databricks` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-databricks`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| requests      | &gt;=2.20.0, &lt;3       |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.databricks` package.


## Operators




### Moved operators

| Airflow 2.0 operators: `airflow.providers.databricks` package                                                                                          | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                              |
|:-------------------------------------------------------------------------------------------------------------------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.databricks.DatabricksRunNowOperator](https://github.com/apache/airflow/blob/master/airflow/providers/databricks/operators/databricks.py)    | [contrib.operators.databricks_operator.DatabricksRunNowOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/databricks_operator.py)    |
| [operators.databricks.DatabricksSubmitRunOperator](https://github.com/apache/airflow/blob/master/airflow/providers/databricks/operators/databricks.py) | [contrib.operators.databricks_operator.DatabricksSubmitRunOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/databricks_operator.py) |





## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.databricks` package                                                                         | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                 |
|:----------------------------------------------------------------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------|
| [hooks.databricks.DatabricksHook](https://github.com/apache/airflow/blob/master/airflow/providers/databricks/hooks/databricks.py) | [contrib.hooks.databricks_hook.DatabricksHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/databricks_hook.py) |






## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                                                                                                            |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [649935e8c](https://github.com/apache/airflow/commit/649935e8ce906759fdd08884ab1e3db0a03f6953) | 2020-04-27  | [AIRFLOW-8472]: `PATCH` for Databricks hook `_do_api_call` (#8473)                                                                                                 |
| [16903ba3a](https://github.com/apache/airflow/commit/16903ba3a6ee5e61f1c6b5d17a8c6cf3c3a9a7f6) | 2020-04-24  | [AIRFLOW-8474]: Adding possibility to get job_id from Databricks run (#8475)                                                                                       |
| [5648dfbc3](https://github.com/apache/airflow/commit/5648dfbc300337b10567ef4e07045ea29d33ec06) | 2020-03-23  | Add missing call to Super class in &#39;amazon&#39;, &#39;cloudant &amp; &#39;databricks&#39; providers (#7827)                                                                            |
| [3320e432a](https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc) | 2020-02-24  | [AIRFLOW-6817] Lazy-load `airflow.DAG` to keep user-facing API untouched (#7517)                                                                                   |
| [4d03e33c1](https://github.com/apache/airflow/commit/4d03e33c115018e30fa413c42b16212481ad25cc) | 2020-02-22  | [AIRFLOW-6817] remove imports from `airflow/__init__.py`, replaced implicit imports with explicit imports, added entry to `UPDATING.MD` - squashed/rebased (#7456) |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                                                                                                           |
| [83c037873](https://github.com/apache/airflow/commit/83c037873ff694eed67ba8b30f2d9c88b2c7c6f2) | 2020-01-30  | [AIRFLOW-6674] Move example_dags in accordance with AIP-21 (#7287)                                                                                                 |
| [c42a375e7](https://github.com/apache/airflow/commit/c42a375e799e5adb3f9536616372dc90ff47e6c8) | 2020-01-27  | [AIRFLOW-6644][AIP-21] Move service classes to providers package (#7265)                                                                                           |

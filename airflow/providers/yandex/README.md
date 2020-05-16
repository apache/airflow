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


# Package apache-airflow-backport-providers-yandex

Release: 2020.05.19

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-class-summary)
    - [Operators](#operators)
        - [New operators](#new-operators)
    - [Hooks](#hooks)
        - [New hooks](#new-hooks)
- [Releases](#releases)
    - [Release 2020.05.19](#release-20200519)

## Backport package

This is a backport providers package for `yandex` provider. All classes for this provider package
are in `airflow.providers.yandex` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-yandex`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| yandexcloud   | &gt;=0.22.0           |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.yandex` package.


## Operators


### New operators

| New Airflow 2.0 operators: `airflow.providers.yandex` package                                                                                                                 |
|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.yandexcloud_dataproc.DataprocCreateClusterOperator](https://github.com/apache/airflow/blob/master/airflow/providers/yandex/operators/yandexcloud_dataproc.py)      |
| [operators.yandexcloud_dataproc.DataprocCreateHiveJobOperator](https://github.com/apache/airflow/blob/master/airflow/providers/yandex/operators/yandexcloud_dataproc.py)      |
| [operators.yandexcloud_dataproc.DataprocCreateMapReduceJobOperator](https://github.com/apache/airflow/blob/master/airflow/providers/yandex/operators/yandexcloud_dataproc.py) |
| [operators.yandexcloud_dataproc.DataprocCreatePysparkJobOperator](https://github.com/apache/airflow/blob/master/airflow/providers/yandex/operators/yandexcloud_dataproc.py)   |
| [operators.yandexcloud_dataproc.DataprocCreateSparkJobOperator](https://github.com/apache/airflow/blob/master/airflow/providers/yandex/operators/yandexcloud_dataproc.py)     |
| [operators.yandexcloud_dataproc.DataprocDeleteClusterOperator](https://github.com/apache/airflow/blob/master/airflow/providers/yandex/operators/yandexcloud_dataproc.py)      |







## Hooks


### New hooks

| New Airflow 2.0 hooks: `airflow.providers.yandex` package                                                                                       |
|:------------------------------------------------------------------------------------------------------------------------------------------------|
| [hooks.yandex.YandexCloudBaseHook](https://github.com/apache/airflow/blob/master/airflow/providers/yandex/hooks/yandex.py)                      |
| [hooks.yandexcloud_dataproc.DataprocHook](https://github.com/apache/airflow/blob/master/airflow/providers/yandex/hooks/yandexcloud_dataproc.py) |







## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                                                                                                            |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807)                                                                                            |
| [59a4f2669](https://github.com/apache/airflow/commit/59a4f26699125b1594496940d62be78d7732b4be) | 2020-04-17  | stop rendering some class docs in wrong place (#8095)                                                                                                              |
| [3320e432a](https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc) | 2020-02-24  | [AIRFLOW-6817] Lazy-load `airflow.DAG` to keep user-facing API untouched (#7517)                                                                                   |
| [4d03e33c1](https://github.com/apache/airflow/commit/4d03e33c115018e30fa413c42b16212481ad25cc) | 2020-02-22  | [AIRFLOW-6817] remove imports from `airflow/__init__.py`, replaced implicit imports with explicit imports, added entry to `UPDATING.MD` - squashed/rebased (#7456) |
| [9cbd7de6d](https://github.com/apache/airflow/commit/9cbd7de6d115795aba8bfb8addb060bfdfbdf87b) | 2020-02-18  | [AIRFLOW-6792] Remove _operator/_hook/_sensor in providers package and add tests (#7412)                                                                           |
| [ee1ab7697](https://github.com/apache/airflow/commit/ee1ab7697c6106b7107b285d8fe9ad01766dc19e) | 2020-02-14  | [AIRFLOW-6531] Initial Yandex.Cloud Dataproc support (#7252)                                                                                                       |

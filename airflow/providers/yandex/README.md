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

Release: 2020.5.20

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
    - [Release 2020.5.20](#release-2020520)

## Backport package

This is a backport providers package for `yandex` provider. All classes for this provider package
are in `airflow.providers.yandex` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.



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

### Release 2020.5.20

| Commit                                                                                         | Committed   | Subject                                                                                                                                                            |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [0b0e4f7a4](https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34) | 2020-05-26  | Preparing for RC3 relase of backports (#9026)                                                                                                                      |
| [00642a46d](https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c) | 2020-05-26  | Fixed name of 20 remaining wrongly named operators. (#8994)                                                                                                        |
| [1d36b0303](https://github.com/apache/airflow/commit/1d36b0303b8632fce6de78ca4e782ae26ee06fea) | 2020-05-23  | Fix references in docs (#8984)                                                                                                                                     |
| [375d1ca22](https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f) | 2020-05-19  | Release candidate 2 for backport packages 2020.05.20 (#8898)                                                                                                       |
| [12c5e5d8a](https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79) | 2020-05-17  | Prepare release candidate for backport packages (#8891)                                                                                                            |
| [f3521fb0e](https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca) | 2020-05-16  | Regenerate readme files for backport package release (#8886)                                                                                                       |
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807)                                                                                            |
| [59a4f2669](https://github.com/apache/airflow/commit/59a4f26699125b1594496940d62be78d7732b4be) | 2020-04-17  | stop rendering some class docs in wrong place (#8095)                                                                                                              |
| [3320e432a](https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc) | 2020-02-24  | [AIRFLOW-6817] Lazy-load `airflow.DAG` to keep user-facing API untouched (#7517)                                                                                   |
| [4d03e33c1](https://github.com/apache/airflow/commit/4d03e33c115018e30fa413c42b16212481ad25cc) | 2020-02-22  | [AIRFLOW-6817] remove imports from `airflow/__init__.py`, replaced implicit imports with explicit imports, added entry to `UPDATING.MD` - squashed/rebased (#7456) |
| [9cbd7de6d](https://github.com/apache/airflow/commit/9cbd7de6d115795aba8bfb8addb060bfdfbdf87b) | 2020-02-18  | [AIRFLOW-6792] Remove _operator/_hook/_sensor in providers package and add tests (#7412)                                                                           |
| [ee1ab7697](https://github.com/apache/airflow/commit/ee1ab7697c6106b7107b285d8fe9ad01766dc19e) | 2020-02-14  | [AIRFLOW-6531] Initial Yandex.Cloud Dataproc support (#7252)                                                                                                       |

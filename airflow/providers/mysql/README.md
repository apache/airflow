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


# Package apache-airflow-backport-providers-mysql

Release: 2020-06-23

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Cross provider package dependencies](#cross-provider-package-dependencies)
- [Provider class summary](#provider-class-summary)
    - [Operators](#operators)
        - [Moved operators](#moved-operators)
    - [Transfer operators](#transfers)
        - [New transfer operators](#new-transfers)
        - [Moved transfer operators](#moved-transfers)
    - [Hooks](#hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 2020-06-23](#release-2020-06-23)

## Backport package

This is a backport providers package for `mysql` provider. All classes for this provider package
are in `airflow.providers.mysql` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.



## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-mysql`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package            | Version required   |
|:-----------------------|:-------------------|
| mysql-connector-python | &gt;=8.0.11, &lt;=8.0.18 |
| mysqlclient            | &gt;=1.3.6,&lt;1.4       |

## Cross provider package dependencies

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified backport providers package in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

```bash
pip install apache-airflow-backport-providers-mysql[amazon]
```

| Dependent package                                                                                                    | Extra   |
|:---------------------------------------------------------------------------------------------------------------------|:--------|
| [apache-airflow-backport-providers-amazon](https://github.com/apache/airflow/tree/master/airflow/providers/amazon)   | amazon  |
| [apache-airflow-backport-providers-presto](https://github.com/apache/airflow/tree/master/airflow/providers/presto)   | presto  |
| [apache-airflow-backport-providers-vertica](https://github.com/apache/airflow/tree/master/airflow/providers/vertica) | vertica |

# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `mysql` provider
are in the `airflow.providers.mysql` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)


## Operators




### Moved operators

| Airflow 2.0 operators: `airflow.providers.mysql` package                                                                  | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                      |
|:--------------------------------------------------------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------|
| [operators.mysql.MySqlOperator](https://github.com/apache/airflow/blob/master/airflow/providers/mysql/operators/mysql.py) | [operators.mysql_operator.MySqlOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/operators/mysql_operator.py) |





### New transfer operators

| New Airflow 2.0 transfers: `airflow.providers.mysql` package                                                                              |
|:------------------------------------------------------------------------------------------------------------------------------------------|
| [transfers.s3_to_mysql.S3ToMySqlOperator](https://github.com/apache/airflow/blob/master/airflow/providers/mysql/transfers/s3_to_mysql.py) |



### Moved transfer operators

| Airflow 2.0 transfers: `airflow.providers.mysql` package                                                                                                 | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                   |
|:---------------------------------------------------------------------------------------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [transfers.presto_to_mysql.PrestoToMySqlOperator](https://github.com/apache/airflow/blob/master/airflow/providers/mysql/transfers/presto_to_mysql.py)    | [operators.presto_to_mysql.PrestoToMySqlTransfer](https://github.com/apache/airflow/blob/v1-10-stable/airflow/operators/presto_to_mysql.py)                    |
| [transfers.vertica_to_mysql.VerticaToMySqlOperator](https://github.com/apache/airflow/blob/master/airflow/providers/mysql/transfers/vertica_to_mysql.py) | [contrib.operators.vertica_to_mysql.VerticaToMySqlTransfer](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/vertica_to_mysql.py) |





## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.mysql` package                                                          | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                  |
|:--------------------------------------------------------------------------------------------------------------|:--------------------------------------------------------------------------------------------------------------|
| [hooks.mysql.MySqlHook](https://github.com/apache/airflow/blob/master/airflow/providers/mysql/hooks/mysql.py) | [hooks.mysql_hook.MySqlHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/hooks/mysql_hook.py) |






## Releases

### Release 2020-06-23

| Commit                                                                                         | Committed   | Subject                                                                                     |
|:-----------------------------------------------------------------------------------------------|:------------|:--------------------------------------------------------------------------------------------|
| [c7e5bce57](https://github.com/apache/airflow/commit/c7e5bce57fe7f51cefce4f8a41ce408ac5675d13) | 2020-06-19  | Prepare backport release candidate for 2020.6.23rc1 (#9370)                                 |
| [f6bd817a3](https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac) | 2020-06-16  | Introduce &#39;transfers&#39; packages (#9320)                                                      |
| [0b0e4f7a4](https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34) | 2020-05-26  | Preparing for RC3 relase of backports (#9026)                                               |
| [00642a46d](https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c) | 2020-05-26  | Fixed name of 20 remaining wrongly named operators. (#8994)                                 |
| [1d36b0303](https://github.com/apache/airflow/commit/1d36b0303b8632fce6de78ca4e782ae26ee06fea) | 2020-05-23  | Fix references in docs (#8984)                                                              |
| [375d1ca22](https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f) | 2020-05-19  | Release candidate 2 for backport packages 2020.05.20 (#8898)                                |
| [12c5e5d8a](https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79) | 2020-05-17  | Prepare release candidate for backport packages (#8891)                                     |
| [f3521fb0e](https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca) | 2020-05-16  | Regenerate readme files for backport package release (#8886)                                |
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807)                     |
| [68d1714f2](https://github.com/apache/airflow/commit/68d1714f296989b7aad1a04b75dc033e76afb747) | 2020-04-04  | [AIRFLOW-6822] AWS hooks should cache boto3 client (#7541)                                  |
| [329e6a5f7](https://github.com/apache/airflow/commit/329e6a5f72bc2e3fc19391754256d974179a6ce0) | 2020-04-01  | [AIRFLOW-5907] Add S3 to MySql Operator (#6578)                                             |
| [4bde99f13](https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a) | 2020-03-23  | Make airflow/providers pylint compatible (#7802)                                            |
| [b39468d28](https://github.com/apache/airflow/commit/b39468d2878554ba60863656364b4a95eda30685) | 2020-03-09  | [AIRFLOW-5922] Add option to specify the mysql client library used in MySqlHook (#6576)     |
| [9cbd7de6d](https://github.com/apache/airflow/commit/9cbd7de6d115795aba8bfb8addb060bfdfbdf87b) | 2020-02-18  | [AIRFLOW-6792] Remove _operator/_hook/_sensor in providers package and add tests (#7412)    |
| [94fccca97](https://github.com/apache/airflow/commit/94fccca97030ee59d89f302a98137b17e7b01a33) | 2020-02-04  | [AIRFLOW-XXXX] Add pre-commit check for utf-8 file encoding (#7347)                         |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                                    |
| [1e576f123](https://github.com/apache/airflow/commit/1e576f12343b30c2a37ab3f4f62ee3aa30326e77) | 2020-02-02  | [AIRFLOW-6680] Last changes for AIP-21 (#7301)                                              |
| [057f3ae3a](https://github.com/apache/airflow/commit/057f3ae3a4afedf6d462ecf58b01dd6304d3e135) | 2020-01-29  | [AIRFLOW-6670][depends on AIRFLOW-6669] Move contrib operators to providers package (#7286) |
| [82c0e5aff](https://github.com/apache/airflow/commit/82c0e5aff6004f636b98e207c3caec40b403fbbe) | 2020-01-28  | [AIRFLOW-6655] Move AWS classes to providers (#7271)                                        |
| [eee34ee80](https://github.com/apache/airflow/commit/eee34ee8080bb7bc81294c3fbd8be93bbf795367) | 2020-01-24  | [AIRFLOW-4204] Update super() calls (#7248)                                                 |
| [059eda05f](https://github.com/apache/airflow/commit/059eda05f82fefce4410f44f761f945a27d83daf) | 2020-01-21  | [AIRFLOW-6610] Move software classes to providers package (#7231)                           |

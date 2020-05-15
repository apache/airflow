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


# Package apache-airflow-backport-providers-postgres

Release: 2020.05.19

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Cross provider package dependencies](#cross-provider-package-dependencies)
- [Provider class summary](#provider-class-summary)
    - [Operators](#operators)
        - [Moved operators](#moved-operators)
    - [Hooks](#hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 2020.05.19](#release-20200519)

## Backport package

This is a backport providers package for `postgres` provider. All classes for this provider package
are in `airflow.providers.postgres` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-postgres`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package     | Version required   |
|:----------------|:-------------------|
| psycopg2-binary | &gt;=2.7.4            |

## Cross provider package dependencies

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified backport providers package in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

```bash
pip install apache-airflow-backport-providers-postgres[amazon]
```

| Dependent package                                                                                                  | Extra   |
|:-------------------------------------------------------------------------------------------------------------------|:--------|
| [apache-airflow-backport-providers-amazon](https://github.com/apache/airflow/tree/master/airflow/providers/amazon) | amazon  |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.postgres` package.


## Operators




### Moved operators

| Airflow 2.0 operators: `airflow.providers.postgres` package                                                                           | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                               |
|:--------------------------------------------------------------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.postgres.PostgresOperator](https://github.com/apache/airflow/blob/master/airflow/providers/postgres/operators/postgres.py) | [operators.postgres_operator.PostgresOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/operators/postgres_operator.py) |





## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.postgres` package                                                                   | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                           |
|:--------------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------|
| [hooks.postgres.PostgresHook](https://github.com/apache/airflow/blob/master/airflow/providers/postgres/hooks/postgres.py) | [hooks.postgres_hook.PostgresHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/hooks/postgres_hook.py) |






## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                                  |
|:-----------------------------------------------------------------------------------------------|:------------|:-----------------------------------------------------------------------------------------|
| [a28c66f23](https://github.com/apache/airflow/commit/a28c66f23d373cd0f8bfc765a515f21d4b66a0e9) | 2020-04-30  | [AIRFLOW-4734] Upsert functionality for PostgresHook.insert_rows() (#8625)               |
| [68d1714f2](https://github.com/apache/airflow/commit/68d1714f296989b7aad1a04b75dc033e76afb747) | 2020-04-04  | [AIRFLOW-6822] AWS hooks should cache boto3 client (#7541)                               |
| [4bde99f13](https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a) | 2020-03-23  | Make airflow/providers pylint compatible (#7802)                                         |
| [9cbd7de6d](https://github.com/apache/airflow/commit/9cbd7de6d115795aba8bfb8addb060bfdfbdf87b) | 2020-02-18  | [AIRFLOW-6792] Remove _operator/_hook/_sensor in providers package and add tests (#7412) |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                                 |
| [82c0e5aff](https://github.com/apache/airflow/commit/82c0e5aff6004f636b98e207c3caec40b403fbbe) | 2020-01-28  | [AIRFLOW-6655] Move AWS classes to providers (#7271)                                     |
| [059eda05f](https://github.com/apache/airflow/commit/059eda05f82fefce4410f44f761f945a27d83daf) | 2020-01-21  | [AIRFLOW-6610] Move software classes to providers package (#7231)                        |

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


# Package apache-airflow-backport-providers-oracle

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

This is a backport providers package for `oracle` provider. All classes for this provider package
are in `airflow.providers.oracle` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-oracle`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| cx_Oracle     | &gt;=5.1.2            |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.oracle` package.


## Operators




### Moved operators

| Airflow 2.0 operators: `airflow.providers.oracle` package                                                                                                                   | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                                     |
|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.oracle.OracleOperator](https://github.com/apache/airflow/blob/master/airflow/providers/oracle/operators/oracle.py)                                               | [operators.oracle_operator.OracleOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/operators/oracle_operator.py)                                             |
| [operators.oracle_to_oracle_transfer.OracleToOracleTransfer](https://github.com/apache/airflow/blob/master/airflow/providers/oracle/operators/oracle_to_oracle_transfer.py) | [contrib.operators.oracle_to_oracle_transfer.OracleToOracleTransfer](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/oracle_to_oracle_transfer.py) |





## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.oracle` package                                                             | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                     |
|:------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------|
| [hooks.oracle.OracleHook](https://github.com/apache/airflow/blob/master/airflow/providers/oracle/hooks/oracle.py) | [hooks.oracle_hook.OracleHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/hooks/oracle_hook.py) |






## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                                     |
|:-----------------------------------------------------------------------------------------------|:------------|:--------------------------------------------------------------------------------------------|
| [4bde99f13](https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a) | 2020-03-23  | Make airflow/providers pylint compatible (#7802)                                            |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                                    |
| [057f3ae3a](https://github.com/apache/airflow/commit/057f3ae3a4afedf6d462ecf58b01dd6304d3e135) | 2020-01-29  | [AIRFLOW-6670][depends on AIRFLOW-6669] Move contrib operators to providers package (#7286) |
| [059eda05f](https://github.com/apache/airflow/commit/059eda05f82fefce4410f44f761f945a27d83daf) | 2020-01-21  | [AIRFLOW-6610] Move software classes to providers package (#7231)                           |

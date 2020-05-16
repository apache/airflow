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


# Package apache-airflow-backport-providers-apache-druid

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

This is a backport providers package for `apache.druid` provider. All classes for this provider package
are in `airflow.providers.apache.druid` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-apache-druid`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| pydruid       | &gt;=0.4.1,&lt;=0.5.8    |

## Cross provider package dependencies

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified backport providers package in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

```bash
pip install apache-airflow-backport-providers-apache-druid[apache.hive]
```

| Dependent package                                                                                                            | Extra       |
|:-----------------------------------------------------------------------------------------------------------------------------|:------------|
| [apache-airflow-backport-providers-apache-hive](https://github.com/apache/airflow/tree/master/airflow/providers/apache/hive) | apache.hive |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.apache.druid` package.


## Operators




### Moved operators

| Airflow 2.0 operators: `airflow.providers.apache.druid` package                                                                                        | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                       |
|:-------------------------------------------------------------------------------------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.druid.DruidOperator](https://github.com/apache/airflow/blob/master/airflow/providers/apache/druid/operators/druid.py)                       | [contrib.operators.druid_operator.DruidOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/druid_operator.py)  |
| [operators.druid_check.DruidCheckOperator](https://github.com/apache/airflow/blob/master/airflow/providers/apache/druid/operators/druid_check.py)      | [operators.druid_check_operator.DruidCheckOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/operators/druid_check_operator.py) |
| [operators.hive_to_druid.HiveToDruidTransfer](https://github.com/apache/airflow/blob/master/airflow/providers/apache/druid/operators/hive_to_druid.py) | [operators.hive_to_druid.HiveToDruidTransfer](https://github.com/apache/airflow/blob/v1-10-stable/airflow/operators/hive_to_druid.py)              |





## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.apache.druid` package                                                               | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                       |
|:--------------------------------------------------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------|
| [hooks.druid.DruidDbApiHook](https://github.com/apache/airflow/blob/master/airflow/providers/apache/druid/hooks/druid.py) | [hooks.druid_hook.DruidDbApiHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/hooks/druid_hook.py) |
| [hooks.druid.DruidHook](https://github.com/apache/airflow/blob/master/airflow/providers/apache/druid/hooks/druid.py)      | [hooks.druid_hook.DruidHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/hooks/druid_hook.py)      |






## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                                     |
|:-----------------------------------------------------------------------------------------------|:------------|:--------------------------------------------------------------------------------------------|
| [4bde99f13](https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a) | 2020-03-23  | Make airflow/providers pylint compatible (#7802)                                            |
| [7e6372a68](https://github.com/apache/airflow/commit/7e6372a681a2a543f4710b083219aeb53b074388) | 2020-03-23  | Add call to Super call in apache providers (#7820)                                          |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                                    |
| [057f3ae3a](https://github.com/apache/airflow/commit/057f3ae3a4afedf6d462ecf58b01dd6304d3e135) | 2020-01-29  | [AIRFLOW-6670][depends on AIRFLOW-6669] Move contrib operators to providers package (#7286) |
| [086d731ce](https://github.com/apache/airflow/commit/086d731ce0066b3037d96df2a05cea1101ed3c17) | 2020-01-14  | [AIRFLOW-6510] Fix druid operator templating (#7127)                                        |
| [0481b9a95](https://github.com/apache/airflow/commit/0481b9a95786a62de4776a735ae80e746583ef2b) | 2020-01-12  | [AIRFLOW-6539][AIP-21] Move Apache classes to providers.apache package (#7142)              |

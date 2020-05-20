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


# Package apache-airflow-backport-providers-apache-pig

Release: 2020.5.20

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [Provider class summary](#provider-class-summary)
    - [Operators](#operators)
        - [Moved operators](#moved-operators)
    - [Hooks](#hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 2020.5.20](#release-2020520)

## Backport package

This is a backport providers package for `apache.pig` provider. All classes for this provider package
are in `airflow.providers.apache.pig` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.



## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-apache-pig`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.apache.pig` package.


## Operators




### Moved operators

| Airflow 2.0 operators: `airflow.providers.apache.pig` package                                                            | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                |
|:-------------------------------------------------------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------|
| [operators.pig.PigOperator](https://github.com/apache/airflow/blob/master/airflow/providers/apache/pig/operators/pig.py) | [operators.pig_operator.PigOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/operators/pig_operator.py) |





## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.apache.pig` package                                                       | Airflow 1.10.* previous location (usually `airflow.contrib`)                                               |
|:----------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------|
| [hooks.pig.PigCliHook](https://github.com/apache/airflow/blob/master/airflow/providers/apache/pig/hooks/pig.py) | [hooks.pig_hook.PigCliHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/hooks/pig_hook.py) |






## Releases

### Release 2020.5.20

| Commit                                                                                         | Committed   | Subject                                                                                  |
|:-----------------------------------------------------------------------------------------------|:------------|:-----------------------------------------------------------------------------------------|
| [12c5e5d8a](https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79) | 2020-05-17  | Prepare release candidate for backport packages (#8891)                                  |
| [f3521fb0e](https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca) | 2020-05-16  | Regenerate readme files for backport package release (#8886)                             |
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807)                  |
| [4bde99f13](https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a) | 2020-03-23  | Make airflow/providers pylint compatible (#7802)                                         |
| [7e6372a68](https://github.com/apache/airflow/commit/7e6372a681a2a543f4710b083219aeb53b074388) | 2020-03-23  | Add call to Super call in apache providers (#7820)                                       |
| [3320e432a](https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc) | 2020-02-24  | [AIRFLOW-6817] Lazy-load `airflow.DAG` to keep user-facing API untouched (#7517)         |
| [9cbd7de6d](https://github.com/apache/airflow/commit/9cbd7de6d115795aba8bfb8addb060bfdfbdf87b) | 2020-02-18  | [AIRFLOW-6792] Remove _operator/_hook/_sensor in providers package and add tests (#7412) |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                                 |
| [83c037873](https://github.com/apache/airflow/commit/83c037873ff694eed67ba8b30f2d9c88b2c7c6f2) | 2020-01-30  | [AIRFLOW-6674] Move example_dags in accordance with AIP-21 (#7287)                       |
| [0481b9a95](https://github.com/apache/airflow/commit/0481b9a95786a62de4776a735ae80e746583ef2b) | 2020-01-12  | [AIRFLOW-6539][AIP-21] Move Apache classes to providers.apache package (#7142)           |

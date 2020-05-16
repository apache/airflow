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


# Package apache-airflow-backport-providers-papermill

Release: 2020.05.19

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-class-summary)
    - [Operators](#operators)
        - [Moved operators](#moved-operators)
- [Releases](#releases)
    - [Release 2020.05.19](#release-20200519)

## Backport package

This is a backport providers package for `papermill` provider. All classes for this provider package
are in `airflow.providers.papermill` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-papermill`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package            | Version required   |
|:-----------------------|:-------------------|
| papermill[all]         | &gt;=1.2.1            |
| nteract-scrapbook[all] | &gt;=0.3.1            |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.papermill` package.


## Operators




### Moved operators

| Airflow 2.0 operators: `airflow.providers.papermill` package                                                                              | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                  |
|:------------------------------------------------------------------------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.papermill.PapermillOperator](https://github.com/apache/airflow/blob/master/airflow/providers/papermill/operators/papermill.py) | [operators.papermill_operator.PapermillOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/operators/papermill_operator.py) |









## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                          |
|:-----------------------------------------------------------------------------------------------|:------------|:---------------------------------------------------------------------------------|
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807)          |
| [4bde99f13](https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a) | 2020-03-23  | Make airflow/providers pylint compatible (#7802)                                 |
| [3320e432a](https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc) | 2020-02-24  | [AIRFLOW-6817] Lazy-load `airflow.DAG` to keep user-facing API untouched (#7517) |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                         |
| [4c81bcd86](https://github.com/apache/airflow/commit/4c81bcd8601fa08efa570ee231f8f103ef830304) | 2020-02-01  | [AIRFLOW-6698] Add shorthand notation for lineage (#7314)                        |
| [83c037873](https://github.com/apache/airflow/commit/83c037873ff694eed67ba8b30f2d9c88b2c7c6f2) | 2020-01-30  | [AIRFLOW-6674] Move example_dags in accordance with AIP-21 (#7287)               |
| [059eda05f](https://github.com/apache/airflow/commit/059eda05f82fefce4410f44f761f945a27d83daf) | 2020-01-21  | [AIRFLOW-6610] Move software classes to providers package (#7231)                |

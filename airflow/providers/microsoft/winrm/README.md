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


# Package apache-airflow-backport-providers-microsoft-winrm

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

This is a backport providers package for `microsoft.winrm` provider. All classes for this provider package
are in `airflow.providers.microsoft.winrm` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-microsoft-winrm`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| pywinrm       | ~=0.4              |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.microsoft.winrm` package.


## Operators




### Moved operators

| Airflow 2.0 operators: `airflow.providers.microsoft.winrm` package                                                                  | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                      |
|:------------------------------------------------------------------------------------------------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.winrm.WinRMOperator](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/winrm/operators/winrm.py) | [contrib.operators.winrm_operator.WinRMOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/winrm_operator.py) |





## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.microsoft.winrm` package                                                          | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                  |
|:------------------------------------------------------------------------------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------|
| [hooks.winrm.WinRMHook](https://github.com/apache/airflow/blob/master/airflow/providers/microsoft/winrm/hooks/winrm.py) | [contrib.hooks.winrm_hook.WinRMHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/winrm_hook.py) |






## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                          |
|:-----------------------------------------------------------------------------------------------|:------------|:---------------------------------------------------------------------------------|
| [4bde99f13](https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a) | 2020-03-23  | Make airflow/providers pylint compatible (#7802)                                 |
| [a83eb335e](https://github.com/apache/airflow/commit/a83eb335e58c6a15e96c517a1b492bc79c869ce8) | 2020-03-23  | Add call to Super call in microsoft providers (#7821)                            |
| [3320e432a](https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc) | 2020-02-24  | [AIRFLOW-6817] Lazy-load `airflow.DAG` to keep user-facing API untouched (#7517) |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                         |
| [83c037873](https://github.com/apache/airflow/commit/83c037873ff694eed67ba8b30f2d9c88b2c7c6f2) | 2020-01-30  | [AIRFLOW-6674] Move example_dags in accordance with AIP-21 (#7287)               |
| [9a04013b0](https://github.com/apache/airflow/commit/9a04013b0e40b0d744ff4ac9f008491806d60df2) | 2020-01-27  | [AIRFLOW-6646][AIP-21] Move protocols classes to providers package (#7268)       |

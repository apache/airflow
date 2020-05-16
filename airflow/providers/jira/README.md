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


# Package apache-airflow-backport-providers-jira

Release: 2020.05.19

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-class-summary)
    - [Operators](#operators)
        - [Moved operators](#moved-operators)
    - [Sensors](#sensors)
        - [Moved sensors](#moved-sensors)
    - [Hooks](#hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 2020.05.19](#release-20200519)

## Backport package

This is a backport providers package for `jira` provider. All classes for this provider package
are in `airflow.providers.jira` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-jira`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| JIRA          | &gt;1.0.7             |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.jira` package.


## Operators




### Moved operators

| Airflow 2.0 operators: `airflow.providers.jira` package                                                               | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                   |
|:----------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.jira.JiraOperator](https://github.com/apache/airflow/blob/master/airflow/providers/jira/operators/jira.py) | [contrib.operators.jira_operator.JiraOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/jira_operator.py) |




## Sensors



### Moved sensors

| Airflow 2.0 sensors: `airflow.providers.jira` package                                                                 | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                               |
|:----------------------------------------------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------|
| [sensors.jira.JiraSensor](https://github.com/apache/airflow/blob/master/airflow/providers/jira/sensors/jira.py)       | [contrib.sensors.jira_sensor.JiraSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/jira_sensor.py)       |
| [sensors.jira.JiraTicketSensor](https://github.com/apache/airflow/blob/master/airflow/providers/jira/sensors/jira.py) | [contrib.sensors.jira_sensor.JiraTicketSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/jira_sensor.py) |



## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.jira` package                                                       | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                               |
|:----------------------------------------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------|
| [hooks.jira.JiraHook](https://github.com/apache/airflow/blob/master/airflow/providers/jira/hooks/jira.py) | [contrib.hooks.jira_hook.JiraHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/jira_hook.py) |






## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                 |
|:-----------------------------------------------------------------------------------------------|:------------|:------------------------------------------------------------------------|
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807) |
| [05443c6dc](https://github.com/apache/airflow/commit/05443c6dc8100e791446bbcc0df04de6e34017bb) | 2020-03-23  | Add missing call to Super class in remaining providers (#7828)          |
| [f3ad5cf61](https://github.com/apache/airflow/commit/f3ad5cf6185b9d406d0fb0a4ecc0b5536f79217a) | 2020-02-03  | [AIRFLOW-4681] Make sensors module pylint compatible (#7309)            |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                |
| [4a21b6216](https://github.com/apache/airflow/commit/4a21b62161a8e14f0dbc06f292f4662832c52669) | 2019-12-13  | [AIRFLOW-5959][AIP-21] Move contrib/*/jira to providers (#6661)         |

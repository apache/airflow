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


# Package apache-airflow-backport-providers-qubole

Release: 2020.6.24

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-classes-summary)
    - [Operators](#operators)
        - [Moved operators](#moved-operators)
    - [Sensors](#sensors)
        - [Moved sensors](#moved-sensors)
    - [Hooks](#hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 2020.6.24](#release-2020624)

## Backport package

This is a backport providers package for `qubole` provider. All classes for this provider package
are in `airflow.providers.qubole` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.



## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-qubole`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| qds-sdk       | &gt;=1.10.4           |

# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `qubole` provider
are in the `airflow.providers.qubole` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)


## Operators




### Moved operators

| Airflow 2.0 operators: `airflow.providers.qubole` package                                                                                           | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                               |
|:----------------------------------------------------------------------------------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.qubole.QuboleOperator](https://github.com/apache/airflow/blob/master/airflow/providers/qubole/operators/qubole.py)                       | [contrib.operators.qubole_operator.QuboleOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/qubole_operator.py)                       |
| [operators.qubole_check.QuboleCheckOperator](https://github.com/apache/airflow/blob/master/airflow/providers/qubole/operators/qubole_check.py)      | [contrib.operators.qubole_check_operator.QuboleCheckOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/qubole_check_operator.py)      |
| [operators.qubole_check.QuboleValueCheckOperator](https://github.com/apache/airflow/blob/master/airflow/providers/qubole/operators/qubole_check.py) | [contrib.operators.qubole_check_operator.QuboleValueCheckOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/qubole_check_operator.py) |






## Sensors



### Moved sensors

| Airflow 2.0 sensors: `airflow.providers.qubole` package                                                                          | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                        |
|:---------------------------------------------------------------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------|
| [sensors.qubole.QuboleFileSensor](https://github.com/apache/airflow/blob/master/airflow/providers/qubole/sensors/qubole.py)      | [contrib.sensors.qubole_sensor.QuboleFileSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/qubole_sensor.py)      |
| [sensors.qubole.QubolePartitionSensor](https://github.com/apache/airflow/blob/master/airflow/providers/qubole/sensors/qubole.py) | [contrib.sensors.qubole_sensor.QubolePartitionSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/qubole_sensor.py) |
| [sensors.qubole.QuboleSensor](https://github.com/apache/airflow/blob/master/airflow/providers/qubole/sensors/qubole.py)          | [contrib.sensors.qubole_sensor.QuboleSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/qubole_sensor.py)          |



## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.qubole` package                                                                              | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                      |
|:-----------------------------------------------------------------------------------------------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------|
| [hooks.qubole.QuboleHook](https://github.com/apache/airflow/blob/master/airflow/providers/qubole/hooks/qubole.py)                  | [contrib.hooks.qubole_hook.QuboleHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/qubole_hook.py)                  |
| [hooks.qubole_check.QuboleCheckHook](https://github.com/apache/airflow/blob/master/airflow/providers/qubole/hooks/qubole_check.py) | [contrib.hooks.qubole_check_hook.QuboleCheckHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/qubole_check_hook.py) |






## Releases

### Release 2020.6.24

| Commit                                                                                         | Committed   | Subject                                                                                                                                                            |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [12af6a080](https://github.com/apache/airflow/commit/12af6a08009b8776e00d8a0aab92363eb8c4e8b1) | 2020-06-19  | Final cleanup for 2020.6.23rc1 release preparation (#9404)                                                                                                         |
| [c7e5bce57](https://github.com/apache/airflow/commit/c7e5bce57fe7f51cefce4f8a41ce408ac5675d13) | 2020-06-19  | Prepare backport release candidate for 2020.6.23rc1 (#9370)                                                                                                        |
| [f6bd817a3](https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac) | 2020-06-16  | Introduce &#39;transfers&#39; packages (#9320)                                                                                                                             |
| [0b0e4f7a4](https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34) | 2020-05-26  | Preparing for RC3 release of backports (#9026)                                                                                                                      |
| [00642a46d](https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c) | 2020-05-26  | Fixed name of 20 remaining wrongly named operators. (#8994)                                                                                                        |
| [1d36b0303](https://github.com/apache/airflow/commit/1d36b0303b8632fce6de78ca4e782ae26ee06fea) | 2020-05-23  | Fix references in docs (#8984)                                                                                                                                     |
| [375d1ca22](https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f) | 2020-05-19  | Release candidate 2 for backport packages 2020.05.20 (#8898)                                                                                                       |
| [12c5e5d8a](https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79) | 2020-05-17  | Prepare release candidate for backport packages (#8891)                                                                                                            |
| [f3521fb0e](https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca) | 2020-05-16  | Regenerate readme files for backport package release (#8886)                                                                                                       |
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807)                                                                                            |
| [4b06fde0f](https://github.com/apache/airflow/commit/4b06fde0f10ce178b3c336c5d901e3b089f2863d) | 2020-05-12  | Fix Flake8 errors (#8841)                                                                                                                                          |
| [87969a350](https://github.com/apache/airflow/commit/87969a350ddd41e9e77776af6d780b31e363eaca) | 2020-04-09  | [AIRFLOW-6515] Change Log Levels from Info/Warn to Error (#8170)                                                                                                   |
| [4bde99f13](https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a) | 2020-03-23  | Make airflow/providers pylint compatible (#7802)                                                                                                                   |
| [05443c6dc](https://github.com/apache/airflow/commit/05443c6dc8100e791446bbcc0df04de6e34017bb) | 2020-03-23  | Add missing call to Super class in remaining providers (#7828)                                                                                                     |
| [de7e934ca](https://github.com/apache/airflow/commit/de7e934ca3f21ce82f67accf92811b3ac044476f) | 2020-03-17  | [AIRFLOW-7079] Remove redundant code for storing template_fields (#7750)                                                                                           |
| [3320e432a](https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc) | 2020-02-24  | [AIRFLOW-6817] Lazy-load `airflow.DAG` to keep user-facing API untouched (#7517)                                                                                   |
| [4d03e33c1](https://github.com/apache/airflow/commit/4d03e33c115018e30fa413c42b16212481ad25cc) | 2020-02-22  | [AIRFLOW-6817] remove imports from `airflow/__init__.py`, replaced implicit imports with explicit imports, added entry to `UPDATING.MD` - squashed/rebased (#7456) |
| [9cbd7de6d](https://github.com/apache/airflow/commit/9cbd7de6d115795aba8bfb8addb060bfdfbdf87b) | 2020-02-18  | [AIRFLOW-6792] Remove _operator/_hook/_sensor in providers package and add tests (#7412)                                                                           |
| [f3ad5cf61](https://github.com/apache/airflow/commit/f3ad5cf6185b9d406d0fb0a4ecc0b5536f79217a) | 2020-02-03  | [AIRFLOW-4681] Make sensors module pylint compatible (#7309)                                                                                                       |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                                                                                                           |
| [cf141506a](https://github.com/apache/airflow/commit/cf141506a25dbba279b85500d781f7e056540721) | 2020-02-02  | [AIRFLOW-6708] Set unique logger names (#7330)                                                                                                                     |
| [a2d6a2f85](https://github.com/apache/airflow/commit/a2d6a2f85e07c38be479e91e4a27981f308f4711) | 2020-01-31  | [AIRFLOW-6687] Switch kubernetes tests to example_dags (#7299)                                                                                                     |
| [83c037873](https://github.com/apache/airflow/commit/83c037873ff694eed67ba8b30f2d9c88b2c7c6f2) | 2020-01-30  | [AIRFLOW-6674] Move example_dags in accordance with AIP-21 (#7287)                                                                                                 |
| [c42a375e7](https://github.com/apache/airflow/commit/c42a375e799e5adb3f9536616372dc90ff47e6c8) | 2020-01-27  | [AIRFLOW-6644][AIP-21] Move service classes to providers package (#7265)                                                                                           |

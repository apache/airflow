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


# Package apache-airflow-providers-qubole

Release: 0.0.2a1

**Table of contents**

- [Provider package](#provider-package)
- [Installation](#installation)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-classes-summary)
    - [Operators](#operators)
        - [Moved operators](#moved-operators)
    - [Sensors](#sensors)
        - [Moved sensors](#moved-sensors)
    - [Hooks](#hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 0.0.2a1](#release-002a1)
    - [Release 0.0.1](#release-001)

## Provider package

This is a provider package for `qubole` provider. All classes for this provider package
are in `airflow.providers.qubole` python package.



## Installation

You can install this package on top of an existing airflow 2.* installation via
`pip install apache-airflow-providers-qubole`

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

### Release 0.0.2a1

| Commit                                                                                         | Committed   | Subject                                                            |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------|
| [872b1566a](https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574) | 2020-10-25  | Generated backport providers readmes/setup for 2020.10.29 (#11826) |
| [349b0811c](https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a) | 2020-10-20  | Add D200 pydocstyle check (#11688)                                 |
| [16e712971](https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746) | 2020-10-13  | Added support for provider packages for Airflow 2.0 (#11487)       |


### Release 0.0.1

| Commit                                                                                         | Committed   | Subject                                                                                                                                                            |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [0a0e1af80](https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa) | 2020-10-03  | Fix Broken Markdown links in Providers README TOC (#11249)                                                                                                         |
| [ca4238eb4](https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13) | 2020-10-02  | Fixed month in backport packages to October (#11242)                                                                                                               |
| [5220e4c38](https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5) | 2020-10-02  | Prepare Backport release 2020.09.07 (#11238)                                                                                                                       |
| [720912f67](https://github.com/apache/airflow/commit/720912f67b3af0bdcbac64d6b8bf6d51c6247e26) | 2020-10-02  | Strict type check for multiple providers (#11229)                                                                                                                  |
| [c58d60635](https://github.com/apache/airflow/commit/c58d60635dbab1a91f38e989f72f91645cb7eb62) | 2020-09-11  | Update qubole_hook to not remove pool as an arg for qubole_operator (#10820)                                                                                       |
| [9549274d1](https://github.com/apache/airflow/commit/9549274d110f689a0bd709db829a4d69e274eed9) | 2020-09-09  | Upgrade black to 20.8b1 (#10818)                                                                                                                                   |
| [36aa88ffc](https://github.com/apache/airflow/commit/36aa88ffc1e3feb5c6f4520871a4f6e3196c0804) | 2020-09-03  | Add jupytercmd and fix task failure when notify set as true in qubole operator (#10599)                                                                            |
| [fdd9b6f65](https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3) | 2020-08-25  | Enable Black on Providers Packages (#10543)                                                                                                                        |
| [d1bce91bb](https://github.com/apache/airflow/commit/d1bce91bb21d5a468fa6a0207156c28fe1ca6513) | 2020-08-25  | PyDocStyle: Enable D403: Capitalized first word of docstring (#10530)                                                                                              |
| [3696c34c2](https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34) | 2020-08-24  | Fix typo in the word &#34;release&#34; (#10528)                                                                                                                            |
| [ee7ca128a](https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94) | 2020-08-22  | Fix broken Markdown refernces in Providers README (#10483)                                                                                                         |
| [cdec30125](https://github.com/apache/airflow/commit/cdec3012542b45d23a05f62d69110944ba542e2a) | 2020-08-07  | Add correct signature to all operators and sensors (#10205)                                                                                                        |
| [24c8e4c2d](https://github.com/apache/airflow/commit/24c8e4c2d6e359ecc2c7d6275dccc68de4a82832) | 2020-08-06  | Changes to all the constructors to remove the args argument (#10163)                                                                                               |
| [aeea71274](https://github.com/apache/airflow/commit/aeea71274d4527ff2351102e94aa38bda6099e7f) | 2020-08-02  | Remove `args` parameter from provider operator constructors (#10097)                                                                                               |
| [7d24b088c](https://github.com/apache/airflow/commit/7d24b088cd736cfa18f9214e4c9d6ce2d5865f3d) | 2020-07-25  | Stop using start_date in default_args in example_dags (2) (#9985)                                                                                                  |
| [33f0cd265](https://github.com/apache/airflow/commit/33f0cd2657b2e77ea3477e0c93f13f1474be628e) | 2020-07-22  | apply_default keeps the function signature for mypy (#9784)                                                                                                        |
| [3190db524](https://github.com/apache/airflow/commit/3190db52469f9d9a338231a9e8e7f333a6fbb638) | 2020-06-24  | [AIRFLOW-9347] Fix QuboleHook unable to add list to tags (#9349)                                                                                                   |
| [d0e7db402](https://github.com/apache/airflow/commit/d0e7db4024806af35e3c9a2cae460fdeedd4d2ec) | 2020-06-19  | Fixed release number for fresh release (#9408)                                                                                                                     |
| [12af6a080](https://github.com/apache/airflow/commit/12af6a08009b8776e00d8a0aab92363eb8c4e8b1) | 2020-06-19  | Final cleanup for 2020.6.23rc1 release preparation (#9404)                                                                                                         |
| [c7e5bce57](https://github.com/apache/airflow/commit/c7e5bce57fe7f51cefce4f8a41ce408ac5675d13) | 2020-06-19  | Prepare backport release candidate for 2020.6.23rc1 (#9370)                                                                                                        |
| [f6bd817a3](https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac) | 2020-06-16  | Introduce &#39;transfers&#39; packages (#9320)                                                                                                                             |
| [0b0e4f7a4](https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34) | 2020-05-26  | Preparing for RC3 relase of backports (#9026)                                                                                                                      |
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

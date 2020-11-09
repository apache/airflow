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


# Package apache-airflow-providers-salesforce

Release: 1.0.0b1

**Table of contents**

- [Provider package](#provider-package)
- [Installation](#installation)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-classes-summary)
    - [Operators](#operators)
        - [New operators](#new-operators)
    - [Sensors](#sensors)
        - [New sensors](#new-sensors)
    - [Hooks](#hooks)
        - [New hooks](#new-hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 1.0.0b1](#release-100b1)
    - [Release 0.0.2a1](#release-002a1)
    - [Release 0.0.1](#release-001)

## Provider package

This is a provider package for `salesforce` provider. All classes for this provider package
are in `airflow.providers.salesforce` python package.



## Installation

You can install this package on top of an existing airflow 2.* installation via
`pip install apache-airflow-providers-salesforce`

## PIP requirements

| PIP package       | Version required   |
|:------------------|:-------------------|
| simple-salesforce | &gt;=1.0.0            |

# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `salesforce` provider
are in the `airflow.providers.salesforce` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)


## Operators


### New operators

| New Airflow 2.0 operators: `airflow.providers.salesforce` package                                                                                                                     |
|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.tableau_refresh_workbook.TableauRefreshWorkbookOperator](https://github.com/apache/airflow/blob/master/airflow/providers/salesforce/operators/tableau_refresh_workbook.py) |



## Sensors


### New sensors

| New Airflow 2.0 sensors: `airflow.providers.salesforce` package                                                                                               |
|:--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [sensors.tableau_job_status.TableauJobStatusSensor](https://github.com/apache/airflow/blob/master/airflow/providers/salesforce/sensors/tableau_job_status.py) |



## Hooks


### New hooks

| New Airflow 2.0 hooks: `airflow.providers.salesforce` package                                                            |
|:-------------------------------------------------------------------------------------------------------------------------|
| [hooks.tableau.TableauHook](https://github.com/apache/airflow/blob/master/airflow/providers/salesforce/hooks/tableau.py) |


### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.salesforce` package                                                                         | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                 |
|:----------------------------------------------------------------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------|
| [hooks.salesforce.SalesforceHook](https://github.com/apache/airflow/blob/master/airflow/providers/salesforce/hooks/salesforce.py) | [contrib.hooks.salesforce_hook.SalesforceHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/salesforce_hook.py) |



## Releases

### Release 1.0.0b1

| Commit                                                                                         | Committed   | Subject                                                            |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------|
| [59eb5de78](https://github.com/apache/airflow/commit/59eb5de78c70ee9c7ae6e4cba5c7a2babb8103ca) | 2020-11-09  | Update provider READMEs for up-coming 1.0.0beta1 releases (#12206) |
| [b2a28d159](https://github.com/apache/airflow/commit/b2a28d1590410630d66966aa1f2b2a049a8c3b32) | 2020-11-09  | Moves provider packages scripts to dev (#12082)                    |
| [4e8f9cc8d](https://github.com/apache/airflow/commit/4e8f9cc8d02b29c325b8a5a76b4837671bdf5f68) | 2020-11-03  | Enable Black - Python Auto Formmatter (#9550)                      |
| [8c42cf1b0](https://github.com/apache/airflow/commit/8c42cf1b00c90f0d7f11b8a3a455381de8e003c5) | 2020-11-03  | Use PyUpgrade to use Python 3.6 features (#11447)                  |
| [5a439e84e](https://github.com/apache/airflow/commit/5a439e84eb6c0544dc6c3d6a9f4ceeb2172cd5d0) | 2020-10-26  | Prepare providers release 0.0.2a1 (#11855)                         |


### Release 0.0.2a1

| Commit                                                                                         | Committed   | Subject                                                            |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------|
| [872b1566a](https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574) | 2020-10-25  | Generated backport providers readmes/setup for 2020.10.29 (#11826) |
| [349b0811c](https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a) | 2020-10-20  | Add D200 pydocstyle check (#11688)                                 |
| [16e712971](https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746) | 2020-10-13  | Added support for provider packages for Airflow 2.0 (#11487)       |


### Release 0.0.1

| Commit                                                                                         | Committed   | Subject                                                                          |
|:-----------------------------------------------------------------------------------------------|:------------|:---------------------------------------------------------------------------------|
| [d305876be](https://github.com/apache/airflow/commit/d305876bee328287ff391a29cc1cd632468cc731) | 2020-10-12  | Remove redundant None provided as default to dict.get() (#11448)                 |
| [0a0e1af80](https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa) | 2020-10-03  | Fix Broken Markdown links in Providers README TOC (#11249)                       |
| [ca4238eb4](https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13) | 2020-10-02  | Fixed month in backport packages to October (#11242)                             |
| [5220e4c38](https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5) | 2020-10-02  | Prepare Backport release 2020.09.07 (#11238)                                     |
| [45669bea4](https://github.com/apache/airflow/commit/45669bea4f690021b69a7d2afa8be9567ae0c49d) | 2020-09-24  | Increasing type coverage for salesforce provide (#11135)                         |
| [f3e87c503](https://github.com/apache/airflow/commit/f3e87c503081a3085dff6c7352640d7f08beb5bc) | 2020-09-22  | Add D202 pydocstyle check (#11032)                                               |
| [fdd9b6f65](https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3) | 2020-08-25  | Enable Black on Providers Packages (#10543)                                      |
| [3696c34c2](https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34) | 2020-08-24  | Fix typo in the word &#34;release&#34; (#10528)                                          |
| [ee7ca128a](https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94) | 2020-08-22  | Fix broken Markdown refernces in Providers README (#10483)                       |
| [cdec30125](https://github.com/apache/airflow/commit/cdec3012542b45d23a05f62d69110944ba542e2a) | 2020-08-07  | Add correct signature to all operators and sensors (#10205)                      |
| [24c8e4c2d](https://github.com/apache/airflow/commit/24c8e4c2d6e359ecc2c7d6275dccc68de4a82832) | 2020-08-06  | Changes to all the constructors to remove the args argument (#10163)             |
| [aeea71274](https://github.com/apache/airflow/commit/aeea71274d4527ff2351102e94aa38bda6099e7f) | 2020-08-02  | Remove `args` parameter from provider operator constructors (#10097)             |
| [7d24b088c](https://github.com/apache/airflow/commit/7d24b088cd736cfa18f9214e4c9d6ce2d5865f3d) | 2020-07-25  | Stop using start_date in default_args in example_dags (2) (#9985)                |
| [d0e7db402](https://github.com/apache/airflow/commit/d0e7db4024806af35e3c9a2cae460fdeedd4d2ec) | 2020-06-19  | Fixed release number for fresh release (#9408)                                   |
| [12af6a080](https://github.com/apache/airflow/commit/12af6a08009b8776e00d8a0aab92363eb8c4e8b1) | 2020-06-19  | Final cleanup for 2020.6.23rc1 release preparation (#9404)                       |
| [c7e5bce57](https://github.com/apache/airflow/commit/c7e5bce57fe7f51cefce4f8a41ce408ac5675d13) | 2020-06-19  | Prepare backport release candidate for 2020.6.23rc1 (#9370)                      |
| [f6bd817a3](https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac) | 2020-06-16  | Introduce &#39;transfers&#39; packages (#9320)                                           |
| [0b0e4f7a4](https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34) | 2020-05-26  | Preparing for RC3 relase of backports (#9026)                                    |
| [00642a46d](https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c) | 2020-05-26  | Fixed name of 20 remaining wrongly named operators. (#8994)                      |
| [1d36b0303](https://github.com/apache/airflow/commit/1d36b0303b8632fce6de78ca4e782ae26ee06fea) | 2020-05-23  | Fix references in docs (#8984)                                                   |
| [375d1ca22](https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f) | 2020-05-19  | Release candidate 2 for backport packages 2020.05.20 (#8898)                     |
| [12c5e5d8a](https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79) | 2020-05-17  | Prepare release candidate for backport packages (#8891)                          |
| [ff342fc23](https://github.com/apache/airflow/commit/ff342fc230982dc5d88acfd5e5eab75187256b58) | 2020-05-17  | Added SalesforceHook missing method to return only dataframe (#8565) (#8644)     |
| [f3521fb0e](https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca) | 2020-05-16  | Regenerate readme files for backport package release (#8886)                     |
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807)          |
| [87969a350](https://github.com/apache/airflow/commit/87969a350ddd41e9e77776af6d780b31e363eaca) | 2020-04-09  | [AIRFLOW-6515] Change Log Levels from Info/Warn to Error (#8170)                 |
| [954619283](https://github.com/apache/airflow/commit/95461928365f255c79ab4a164ce60d8eebea29d7) | 2020-03-26  | bumping simple-salesforce to 1.0.0 (#7857)                                       |
| [31efc931e](https://github.com/apache/airflow/commit/31efc931e32841b7da8decd576cafa1e5a6f6d95) | 2020-03-23  | Add missing call to Super class in &#39;salesforce&#39; provider (#7824)                 |
| [6140356b8](https://github.com/apache/airflow/commit/6140356b80f68906e89ccf46941a949bdc4d43fa) | 2020-03-12  | [AIRFLOW-6481] Fix bug in SalesforceHook (#7703)                                 |
| [3320e432a](https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc) | 2020-02-24  | [AIRFLOW-6817] Lazy-load `airflow.DAG` to keep user-facing API untouched (#7517) |
| [61a8bb658](https://github.com/apache/airflow/commit/61a8bb65818521ccbb846e647103535b3e36b26d) | 2020-02-22  | [AIRFLOW-6879] Fix Failing CI: Update New import paths (#7500)                   |
| [a9ad0a929](https://github.com/apache/airflow/commit/a9ad0a929851b6912e0bb8551f1ff80b50281944) | 2020-02-22  | [AIRFLOW-6790] Add basic Tableau Integration (#7410)                             |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                         |
| [cf141506a](https://github.com/apache/airflow/commit/cf141506a25dbba279b85500d781f7e056540721) | 2020-02-02  | [AIRFLOW-6708] Set unique logger names (#7330)                                   |
| [c42a375e7](https://github.com/apache/airflow/commit/c42a375e799e5adb3f9536616372dc90ff47e6c8) | 2020-01-27  | [AIRFLOW-6644][AIP-21] Move service classes to providers package (#7265)         |

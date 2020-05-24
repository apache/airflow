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


# Package apache-airflow-backport-providers-salesforce

Release: 2020.5.20

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-class-summary)
    - [Operators](#operators)
        - [New operators](#new-operators)
    - [Sensors](#sensors)
        - [New sensors](#new-sensors)
    - [Hooks](#hooks)
        - [New hooks](#new-hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 2020.5.20](#release-2020520)

## Backport package

This is a backport providers package for `salesforce` provider. All classes for this provider package
are in `airflow.providers.salesforce` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.



## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-salesforce`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package       | Version required   |
|:------------------|:-------------------|
| simple-salesforce | &gt;=1.0.0            |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.salesforce` package.


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

### Release 2020.5.20

| Commit                                                                                         | Committed   | Subject                                                                          |
|:-----------------------------------------------------------------------------------------------|:------------|:---------------------------------------------------------------------------------|
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

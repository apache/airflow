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


# Package apache-airflow-backport-providers-jenkins

Release: 2020.5.20

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
    - [Release 2020.5.20](#release-2020520)

## Backport package

This is a backport providers package for `jenkins` provider. All classes for this provider package
are in `airflow.providers.jenkins` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.



## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-jenkins`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package    | Version required   |
|:---------------|:-------------------|
| python-jenkins | &gt;=1.0.0            |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.jenkins` package.


## Operators




### Moved operators

| Airflow 2.0 operators: `airflow.providers.jenkins` package                                                                                                          | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                                              |
|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.jenkins_job_trigger.JenkinsJobTriggerOperator](https://github.com/apache/airflow/blob/master/airflow/providers/jenkins/operators/jenkins_job_trigger.py) | [contrib.operators.jenkins_job_trigger_operator.JenkinsJobTriggerOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/jenkins_job_trigger_operator.py) |







## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.jenkins` package                                                                | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                        |
|:----------------------------------------------------------------------------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------|
| [hooks.jenkins.JenkinsHook](https://github.com/apache/airflow/blob/master/airflow/providers/jenkins/hooks/jenkins.py) | [contrib.hooks.jenkins_hook.JenkinsHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/jenkins_hook.py) |






## Releases

### Release 2020.5.20

| Commit                                                                                         | Committed   | Subject                                                                                                                                                            |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [0b0e4f7a4](https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34) | 2020-05-26  | Preparing for RC3 relase of backports (#9026)                                                                                                                      |
| [00642a46d](https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c) | 2020-05-26  | Fixed name of 20 remaining wrongly named operators. (#8994)                                                                                                        |
| [375d1ca22](https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f) | 2020-05-19  | Release candidate 2 for backport packages 2020.05.20 (#8898)                                                                                                       |
| [12c5e5d8a](https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79) | 2020-05-17  | Prepare release candidate for backport packages (#8891)                                                                                                            |
| [f3521fb0e](https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca) | 2020-05-16  | Regenerate readme files for backport package release (#8886)                                                                                                       |
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807)                                                                                            |
| [4bde99f13](https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a) | 2020-03-23  | Make airflow/providers pylint compatible (#7802)                                                                                                                   |
| [05443c6dc](https://github.com/apache/airflow/commit/05443c6dc8100e791446bbcc0df04de6e34017bb) | 2020-03-23  | Add missing call to Super class in remaining providers (#7828)                                                                                                     |
| [3320e432a](https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc) | 2020-02-24  | [AIRFLOW-6817] Lazy-load `airflow.DAG` to keep user-facing API untouched (#7517)                                                                                   |
| [4d03e33c1](https://github.com/apache/airflow/commit/4d03e33c115018e30fa413c42b16212481ad25cc) | 2020-02-22  | [AIRFLOW-6817] remove imports from `airflow/__init__.py`, replaced implicit imports with explicit imports, added entry to `UPDATING.MD` - squashed/rebased (#7456) |
| [dbcd3d878](https://github.com/apache/airflow/commit/dbcd3d8787741fd8203b6d9bdbc5d1da4b10a15b) | 2020-02-18  | [AIRFLOW-6804] Add the basic test for all example DAGs (#7419)                                                                                                     |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                                                                                                           |
| [633eca1de](https://github.com/apache/airflow/commit/633eca1de5042e95e23aaf2e7680ed3106cb0e87) | 2020-02-02  | [AIRFLOW-6692] Generate excluded_patterns in docs/conf.py (#7304)                                                                                                  |
| [83c037873](https://github.com/apache/airflow/commit/83c037873ff694eed67ba8b30f2d9c88b2c7c6f2) | 2020-01-30  | [AIRFLOW-6674] Move example_dags in accordance with AIP-21 (#7287)                                                                                                 |
| [ceea293c1](https://github.com/apache/airflow/commit/ceea293c1652240e7e856c201e4341a87ef97a0f) | 2020-01-28  | [AIRFLOW-6656] Fix AIP-21 moving (#7272)                                                                                                                           |

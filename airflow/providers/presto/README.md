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


# Package apache-airflow-backport-providers-presto

Release: 2020.5.20

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-class-summary)
    - [Hooks](#hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 2020.5.20](#release-2020520)

## Backport package

This is a backport providers package for `presto` provider. All classes for this provider package
are in `airflow.providers.presto` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-presto`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package          | Version required   |
|:---------------------|:-------------------|
| presto-python-client | &gt;=0.7.0,&lt;0.8       |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.presto` package.





## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.presto` package                                                             | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                     |
|:------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------|
| [hooks.presto.PrestoHook](https://github.com/apache/airflow/blob/master/airflow/providers/presto/hooks/presto.py) | [hooks.presto_hook.PrestoHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/hooks/presto_hook.py) |






## Releases

### Release 2020.5.20

| Commit                                                                                         | Committed   | Subject                                                                 |
|:-----------------------------------------------------------------------------------------------|:------------|:------------------------------------------------------------------------|
| [f3521fb0e](https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca) | 2020-05-16  | Regenerate readme files for backport package release (#8886)            |
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807) |
| [1100cea1f](https://github.com/apache/airflow/commit/1100cea1fb9e010e6f4acc699c6d54d056c0541c) | 2020-05-03  | Remove _get_pretty_exception_message in PrestoHook                      |
| [35834c380](https://github.com/apache/airflow/commit/35834c3809ce6f5f1dcff130d0e68cabed7f72de) | 2020-03-26  | Remove Presto check operators (#7884)                                   |
| [4bde99f13](https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a) | 2020-03-23  | Make airflow/providers pylint compatible (#7802)                        |
| [029c84e55](https://github.com/apache/airflow/commit/029c84e5527b6db6bdbdbe026f455da325bedef3) | 2020-03-18  | [AIRFLOW-5421] Add Presto to GCS transfer operator (#7718)              |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                |
| [059eda05f](https://github.com/apache/airflow/commit/059eda05f82fefce4410f44f761f945a27d83daf) | 2020-01-21  | [AIRFLOW-6610] Move software classes to providers package (#7231)       |

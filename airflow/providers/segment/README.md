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


# Package apache-airflow-backport-providers-segment

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

This is a backport providers package for `segment` provider. All classes for this provider package
are in `airflow.providers.segment` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-segment`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package      | Version required   |
|:-----------------|:-------------------|
| analytics-python | &gt;=1.2.9            |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.segment` package.


## Operators




### Moved operators

| Airflow 2.0 operators: `airflow.providers.segment` package                                                                                                          | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                                              |
|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.segment_track_event.SegmentTrackEventOperator](https://github.com/apache/airflow/blob/master/airflow/providers/segment/operators/segment_track_event.py) | [contrib.operators.segment_track_event_operator.SegmentTrackEventOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/segment_track_event_operator.py) |





## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.segment` package                                                                | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                        |
|:----------------------------------------------------------------------------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------|
| [hooks.segment.SegmentHook](https://github.com/apache/airflow/blob/master/airflow/providers/segment/hooks/segment.py) | [contrib.hooks.segment_hook.SegmentHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/segment_hook.py) |






## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                  |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------------|
| [4bde99f13](https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a) | 2020-03-23  | Make airflow/providers pylint compatible (#7802)                         |
| [05443c6dc](https://github.com/apache/airflow/commit/05443c6dc8100e791446bbcc0df04de6e34017bb) | 2020-03-23  | Add missing call to Super class in remaining providers (#7828)           |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                 |
| [c42a375e7](https://github.com/apache/airflow/commit/c42a375e799e5adb3f9536616372dc90ff47e6c8) | 2020-01-27  | [AIRFLOW-6644][AIP-21] Move service classes to providers package (#7265) |

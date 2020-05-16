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


# Package apache-airflow-backport-providers-cloudant

Release: 2020.05.19

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-class-summary)
    - [Hooks](#hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 2020.05.19](#release-20200519)

## Backport package

This is a backport providers package for `cloudant` provider. All classes for this provider package
are in `airflow.providers.cloudant` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-cloudant`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| cloudant      | &gt;=2.0              |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.cloudant` package.





## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.cloudant` package                                                                   | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                           |
|:--------------------------------------------------------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------|
| [hooks.cloudant.CloudantHook](https://github.com/apache/airflow/blob/master/airflow/providers/cloudant/hooks/cloudant.py) | [contrib.hooks.cloudant_hook.CloudantHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/cloudant_hook.py) |






## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                                 |
|:-----------------------------------------------------------------------------------------------|:------------|:----------------------------------------------------------------------------------------|
| [5648dfbc3](https://github.com/apache/airflow/commit/5648dfbc300337b10567ef4e07045ea29d33ec06) | 2020-03-23  | Add missing call to Super class in &#39;amazon&#39;, &#39;cloudant &amp; &#39;databricks&#39; providers (#7827) |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                                |
| [c42a375e7](https://github.com/apache/airflow/commit/c42a375e799e5adb3f9536616372dc90ff47e6c8) | 2020-01-27  | [AIRFLOW-6644][AIP-21] Move service classes to providers package (#7265)                |

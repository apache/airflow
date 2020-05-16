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


# Package apache-airflow-backport-providers-apache-pinot

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

This is a backport providers package for `apache.pinot` provider. All classes for this provider package
are in `airflow.providers.apache.pinot` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-apache-pinot`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| pinotdb       | ==0.1.1            |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.apache.pinot` package.





## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.apache.pinot` package                                                               | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                       |
|:--------------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------|
| [hooks.pinot.PinotAdminHook](https://github.com/apache/airflow/blob/master/airflow/providers/apache/pinot/hooks/pinot.py) | [contrib.hooks.pinot_hook.PinotAdminHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/pinot_hook.py) |
| [hooks.pinot.PinotDbApiHook](https://github.com/apache/airflow/blob/master/airflow/providers/apache/pinot/hooks/pinot.py) | [contrib.hooks.pinot_hook.PinotDbApiHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/pinot_hook.py) |






## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                        |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------------------|
| [45c898330](https://github.com/apache/airflow/commit/45c8983306ab1c54abdacd8f870e790fad25cb37) | 2020-04-13  | Less aggressive eager upgrade of requirements (#8267)                          |
| [4bde99f13](https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a) | 2020-03-23  | Make airflow/providers pylint compatible (#7802)                               |
| [7e6372a68](https://github.com/apache/airflow/commit/7e6372a681a2a543f4710b083219aeb53b074388) | 2020-03-23  | Add call to Super call in apache providers (#7820)                             |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                       |
| [0481b9a95](https://github.com/apache/airflow/commit/0481b9a95786a62de4776a735ae80e746583ef2b) | 2020-01-12  | [AIRFLOW-6539][AIP-21] Move Apache classes to providers.apache package (#7142) |

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


# Package apache-airflow-backport-providers-facebook

Release: 2020.05.19

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-class-summary)
    - [Hooks](#hooks)
        - [New hooks](#new-hooks)
- [Releases](#releases)
    - [Release 2020.05.19](#release-20200519)

## Backport package

This is a backport providers package for `facebook` provider. All classes for this provider package
are in `airflow.providers.facebook` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-facebook`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package       | Version required   |
|:------------------|:-------------------|
| facebook-business | &gt;=6.0.2            |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.facebook` package.





## Hooks


### New hooks

| New Airflow 2.0 hooks: `airflow.providers.facebook` package                                                                         |
|:------------------------------------------------------------------------------------------------------------------------------------|
| [ads.hooks.ads.FacebookAdsReportingHook](https://github.com/apache/airflow/blob/master/airflow/providers/facebook/ads/hooks/ads.py) |







## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                 |
|:-----------------------------------------------------------------------------------------------|:------------|:------------------------------------------------------------------------|
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807) |
| [bc45fa675](https://github.com/apache/airflow/commit/bc45fa6759203b4c26b52e693dac97486a84204e) | 2020-05-03  | Add system test and docs for Facebook Ads operators (#8503)             |
| [eee4ebaee](https://github.com/apache/airflow/commit/eee4ebaeeb1991480ee178ddb600bc69b2a88764) | 2020-04-14  | Added Facebook Ads Operator #7887 (#8008)                               |

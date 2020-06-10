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


# Package apache-airflow-backport-providers-snowflake

Release: 2020.5.20

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-class-summary)
    - [Operators](#operators)
        - [New operators](#new-operators)
    - [Hooks](#hooks)
        - [New hooks](#new-hooks)
- [Releases](#releases)
    - [Release 2020.5.20](#release-2020520)

## Backport package

This is a backport providers package for `snowflake` provider. All classes for this provider package
are in `airflow.providers.snowflake` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.



## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-snowflake`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package                | Version required   |
|:---------------------------|:-------------------|
| snowflake-connector-python | &gt;=1.5.2            |
| snowflake-sqlalchemy       | &gt;=1.1.0            |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.snowflake` package.


## Operators


### New operators

| New Airflow 2.0 operators: `airflow.providers.snowflake` package                                                                                                  |
|:------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.s3_to_snowflake.S3ToSnowflakeTransferOperator](https://github.com/apache/airflow/blob/master/airflow/providers/snowflake/operators/s3_to_snowflake.py) |
| [operators.snowflake.SnowflakeOperator](https://github.com/apache/airflow/blob/master/airflow/providers/snowflake/operators/snowflake.py)                         |







## Hooks


### New hooks

| New Airflow 2.0 hooks: `airflow.providers.snowflake` package                                                                  |
|:------------------------------------------------------------------------------------------------------------------------------|
| [hooks.snowflake.SnowflakeHook](https://github.com/apache/airflow/blob/master/airflow/providers/snowflake/hooks/snowflake.py) |







## Releases

### Release 2020.5.20

| Commit                                                                                         | Committed   | Subject                                                                 |
|:-----------------------------------------------------------------------------------------------|:------------|:------------------------------------------------------------------------|
| [00642a46d](https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c) | 2020-05-26  | Fixed name of 20 remaining wrongly named operators. (#8994)             |
| [1d36b0303](https://github.com/apache/airflow/commit/1d36b0303b8632fce6de78ca4e782ae26ee06fea) | 2020-05-23  | Fix references in docs (#8984)                                          |
| [375d1ca22](https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f) | 2020-05-19  | Release candidate 2 for backport packages 2020.05.20 (#8898)            |
| [12c5e5d8a](https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79) | 2020-05-17  | Prepare release candidate for backport packages (#8891)                 |
| [a546a10b1](https://github.com/apache/airflow/commit/a546a10b13b1f7a119071d8d2001cb17ccdcbbf7) | 2020-05-16  | Add Snowflake system test (#8422)                                       |
| [f3521fb0e](https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca) | 2020-05-16  | Regenerate readme files for backport package release (#8886)            |
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807) |
| [cd635dd7d](https://github.com/apache/airflow/commit/cd635dd7d57cab2f41efac2d3d94e8f80a6c96d6) | 2020-05-10  | [AIRFLOW-5906] Add authenticator parameter to snowflake_hook (#8642)    |
| [297ad3088](https://github.com/apache/airflow/commit/297ad30885eeb77c062f37df78a78f381e7d140e) | 2020-04-20  | Fix Snowflake hook conn id (#8423)                                      |
| [cf1109d66](https://github.com/apache/airflow/commit/cf1109d661991943bb4861a0468ba4bc8946376d) | 2020-02-07  | [AIRFLOW-6755] Fix snowflake hook bug and tests (#7380)                 |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                |
| [eee34ee80](https://github.com/apache/airflow/commit/eee34ee8080bb7bc81294c3fbd8be93bbf795367) | 2020-01-24  | [AIRFLOW-4204] Update super() calls (#7248)                             |
| [17af3beea](https://github.com/apache/airflow/commit/17af3beea5095d9aec81c06404614ca6d1057a45) | 2020-01-21  | [AIRFLOW-5816] Add S3 to snowflake operator (#6469)                     |

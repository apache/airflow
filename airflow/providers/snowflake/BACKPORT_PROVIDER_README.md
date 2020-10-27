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

Release: 2020.10.29

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [PIP requirements](#pip-requirements)
- [Cross provider package dependencies](#cross-provider-package-dependencies)
- [Provider class summary](#provider-classes-summary)
    - [Operators](#operators)
        - [New operators](#new-operators)
    - [Transfer operators](#transfer-operators)
        - [New transfer operators](#new-transfer-operators)
    - [Hooks](#hooks)
        - [New hooks](#new-hooks)
- [Releases](#releases)
    - [Release 2020.10.29](#release-20201029)
    - [Release 2020.10.5](#release-2020105)
    - [Release 2020.6.24](#release-2020624)

## Backport package

This is a backport providers package for `snowflake` provider. All classes for this provider package
are in `airflow.providers.snowflake` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.



## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-snowflake`

## PIP requirements

| PIP package                | Version required   |
|:---------------------------|:-------------------|
| snowflake-connector-python | &gt;=1.5.2            |
| snowflake-sqlalchemy       | &gt;=1.1.0            |

## Cross provider package dependencies

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified backport providers package in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

```bash
pip install apache-airflow-backport-providers-snowflake[slack]
```

| Dependent package                                                                                                | Extra   |
|:-----------------------------------------------------------------------------------------------------------------|:--------|
| [apache-airflow-backport-providers-slack](https://github.com/apache/airflow/tree/master/airflow/providers/slack) | slack   |

# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `snowflake` provider
are in the `airflow.providers.snowflake` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)


## Operators


### New operators

| New Airflow 2.0 operators: `airflow.providers.snowflake` package                                                                          |
|:------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.snowflake.SnowflakeOperator](https://github.com/apache/airflow/blob/master/airflow/providers/snowflake/operators/snowflake.py) |



## Transfer operators


### New transfer operators

| New Airflow 2.0 transfers: `airflow.providers.snowflake` package                                                                                                   |
|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [transfers.s3_to_snowflake.S3ToSnowflakeOperator](https://github.com/apache/airflow/blob/master/airflow/providers/snowflake/transfers/s3_to_snowflake.py)          |
| [transfers.snowflake_to_slack.SnowflakeToSlackOperator](https://github.com/apache/airflow/blob/master/airflow/providers/snowflake/transfers/snowflake_to_slack.py) |



## Hooks


### New hooks

| New Airflow 2.0 hooks: `airflow.providers.snowflake` package                                                                  |
|:------------------------------------------------------------------------------------------------------------------------------|
| [hooks.snowflake.SnowflakeHook](https://github.com/apache/airflow/blob/master/airflow/providers/snowflake/hooks/snowflake.py) |




## Releases

### Release 2020.10.29

| Commit                                                                                         | Committed   | Subject                                                          |
|:-----------------------------------------------------------------------------------------------|:------------|:-----------------------------------------------------------------|
| [b680bbc0b](https://github.com/apache/airflow/commit/b680bbc0b05ad71d403a5d58bc7023a2453b9a48) | 2020-10-24  | Generated backport providers readmes/setup for 2020.10.29        |
| [483068745](https://github.com/apache/airflow/commit/48306874538eea7cfd42358d5ebb59705204bfc4) | 2020-10-24  | Use Python 3 style super classes (#11806)                        |
| [349b0811c](https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a) | 2020-10-20  | Add D200 pydocstyle check (#11688)                               |
| [16e712971](https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746) | 2020-10-13  | Added support for provider packages for Airflow 2.0 (#11487)     |
| [d305876be](https://github.com/apache/airflow/commit/d305876bee328287ff391a29cc1cd632468cc731) | 2020-10-12  | Remove redundant None provided as default to dict.get() (#11448) |
| [0a0e1af80](https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa) | 2020-10-03  | Fix Broken Markdown links in Providers README TOC (#11249)       |


### Release 2020.10.5

| Commit                                                                                         | Committed   | Subject                                                               |
|:-----------------------------------------------------------------------------------------------|:------------|:----------------------------------------------------------------------|
| [ca4238eb4](https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13) | 2020-10-02  | Fixed month in backport packages to October (#11242)                  |
| [5220e4c38](https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5) | 2020-10-02  | Prepare Backport release 2020.09.07 (#11238)                          |
| [0161b5ea2](https://github.com/apache/airflow/commit/0161b5ea2b805d62a0317e5cab6f797b92c8abf1) | 2020-09-26  | Increasing type coverage for multiple provider (#11159)               |
| [9549274d1](https://github.com/apache/airflow/commit/9549274d110f689a0bd709db829a4d69e274eed9) | 2020-09-09  | Upgrade black to 20.8b1 (#10818)                                      |
| [fdd9b6f65](https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3) | 2020-08-25  | Enable Black on Providers Packages (#10543)                           |
| [d1bce91bb](https://github.com/apache/airflow/commit/d1bce91bb21d5a468fa6a0207156c28fe1ca6513) | 2020-08-25  | PyDocStyle: Enable D403: Capitalized first word of docstring (#10530) |
| [3696c34c2](https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34) | 2020-08-24  | Fix typo in the word &#34;release&#34; (#10528)                               |
| [ee7ca128a](https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94) | 2020-08-22  | Fix broken Markdown refernces in Providers README (#10483)            |
| [f6734b3b8](https://github.com/apache/airflow/commit/f6734b3b850d33d3712763f93c114e80f5af9ffb) | 2020-08-12  | Enable Sphinx spellcheck for doc generation (#10280)                  |
| [cdec30125](https://github.com/apache/airflow/commit/cdec3012542b45d23a05f62d69110944ba542e2a) | 2020-08-07  | Add correct signature to all operators and sensors (#10205)           |
| [24c8e4c2d](https://github.com/apache/airflow/commit/24c8e4c2d6e359ecc2c7d6275dccc68de4a82832) | 2020-08-06  | Changes to all the constructors to remove the args argument (#10163)  |
| [aeea71274](https://github.com/apache/airflow/commit/aeea71274d4527ff2351102e94aa38bda6099e7f) | 2020-08-02  | Remove `args` parameter from provider operator constructors (#10097)  |
| [7d24b088c](https://github.com/apache/airflow/commit/7d24b088cd736cfa18f9214e4c9d6ce2d5865f3d) | 2020-07-25  | Stop using start_date in default_args in example_dags (2) (#9985)     |
| [d0e7db402](https://github.com/apache/airflow/commit/d0e7db4024806af35e3c9a2cae460fdeedd4d2ec) | 2020-06-19  | Fixed release number for fresh release (#9408)                        |


### Release 2020.6.24

| Commit                                                                                         | Committed   | Subject                                                                 |
|:-----------------------------------------------------------------------------------------------|:------------|:------------------------------------------------------------------------|
| [12af6a080](https://github.com/apache/airflow/commit/12af6a08009b8776e00d8a0aab92363eb8c4e8b1) | 2020-06-19  | Final cleanup for 2020.6.23rc1 release preparation (#9404)              |
| [c7e5bce57](https://github.com/apache/airflow/commit/c7e5bce57fe7f51cefce4f8a41ce408ac5675d13) | 2020-06-19  | Prepare backport release candidate for 2020.6.23rc1 (#9370)             |
| [f6bd817a3](https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac) | 2020-06-16  | Introduce &#39;transfers&#39; packages (#9320)                                  |
| [1c9374d25](https://github.com/apache/airflow/commit/1c9374d2573483dd66f5c35032e24140864e72c0) | 2020-06-03  | Add snowflake to slack operator (#9023)                                 |
| [0b0e4f7a4](https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34) | 2020-05-26  | Preparing for RC3 release of backports (#9026)                           |
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

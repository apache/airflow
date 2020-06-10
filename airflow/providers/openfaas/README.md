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


# Package apache-airflow-backport-providers-openfaas

Release: 2020.5.20

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [Provider class summary](#provider-class-summary)
    - [Hooks](#hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 2020.5.20](#release-2020520)

## Backport package

This is a backport providers package for `openfaas` provider. All classes for this provider package
are in `airflow.providers.openfaas` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.



## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-openfaas`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.openfaas` package.







## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.openfaas` package                                                                   | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                           |
|:--------------------------------------------------------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------|
| [hooks.openfaas.OpenFaasHook](https://github.com/apache/airflow/blob/master/airflow/providers/openfaas/hooks/openfaas.py) | [contrib.hooks.openfaas_hook.OpenFaasHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/openfaas_hook.py) |






## Releases

### Release 2020.5.20

| Commit                                                                                         | Committed   | Subject                                                                 |
|:-----------------------------------------------------------------------------------------------|:------------|:------------------------------------------------------------------------|
| [0b0e4f7a4](https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34) | 2020-05-26  | Preparing for RC3 relase of backports (#9026)                           |
| [00642a46d](https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c) | 2020-05-26  | Fixed name of 20 remaining wrongly named operators. (#8994)             |
| [375d1ca22](https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f) | 2020-05-19  | Release candidate 2 for backport packages 2020.05.20 (#8898)            |
| [12c5e5d8a](https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79) | 2020-05-17  | Prepare release candidate for backport packages (#8891)                 |
| [f3521fb0e](https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca) | 2020-05-16  | Regenerate readme files for backport package release (#8886)            |
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807) |
| [4bde99f13](https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a) | 2020-03-23  | Make airflow/providers pylint compatible (#7802)                        |
| [05443c6dc](https://github.com/apache/airflow/commit/05443c6dc8100e791446bbcc0df04de6e34017bb) | 2020-03-23  | Add missing call to Super class in remaining providers (#7828)          |
| [5f784ae5c](https://github.com/apache/airflow/commit/5f784ae5c0e629ebe117874029b4a9d789587be0) | 2020-03-14  | [AIRFLOW-7061] Rename openfass to openfaas (#7721)                      |

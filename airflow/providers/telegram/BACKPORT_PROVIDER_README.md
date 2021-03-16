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


# Package apache-airflow-backport-providers-telegram

Release: 2021.3.3

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-classes-summary)
    - [Operators](#operators)
        - [New operators](#new-operators)
    - [Hooks](#hooks)
        - [New hooks](#new-hooks)
- [Releases](#releases)
    - [Release 2021.3.3](#release-202133)

## Backport package

This is a backport providers package for `telegram` provider. All classes for this provider package
are in `airflow.providers.telegram` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.



## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-telegram`

## PIP requirements

| PIP package           | Version required   |
|:----------------------|:-------------------|
| `python-telegram-bot` | `==13.0`           |

# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `telegram` provider
are in the `airflow.providers.telegram` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)


## Operators


### New operators

| New Airflow 2.0 operators: `airflow.providers.telegram` package                                                                       |
|:--------------------------------------------------------------------------------------------------------------------------------------|
| [operators.telegram.TelegramOperator](https://github.com/apache/airflow/blob/master/airflow/providers/telegram/operators/telegram.py) |



## Hooks


### New hooks

| New Airflow 2.0 hooks: `airflow.providers.telegram` package                                                               |
|:--------------------------------------------------------------------------------------------------------------------------|
| [hooks.telegram.TelegramHook](https://github.com/apache/airflow/blob/master/airflow/providers/telegram/hooks/telegram.py) |




## Releases

### Release 2021.3.3

| Commit                                                                                         | Committed   | Subject                                                               |
|:-----------------------------------------------------------------------------------------------|:------------|:----------------------------------------------------------------------|
| [106d2c85e](https://github.com/apache/airflow/commit/106d2c85ec4a240605830bf41962c0197b003135) | 2021-02-10  | `Fix the AttributeError with text field in TelegramOperator (#13990)` |
| [88bdcfa0d](https://github.com/apache/airflow/commit/88bdcfa0df5bcb4c489486e05826544b428c8f43) | 2021-02-04  | `Prepare to release a new wave of providers. (#14013)`                |
| [ac2f72c98](https://github.com/apache/airflow/commit/ac2f72c98dc0821b33721054588adbf2bb53bb0b) | 2021-02-01  | `Implement provider versioning tools (#13767)`                        |
| [3fd5ef355](https://github.com/apache/airflow/commit/3fd5ef355556cf0ad7896bb570bbe4b2eabbf46e) | 2021-01-21  | `Add missing logos for integrations (#13717)`                         |
| [295d66f91](https://github.com/apache/airflow/commit/295d66f91446a69610576d040ba687b38f1c5d0a) | 2020-12-30  | `Fix Grammar in PIP warning (#13380)`                                 |
| [6cf76d7ac](https://github.com/apache/airflow/commit/6cf76d7ac01270930de7f105fb26428763ee1d4e) | 2020-12-18  | `Fix typo in pip upgrade command :( (#13148)`                         |
| [32971a1a2](https://github.com/apache/airflow/commit/32971a1a2de1db0b4f7442ed26facdf8d3b7a36f) | 2020-12-09  | `Updates providers versions to 1.0.0 (#12955)`                        |
| [b40dffa08](https://github.com/apache/airflow/commit/b40dffa08547b610162f8cacfa75847f3c4ca364) | 2020-12-08  | `Rename remaing modules to match AIP-21 (#12917)`                     |
| [cd66450b4](https://github.com/apache/airflow/commit/cd66450b4ee2a219ddc847970255e420ed679700) | 2020-12-05  | `Add Telegram hook and operator (#11850)`                             |

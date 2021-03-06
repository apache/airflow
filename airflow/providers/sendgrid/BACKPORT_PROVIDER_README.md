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


# Package apache-airflow-backport-providers-sendgrid

Release: 2021.3.3

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-classes-summary)
- [Releases](#releases)
    - [Release 2021.3.3](#release-202133)
    - [Release 2021.2.5](#release-202125)

## Backport package

This is a backport providers package for `sendgrid` provider. All classes for this provider package
are in `airflow.providers.sendgrid` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.


## Release 2021.3.3

### Bug fixes

* `Corrections in docs and tools after releasing provider RCs (#14082)`

## Release 2021.2.5

  * `Deprecate email credentials from environment variables. (#13601)`


## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-sendgrid`

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| `sendgrid`    | `>=6.0.0,<7`       |

# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `sendgrid` provider
are in the `airflow.providers.sendgrid` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)



## Releases

### Release 2021.3.3

| Commit                                                                                         | Committed   | Subject                                                               |
|:-----------------------------------------------------------------------------------------------|:------------|:----------------------------------------------------------------------|
| [10343ec29](https://github.com/apache/airflow/commit/10343ec29f8f0abc5b932ba26faf49bc63c6bcda) | 2021-02-05  | `Corrections in docs and tools after releasing provider RCs (#14082)` |


### Release 2021.2.5

| Commit                                                                                         | Committed   | Subject                                                            |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------|
| [88bdcfa0d](https://github.com/apache/airflow/commit/88bdcfa0df5bcb4c489486e05826544b428c8f43) | 2021-02-04  | `Prepare to release a new wave of providers. (#14013)`             |
| [ac2f72c98](https://github.com/apache/airflow/commit/ac2f72c98dc0821b33721054588adbf2bb53bb0b) | 2021-02-01  | `Implement provider versioning tools (#13767)`                     |
| [86695b62a](https://github.com/apache/airflow/commit/86695b62a0281364088642fa3dc17d92cf9e7cbe) | 2021-01-30  | `Deprecate email credentials from environment variables. (#13601)` |
| [295d66f91](https://github.com/apache/airflow/commit/295d66f91446a69610576d040ba687b38f1c5d0a) | 2020-12-30  | `Fix Grammar in PIP warning (#13380)`                              |
| [6cf76d7ac](https://github.com/apache/airflow/commit/6cf76d7ac01270930de7f105fb26428763ee1d4e) | 2020-12-18  | `Fix typo in pip upgrade command :( (#13148)`                      |
| [32971a1a2](https://github.com/apache/airflow/commit/32971a1a2de1db0b4f7442ed26facdf8d3b7a36f) | 2020-12-09  | `Updates providers versions to 1.0.0 (#12955)`                     |
| [c34ef853c](https://github.com/apache/airflow/commit/c34ef853c890e08f5468183c03dc8f3f3ce84af2) | 2020-11-20  | `Separate out documentation building per provider  (#12444)`       |
| [008035450](https://github.com/apache/airflow/commit/00803545023b096b8db4fbd6eb473843096d7ce4) | 2020-11-18  | `Update provider READMEs for 1.0.0b2 batch release (#12449)`       |
| [7825e8f59](https://github.com/apache/airflow/commit/7825e8f59034645ab3247229be83a3aa90baece1) | 2020-11-13  | `Docs installation improvements (#12304)`                          |
| [c5806efb5](https://github.com/apache/airflow/commit/c5806efb54ad06049e13a5fc7df2f03846fe566e) | 2020-11-10  | `Added missing sendgrid readme (#12245)`                           |
| [349b0811c](https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a) | 2020-10-20  | `Add D200 pydocstyle check (#11688)`                               |
| [d305876be](https://github.com/apache/airflow/commit/d305876bee328287ff391a29cc1cd632468cc731) | 2020-10-12  | `Remove redundant None provided as default to dict.get() (#11448)` |
| [f3e87c503](https://github.com/apache/airflow/commit/f3e87c503081a3085dff6c7352640d7f08beb5bc) | 2020-09-22  | `Add D202 pydocstyle check (#11032)`                               |
| [fdd9b6f65](https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3) | 2020-08-25  | `Enable Black on Providers Packages (#10543)`                      |
| [7269d15ad](https://github.com/apache/airflow/commit/7269d15adfb74188359757b1705485f5d368486a) | 2020-08-03  | `[GH-9708] Add type coverage to Sendgrid module (#10134)`          |
| [a97400d0d](https://github.com/apache/airflow/commit/a97400d0d89ccd6de0cab3a50c58a2969d164a0d) | 2020-06-28  | `Move out sendgrid emailer from airflow.contrib (#9355)`           |

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


# Package apache-airflow-backport-providers-plexus

Release: 2021.3.17

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
    - [Release 2021.3.17](#release-2021317)
    - [Release 2020.10.29](#release-20201029)
    - [Release 2020.10.5](#release-2020105)

## Backport package

This is a backport providers package for `plexus` provider. All classes for this provider package
are in `airflow.providers.plexus` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.


## Release 2021.3.17

### Bug fixes

* `The 2020.10.29 relese was broken  (#14673)`
* `Switch to f-strings using flynt. (#13732)`
* `Rename remaing modules to match AIP-21 (#12917)`


## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-plexus`

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| `arrow`       | `>=0.16.0`         |

# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `plexus` provider
are in the `airflow.providers.plexus` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)


## Operators


### New operators

| New Airflow 2.0 operators: `airflow.providers.plexus` package                                                              |
|:---------------------------------------------------------------------------------------------------------------------------|
| [operators.job.PlexusJobOperator](https://github.com/apache/airflow/blob/master/airflow/providers/plexus/operators/job.py) |



## Hooks


### New hooks

| New Airflow 2.0 hooks: `airflow.providers.plexus` package                                                         |
|:------------------------------------------------------------------------------------------------------------------|
| [hooks.plexus.PlexusHook](https://github.com/apache/airflow/blob/master/airflow/providers/plexus/hooks/plexus.py) |




## Releases

### Release 2021.3.17

| Commit                                                                                         | Committed   | Subject                                                                          |
|:-----------------------------------------------------------------------------------------------|:------------|:---------------------------------------------------------------------------------|
| [88bdcfa0d](https://github.com/apache/airflow/commit/88bdcfa0df5bcb4c489486e05826544b428c8f43) | 2021-02-04  | `Prepare to release a new wave of providers. (#14013)`                           |
| [ac2f72c98](https://github.com/apache/airflow/commit/ac2f72c98dc0821b33721054588adbf2bb53bb0b) | 2021-02-01  | `Implement provider versioning tools (#13767)`                                   |
| [a9ac2b040](https://github.com/apache/airflow/commit/a9ac2b040b64de1aa5d9c2b9def33334e36a8d22) | 2021-01-23  | `Switch to f-strings using flynt. (#13732)`                                      |
| [3fd5ef355](https://github.com/apache/airflow/commit/3fd5ef355556cf0ad7896bb570bbe4b2eabbf46e) | 2021-01-21  | `Add missing logos for integrations (#13717)`                                    |
| [295d66f91](https://github.com/apache/airflow/commit/295d66f91446a69610576d040ba687b38f1c5d0a) | 2020-12-30  | `Fix Grammar in PIP warning (#13380)`                                            |
| [6cf76d7ac](https://github.com/apache/airflow/commit/6cf76d7ac01270930de7f105fb26428763ee1d4e) | 2020-12-18  | `Fix typo in pip upgrade command :( (#13148)`                                    |
| [32971a1a2](https://github.com/apache/airflow/commit/32971a1a2de1db0b4f7442ed26facdf8d3b7a36f) | 2020-12-09  | `Updates providers versions to 1.0.0 (#12955)`                                   |
| [b40dffa08](https://github.com/apache/airflow/commit/b40dffa08547b610162f8cacfa75847f3c4ca364) | 2020-12-08  | `Rename remaing modules to match AIP-21 (#12917)`                                |
| [c34ef853c](https://github.com/apache/airflow/commit/c34ef853c890e08f5468183c03dc8f3f3ce84af2) | 2020-11-20  | `Separate out documentation building per provider  (#12444)`                     |
| [008035450](https://github.com/apache/airflow/commit/00803545023b096b8db4fbd6eb473843096d7ce4) | 2020-11-18  | `Update provider READMEs for 1.0.0b2 batch release (#12449)`                     |
| [ae7cb4a1e](https://github.com/apache/airflow/commit/ae7cb4a1e2a96351f1976cf5832615e24863e05d) | 2020-11-17  | `Update wrong commit hash in backport provider changes (#12390)`                 |
| [6889a333c](https://github.com/apache/airflow/commit/6889a333cff001727eb0a66e375544a28c9a5f03) | 2020-11-15  | `Improvements for operators and hooks ref docs (#12366)`                         |
| [7825e8f59](https://github.com/apache/airflow/commit/7825e8f59034645ab3247229be83a3aa90baece1) | 2020-11-13  | `Docs installation improvements (#12304)`                                        |
| [85a18e13d](https://github.com/apache/airflow/commit/85a18e13d9dec84275283ff69e34704b60d54a75) | 2020-11-09  | `Point at pypi project pages for cross-dependency of provider packages (#12212)` |
| [59eb5de78](https://github.com/apache/airflow/commit/59eb5de78c70ee9c7ae6e4cba5c7a2babb8103ca) | 2020-11-09  | `Update provider READMEs for up-coming 1.0.0beta1 releases (#12206)`             |
| [b2a28d159](https://github.com/apache/airflow/commit/b2a28d1590410630d66966aa1f2b2a049a8c3b32) | 2020-11-09  | `Moves provider packages scripts to dev (#12082)`                                |
| [4e8f9cc8d](https://github.com/apache/airflow/commit/4e8f9cc8d02b29c325b8a5a76b4837671bdf5f68) | 2020-11-03  | `Enable Black - Python Auto Formmatter (#9550)`                                  |
| [8c42cf1b0](https://github.com/apache/airflow/commit/8c42cf1b00c90f0d7f11b8a3a455381de8e003c5) | 2020-11-03  | `Use PyUpgrade to use Python 3.6 features (#11447)`                              |
| [5a439e84e](https://github.com/apache/airflow/commit/5a439e84eb6c0544dc6c3d6a9f4ceeb2172cd5d0) | 2020-10-26  | `Prepare providers release 0.0.2a1 (#11855)`                                     |


### Release 2020.10.29

| Commit                                                                                         | Committed   | Subject                                                      |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------|
| [b680bbc0b](https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574) | 2020-10-24  | Generated backport providers readmes/setup for 2020.10.29    |
| [56d72e3ff](https://github.com/apache/airflow/commit/56d72e3ff8798a2662847355d1b73b2c1f57b31f) | 2020-10-24  | Replace non-empty sets with set literals (#11810)            |
| [16e712971](https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746) | 2020-10-13  | Added support for provider packages for Airflow 2.0 (#11487) |
| [0a0e1af80](https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa) | 2020-10-03  | Fix Broken Markdown links in Providers README TOC (#11249)   |


### Release 2020.10.5

| Commit                                                                                         | Committed   | Subject                                                 |
|:-----------------------------------------------------------------------------------------------|:------------|:--------------------------------------------------------|
| [ca4238eb4](https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13) | 2020-10-02  | Fixed month in backport packages to October (#11242)    |
| [5220e4c38](https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5) | 2020-10-02  | Prepare Backport release 2020.09.07 (#11238)            |
| [0161b5ea2](https://github.com/apache/airflow/commit/0161b5ea2b805d62a0317e5cab6f797b92c8abf1) | 2020-09-26  | Increasing type coverage for multiple provider (#11159) |
| [b9dc3c51b](https://github.com/apache/airflow/commit/b9dc3c51ba2cba1c61d327488cecf2623d6445b3) | 2020-09-10  | Added Plexus as an Airflow provider (#10591)            |

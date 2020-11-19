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


# Package apache-airflow-backport-providers-apache-kylin

Release: 2020.10.29

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
    - [Release 2020.10.29](#release-20201029)
    - [Release 2020.10.5](#release-2020105)

## Backport package

This is a backport providers package for `apache.kylin` provider. All classes for this provider package
are in `airflow.providers.apache.kylin` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.



## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-apache-kylin`

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| kylinpy       | &gt;=2.6              |

# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `apache.kylin` provider
are in the `airflow.providers.apache.kylin` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)


## Operators


### New operators

| New Airflow 2.0 operators: `airflow.providers.apache.kylin` package                                                                            |
|:-----------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.kylin_cube.KylinCubeOperator](https://github.com/apache/airflow/blob/master/airflow/providers/apache/kylin/operators/kylin_cube.py) |



## Hooks


### New hooks

| New Airflow 2.0 hooks: `airflow.providers.apache.kylin` package                                                      |
|:---------------------------------------------------------------------------------------------------------------------|
| [hooks.kylin.KylinHook](https://github.com/apache/airflow/blob/master/airflow/providers/apache/kylin/hooks/kylin.py) |




## Releases

### Release 2020.10.29

| Commit                                                                                         | Committed   | Subject                                                      |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------|
| [b680bbc0b](https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574) | 2020-10-24  | Generated backport providers readmes/setup for 2020.10.29    |
| [16e712971](https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746) | 2020-10-13  | Added support for provider packages for Airflow 2.0 (#11487) |
| [0a0e1af80](https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa) | 2020-10-03  | Fix Broken Markdown links in Providers README TOC (#11249)   |


### Release 2020.10.5

| Commit                                                                                         | Committed   | Subject                                                               |
|:-----------------------------------------------------------------------------------------------|:------------|:----------------------------------------------------------------------|
| [ca4238eb4](https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13) | 2020-10-02  | Fixed month in backport packages to October (#11242)                  |
| [5220e4c38](https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5) | 2020-10-02  | Prepare Backport release 2020.09.07 (#11238)                          |
| [99accec29](https://github.com/apache/airflow/commit/99accec29d71b0a57fd4e90151b9d4d10321be07) | 2020-09-25  | Fix incorrect Usage of Optional[str] &amp; Optional[int] (#11141)         |
| [e3f96ce7a](https://github.com/apache/airflow/commit/e3f96ce7a8ac098aeef5e9930e6de6c428274d57) | 2020-09-24  | Fix incorrect Usage of Optional[bool] (#11138)                        |
| [fdd9b6f65](https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3) | 2020-08-25  | Enable Black on Providers Packages (#10543)                           |
| [d1bce91bb](https://github.com/apache/airflow/commit/d1bce91bb21d5a468fa6a0207156c28fe1ca6513) | 2020-08-25  | PyDocStyle: Enable D403: Capitalized first word of docstring (#10530) |
| [f6734b3b8](https://github.com/apache/airflow/commit/f6734b3b850d33d3712763f93c114e80f5af9ffb) | 2020-08-12  | Enable Sphinx spellcheck for doc generation (#10280)                  |
| [b43f90abf](https://github.com/apache/airflow/commit/b43f90abf4c7219d5d59cccb0514256bd3f2fdc7) | 2020-08-09  | Fix various typos in the repo (#10263)                                |
| [edc51e313](https://github.com/apache/airflow/commit/edc51e313b50359e0258cce5f7f7283f69342fb9) | 2020-08-08  | Remove Unnecessary list literal in Tuple for Kylin Operator (#10252)  |
| [3b3287d7a](https://github.com/apache/airflow/commit/3b3287d7acc76430f12b758d52cec61c7f74e726) | 2020-08-05  | Enforce keyword only arguments on apache operators (#10170)           |
| [7d24b088c](https://github.com/apache/airflow/commit/7d24b088cd736cfa18f9214e4c9d6ce2d5865f3d) | 2020-07-25  | Stop using start_date in default_args in example_dags (2) (#9985)     |
| [33f0cd265](https://github.com/apache/airflow/commit/33f0cd2657b2e77ea3477e0c93f13f1474be628e) | 2020-07-22  | apply_default keeps the function signature for mypy (#9784)           |
| [a2c5389a6](https://github.com/apache/airflow/commit/a2c5389a60f68482a60eb40c67b1542d827c187e) | 2020-07-14  | Add kylin operator (#9149)                                            |

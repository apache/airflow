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


# Package apache-airflow-providers-plexus

Release: 0.0.1

**Table of contents**

- [Provider package](#provider-package)
- [Installation](#installation)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-classes-summary)
    - [Operators](#operators)
        - [New operators](#new-operators)
    - [Hooks](#hooks)
        - [New hooks](#new-hooks)
- [Releases](#releases)
    - [Release 0.0.1](#release-001)

## Provider package

This is a provider package for `plexus` provider. All classes for this provider package
are in `airflow.providers.plexus` python package.



## Installation

You can install this package on top of an existing airflow 2.* installation via
`pip install apache-airflow-providers-plexus`

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| arrow         | &gt;=0.16.0           |

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

### Release 0.0.1

| Commit                                                                                         | Committed   | Subject                                                    |
|:-----------------------------------------------------------------------------------------------|:------------|:-----------------------------------------------------------|
| [0a0e1af80](https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa) | 2020-10-03  | Fix Broken Markdown links in Providers README TOC (#11249) |
| [ca4238eb4](https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13) | 2020-10-02  | Fixed month in backport packages to October (#11242)       |
| [5220e4c38](https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5) | 2020-10-02  | Prepare Backport release 2020.09.07 (#11238)               |
| [0161b5ea2](https://github.com/apache/airflow/commit/0161b5ea2b805d62a0317e5cab6f797b92c8abf1) | 2020-09-26  | Increasing type coverage for multiple provider (#11159)    |
| [b9dc3c51b](https://github.com/apache/airflow/commit/b9dc3c51ba2cba1c61d327488cecf2623d6445b3) | 2020-09-10  | Added Plexus as an Airflow provider (#10591)               |

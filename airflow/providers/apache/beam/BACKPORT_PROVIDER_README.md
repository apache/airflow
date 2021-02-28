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


# Package apache-airflow-backport-providers-apache-beam

Release: 2021.3.3

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [PIP requirements](#pip-requirements)
- [Cross provider package dependencies](#cross-provider-package-dependencies)
- [Provider class summary](#provider-classes-summary)
    - [Operators](#operators)
        - [New operators](#new-operators)
    - [Hooks](#hooks)
        - [New hooks](#new-hooks)
- [Releases](#releases)
    - [Release 2021.3.3](#release-202133)
    - [Release 2021.2.5](#release-202125)

## Backport package

This is a backport providers package for `apache.beam` provider. All classes for this provider package
are in `airflow.providers.apache.beam` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.


## Release 2021.3.3

### Bug fixes

* `Improve Apache Beam operators - refactor operator - common Dataflow logic (#14094)`
* `Corrections in docs and tools after releasing provider RCs (#14082)`


## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-apache-beam`

## PIP requirements

| PIP package        | Version required   |
|:-------------------|:-------------------|
| `apache-beam[gcp]` |                    |

## Cross provider package dependencies

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified backport providers package in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

```bash
pip install apache-airflow-backport-providers-apache-beam[google]
```

| Dependent package                                                                                                  | Extra    |
|:-------------------------------------------------------------------------------------------------------------------|:---------|
| [apache-airflow-backport-providers-google](https://github.com/apache/airflow/tree/master/airflow/providers/google) | `google` |

# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `apache.beam` provider
are in the `airflow.providers.apache.beam` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)


## Operators


### New operators

| New Airflow 2.0 operators: `airflow.providers.apache.beam` package                                                                            |
|:----------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.beam.BeamRunJavaPipelineOperator](https://github.com/apache/airflow/blob/master/airflow/providers/apache/beam/operators/beam.py)   |
| [operators.beam.BeamRunPythonPipelineOperator](https://github.com/apache/airflow/blob/master/airflow/providers/apache/beam/operators/beam.py) |



## Hooks


### New hooks

| New Airflow 2.0 hooks: `airflow.providers.apache.beam` package                                                   |
|:-----------------------------------------------------------------------------------------------------------------|
| [hooks.beam.BeamHook](https://github.com/apache/airflow/blob/master/airflow/providers/apache/beam/hooks/beam.py) |




## Releases

### Release 2021.3.3

| Commit                                                                                         | Committed   | Subject                                                                              |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------------------------|
| [8a731f536](https://github.com/apache/airflow/commit/8a731f536cc946cc62c20921187354b828df931e) | 2021-02-05  | `Improve Apache Beam operators - refactor operator - common Dataflow logic (#14094)` |
| [10343ec29](https://github.com/apache/airflow/commit/10343ec29f8f0abc5b932ba26faf49bc63c6bcda) | 2021-02-05  | `Corrections in docs and tools after releasing provider RCs (#14082)`                |


### Release 2021.2.5

| Commit                                                                                         | Committed   | Subject                                                                   |
|:-----------------------------------------------------------------------------------------------|:------------|:--------------------------------------------------------------------------|
| [d45739f7c](https://github.com/apache/airflow/commit/d45739f7ce0de183329d67fff88a9da3943a9280) | 2021-02-04  | `Fixes to release process after releasing 2nd wave of providers (#14059)` |
| [1872d8719](https://github.com/apache/airflow/commit/1872d8719d24f94aeb1dcba9694837070b9884ca) | 2021-02-03  | `Add Apache Beam operators (#12814)`                                      |

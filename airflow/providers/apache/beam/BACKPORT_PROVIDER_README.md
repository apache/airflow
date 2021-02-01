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

Release:

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [PIP requirements](#pip-requirements)
- [Cross provider package dependencies](#cross-provider-package-dependencies)
- [Provider class summary](#provider-classes-summary)
    - [Operators](#operators)
        - [Moved operators](#moved-operators)
    - [Transfer operators](#transfer-operators)
        - [Moved transfer operators](#moved-transfer-operators)
    - [Hooks](#hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release](#release)

## Backport package

This is a backport providers package for `apache.beam` provider. All classes for this provider package
are in `airflow.providers.apache.beam` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.


## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-apache-beam`

## Cross provider package dependencies

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified backport providers package in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

```bash
pip install apache-airflow-beckport-providers-apache-beam[google]
```

| Dependent package                                                                                         | Extra       |
|:----------------------------------------------------------------------------------------------------------|:------------|
| [apache-airflow-providers-apache-google](https://pypi.org/project/apache-airflow-providers-apache-google) | google      |


# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `apache.beam` provider
are in the `airflow.providers.apache.beam` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)


## Operators

### New operators

| New Airflow 2.0 operators: `airflow.providers.apache.beam` package                                                                                                                 |
|:-----------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.beam.BeamRunJavaPipelineOperator](https://github.com/apache/airflow/blob/master/airflow/providers/apache/beam/operators/beam.py)    |
| [operators.beam.BeamRunPythonPipelineOperator](https://github.com/apache/airflow/blob/master/airflow/providers/apache/beam/operators/beam.py)  |


## Hooks

### New hooks

| New Airflow 2.0 hooks: `airflow.providers.apache.beam` package                                                   |
|:-----------------------------------------------------------------------------------------------------------------|
| [hooks.beam.BeamHook](https://github.com/apache/airflow/blob/master/airflow/providers/apache/beam/hooks/beam.py) |


## Releases

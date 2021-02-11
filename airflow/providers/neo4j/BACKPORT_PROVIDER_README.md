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


# Package apache-airflow-backport-providers-neo4j

Release: 2021.2.5

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
    - [Release 2021.2.5](#release-202125)

## Backport package

This is a backport providers package for `neo4j` provider. All classes for this provider package
are in `airflow.providers.neo4j` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.



## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-neo4j`

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| `neo4j`       | `>=4.2.1`          |

# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `neo4j` provider
are in the `airflow.providers.neo4j` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)


## Operators


### New operators

| New Airflow 2.0 operators: `airflow.providers.neo4j` package                                                              |
|:--------------------------------------------------------------------------------------------------------------------------|
| [operators.neo4j.Neo4jOperator](https://github.com/apache/airflow/blob/master/airflow/providers/neo4j/operators/neo4j.py) |



## Hooks


### New hooks

| New Airflow 2.0 hooks: `airflow.providers.neo4j` package                                                      |
|:--------------------------------------------------------------------------------------------------------------|
| [hooks.neo4j.Neo4jHook](https://github.com/apache/airflow/blob/master/airflow/providers/neo4j/hooks/neo4j.py) |




## Releases

### Release 2021.2.5

| Commit                                                                                         | Committed   | Subject                                        |
|:-----------------------------------------------------------------------------------------------|:------------|:-----------------------------------------------|
| [ac2f72c98](https://github.com/apache/airflow/commit/ac2f72c98dc0821b33721054588adbf2bb53bb0b) | 2021-02-01  | `Implement provider versioning tools (#13767)` |
| [1d2977f6a](https://github.com/apache/airflow/commit/1d2977f6a4c67fa6174c79dcdc4e9ee3ce06f1b1) | 2021-01-14  | `Add Neo4j hook and operator (#13324)`         |

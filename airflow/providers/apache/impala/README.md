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

# Package apache-airflow-backport-providers-apache-impala

 Release: 1.0.0b2

**Table of contents**

- [Provider package](#provider-package)
- [Installation](#installation)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-classes-summary)
    - [Hooks](#hooks)


## Provider package

This is a provider package for `apache.impala` provider. All classes for this provider package
are in `airflow.providers.apache.impala` python package.

## Installation

You can install this package on top of an existing airflow 2.* installation via
`pip install apache-airflow-providers-apache-impala`

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| impyla        | &gt;=0.16.3        |

# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `apache.impala` provider
are in the `airflow.providers.apache.impala` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)

## Hooks

| Airflow 2.0 hooks: `airflow.providers.apache.impala` package                                                          |
|:----------------------------------------------------------------------------------------------------------------------|
| [hooks.impala.ImpalaHook](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hive/hooks/hive.py)  |

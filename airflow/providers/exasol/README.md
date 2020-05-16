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


# Package apache-airflow-backport-providers-exasol

Release: 2020.05.19

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-class-summary)
    - [Operators](#operators)
        - [New operators](#new-operators)
    - [Hooks](#hooks)
        - [New hooks](#new-hooks)
- [Releases](#releases)
    - [Release 2020.05.19](#release-20200519)

## Backport package

This is a backport providers package for `exasol` provider. All classes for this provider package
are in `airflow.providers.exasol` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-exasol`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| pyexasol      | &gt;=0.5.1,&lt;1.0.0     |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.exasol` package.


## Operators


### New operators

| New Airflow 2.0 operators: `airflow.providers.exasol` package                                                                 |
|:------------------------------------------------------------------------------------------------------------------------------|
| [operators.exasol.ExasolOperator](https://github.com/apache/airflow/blob/master/airflow/providers/exasol/operators/exasol.py) |







## Hooks


### New hooks

| New Airflow 2.0 hooks: `airflow.providers.exasol` package                                                         |
|:------------------------------------------------------------------------------------------------------------------|
| [hooks.exasol.ExasolHook](https://github.com/apache/airflow/blob/master/airflow/providers/exasol/hooks/exasol.py) |







## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                 |
|:-----------------------------------------------------------------------------------------------|:------------|:------------------------------------------------------------------------|
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807) |
| [69dc91b4e](https://github.com/apache/airflow/commit/69dc91b4ef92d0f89abe097afd27bbe7ec2febd0) | 2020-04-02  | [AIRFLOW-6982] add native python exasol support (#7621)                 |

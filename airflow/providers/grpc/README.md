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


# Package apache-airflow-backport-providers-grpc

Release: 2020.05.19

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-class-summary)
    - [Operators](#operators)
        - [Moved operators](#moved-operators)
    - [Hooks](#hooks)
        - [New hooks](#new-hooks)
- [Releases](#releases)
    - [Release 2020.05.19](#release-20200519)

## Backport package

This is a backport providers package for `grpc` provider. All classes for this provider package
are in `airflow.providers.grpc` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-grpc`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| grpcio        | &gt;=1.15.0           |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.grpc` package.


## Operators




### Moved operators

| Airflow 2.0 operators: `airflow.providers.grpc` package                                                               | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                   |
|:----------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.grpc.GrpcOperator](https://github.com/apache/airflow/blob/master/airflow/providers/grpc/operators/grpc.py) | [contrib.operators.grpc_operator.GrpcOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/grpc_operator.py) |





## Hooks


### New hooks

| New Airflow 2.0 hooks: `airflow.providers.grpc` package                                                   |
|:----------------------------------------------------------------------------------------------------------|
| [hooks.grpc.GrpcHook](https://github.com/apache/airflow/blob/master/airflow/providers/grpc/hooks/grpc.py) |







## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                       |
|:-----------------------------------------------------------------------------------------------|:------------|:------------------------------------------------------------------------------|
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807)       |
| [cb0bf4a14](https://github.com/apache/airflow/commit/cb0bf4a142656ee40b43a01660b6f6b08a9840fa) | 2020-03-30  | Remove sql like function in base_hook (#7901)                                 |
| [be2b2baa7](https://github.com/apache/airflow/commit/be2b2baa7c5f53c2d73646e4623cdb6731551b70) | 2020-03-23  | Add missing call to Super class in &#39;http&#39;, &#39;grpc&#39; &amp; &#39;slack&#39; providers (#7826) |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                      |
| [cf141506a](https://github.com/apache/airflow/commit/cf141506a25dbba279b85500d781f7e056540721) | 2020-02-02  | [AIRFLOW-6708] Set unique logger names (#7330)                                |
| [9a04013b0](https://github.com/apache/airflow/commit/9a04013b0e40b0d744ff4ac9f008491806d60df2) | 2020-01-27  | [AIRFLOW-6646][AIP-21] Move protocols classes to providers package (#7268)    |

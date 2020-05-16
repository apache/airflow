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


# Package apache-airflow-backport-providers-singularity

Release: 2020.05.19

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-class-summary)
    - [Operators](#operators)
        - [New operators](#new-operators)
- [Releases](#releases)
    - [Release 2020.05.19](#release-20200519)

## Backport package

This is a backport providers package for `singularity` provider. All classes for this provider package
are in `airflow.providers.singularity` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-singularity`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| spython       | &gt;=0.0.56           |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.singularity` package.


## Operators


### New operators

| New Airflow 2.0 operators: `airflow.providers.singularity` package                                                                                |
|:--------------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.singularity.SingularityOperator](https://github.com/apache/airflow/blob/master/airflow/providers/singularity/operators/singularity.py) |











## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                          |
|:-----------------------------------------------------------------------------------------------|:------------|:---------------------------------------------------------------------------------|
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807)          |
| [42c59755a](https://github.com/apache/airflow/commit/42c59755affd49cd35bea8464e2a4c9256084d88) | 2020-05-09  | Update example SingularityOperator DAG (#8790)                                   |
| [3320e432a](https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc) | 2020-02-24  | [AIRFLOW-6817] Lazy-load `airflow.DAG` to keep user-facing API untouched (#7517) |
| [0bb687990](https://github.com/apache/airflow/commit/0bb687990b94da7445f4ba081592de8cea73119e) | 2020-02-23  | [AIRFLOW-4030] second attempt to add singularity to airflow (#7191)              |

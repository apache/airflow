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


# Package apache-airflow-backport-providers-tableau

Release: 2021.3.3

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-classes-summary)
    - [Operators](#operators)
        - [New operators](#new-operators)
    - [Sensors](#sensors)
        - [New sensors](#new-sensors)
    - [Hooks](#hooks)
        - [New hooks](#new-hooks)
- [Releases](#releases)
    - [Release 2021.3.3](#release-202133)

## Backport package

This is a backport providers package for `tableau` provider. All classes for this provider package
are in `airflow.providers.tableau` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.


## Release 2021.3.3

Initial version of the provider


## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-tableau`

## PIP requirements

| PIP package           | Version required   |
|:----------------------|:-------------------|
| `tableauserverclient` |                    |

# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `tableau` provider
are in the `airflow.providers.tableau` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)


## Operators


### New operators

| New Airflow 2.0 operators: `airflow.providers.tableau` package                                                                                                                     |
|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.tableau_refresh_workbook.TableauRefreshWorkbookOperator](https://github.com/apache/airflow/blob/master/airflow/providers/tableau/operators/tableau_refresh_workbook.py) |



## Sensors


### New sensors

| New Airflow 2.0 sensors: `airflow.providers.tableau` package                                                                                               |
|:-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| [sensors.tableau_job_status.TableauJobStatusSensor](https://github.com/apache/airflow/blob/master/airflow/providers/tableau/sensors/tableau_job_status.py) |



## Hooks


### New hooks

| New Airflow 2.0 hooks: `airflow.providers.tableau` package                                                            |
|:----------------------------------------------------------------------------------------------------------------------|
| [hooks.tableau.TableauHook](https://github.com/apache/airflow/blob/master/airflow/providers/tableau/hooks/tableau.py) |




## Releases

### Release 2021.3.3

| Commit                                                                                         | Committed   | Subject                                                           |
|:-----------------------------------------------------------------------------------------------|:------------|:------------------------------------------------------------------|
| [45e72ca83](https://github.com/apache/airflow/commit/45e72ca83049a7db526b1f0fbd94c75f5f92cc75) | 2021-02-25  | `Add Tableau provider separate from Salesforce Provider (#14030)` |

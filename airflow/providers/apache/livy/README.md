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


# Package apache-airflow-backport-providers-apache-livy

Release: 2020.05.19

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [Cross provider package dependencies](#cross-provider-package-dependencies)
- [Provider class summary](#provider-class-summary)
    - [Operators](#operators)
        - [New operators](#new-operators)
    - [Sensors](#sensors)
        - [New sensors](#new-sensors)
    - [Hooks](#hooks)
        - [New hooks](#new-hooks)
- [Releases](#releases)
    - [Release 2020.05.19](#release-20200519)

## Backport package

This is a backport providers package for `apache.livy` provider. All classes for this provider package
are in `airflow.providers.apache.livy` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-apache-livy`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## Cross provider package dependencies

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified backport providers package in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

```bash
pip install apache-airflow-backport-providers-apache-livy[http]
```

| Dependent package                                                                                              | Extra   |
|:---------------------------------------------------------------------------------------------------------------|:--------|
| [apache-airflow-backport-providers-http](https://github.com/apache/airflow/tree/master/airflow/providers/http) | http    |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.apache.livy` package.


## Operators


### New operators

| New Airflow 2.0 operators: `airflow.providers.apache.livy` package                                                           |
|:-----------------------------------------------------------------------------------------------------------------------------|
| [operators.livy.LivyOperator](https://github.com/apache/airflow/blob/master/airflow/providers/apache/livy/operators/livy.py) |






## Sensors


### New sensors

| New Airflow 2.0 sensors: `airflow.providers.apache.livy` package                                                       |
|:-----------------------------------------------------------------------------------------------------------------------|
| [sensors.livy.LivySensor](https://github.com/apache/airflow/blob/master/airflow/providers/apache/livy/sensors/livy.py) |




## Hooks


### New hooks

| New Airflow 2.0 hooks: `airflow.providers.apache.livy` package                                                   |
|:-----------------------------------------------------------------------------------------------------------------|
| [hooks.livy.LivyHook](https://github.com/apache/airflow/blob/master/airflow/providers/apache/livy/hooks/livy.py) |







## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                                                                                                            |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807)                                                                                            |
| [3320e432a](https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc) | 2020-02-24  | [AIRFLOW-6817] Lazy-load `airflow.DAG` to keep user-facing API untouched (#7517)                                                                                   |
| [4d03e33c1](https://github.com/apache/airflow/commit/4d03e33c115018e30fa413c42b16212481ad25cc) | 2020-02-22  | [AIRFLOW-6817] remove imports from `airflow/__init__.py`, replaced implicit imports with explicit imports, added entry to `UPDATING.MD` - squashed/rebased (#7456) |
| [d3cf23dc0](https://github.com/apache/airflow/commit/d3cf23dc07b5fb92ee2a5be07b0685a4fca36f86) | 2020-02-19  | [AIRFLOW-5470] Add Apache Livy REST operator (#6090)                                                                                                               |

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


# Package apache-airflow-backport-providers-odbc

Release: 2020.5.20

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-class-summary)
    - [Hooks](#hooks)
        - [New hooks](#new-hooks)
- [Releases](#releases)
    - [Release 2020.5.20](#release-2020520)

## Backport package

This is a backport providers package for `odbc` provider. All classes for this provider package
are in `airflow.providers.odbc` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-odbc`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| pyodbc        |                    |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.odbc` package.





## Hooks


### New hooks

| New Airflow 2.0 hooks: `airflow.providers.odbc` package                                                   |
|:----------------------------------------------------------------------------------------------------------|
| [hooks.odbc.OdbcHook](https://github.com/apache/airflow/blob/master/airflow/providers/odbc/hooks/odbc.py) |







## Releases

### Release 2020.5.20

| Commit                                                                                         | Committed   | Subject                                                                 |
|:-----------------------------------------------------------------------------------------------|:------------|:------------------------------------------------------------------------|
| [f3521fb0e](https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca) | 2020-05-16  | Regenerate readme files for backport package release (#8886)            |
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807) |
| [2b06d0a3d](https://github.com/apache/airflow/commit/2b06d0a3deb4a4fcc64ee1948bb484e457096474) | 2020-01-21  | [AIRFLOW-6603] Remove unnecessary pylint warnings (#7224)               |
| [2a819b11f](https://github.com/apache/airflow/commit/2a819b11fb8dfba7b3c9b500d07467b455724506) | 2020-01-19  | [AIRFLOW-6296] add OdbcHook &amp; deprecation warning for pymssql (#6850)   |

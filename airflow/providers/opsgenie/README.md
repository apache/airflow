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


# Package apache-airflow-backport-providers-opsgenie

Release: 2020.05.19

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [Cross provider package dependencies](#cross-provider-package-dependencies)
- [Provider class summary](#provider-class-summary)
    - [Operators](#operators)
        - [Moved operators](#moved-operators)
    - [Hooks](#hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 2020.05.19](#release-20200519)

## Backport package

This is a backport providers package for `opsgenie` provider. All classes for this provider package
are in `airflow.providers.opsgenie` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-opsgenie`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## Cross provider package dependencies

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified backport providers package in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

```bash
pip install apache-airflow-backport-providers-opsgenie[http]
```

| Dependent package                                                                                              | Extra   |
|:---------------------------------------------------------------------------------------------------------------|:--------|
| [apache-airflow-backport-providers-http](https://github.com/apache/airflow/tree/master/airflow/providers/http) | http    |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.opsgenie` package.


## Operators




### Moved operators

| Airflow 2.0 operators: `airflow.providers.opsgenie` package                                                                                            | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                                |
|:-------------------------------------------------------------------------------------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.opsgenie_alert.OpsgenieAlertOperator](https://github.com/apache/airflow/blob/master/airflow/providers/opsgenie/operators/opsgenie_alert.py) | [contrib.operators.opsgenie_alert_operator.OpsgenieAlertOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/opsgenie_alert_operator.py) |





## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.opsgenie` package                                                                                    | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                            |
|:-------------------------------------------------------------------------------------------------------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------|
| [hooks.opsgenie_alert.OpsgenieAlertHook](https://github.com/apache/airflow/blob/master/airflow/providers/opsgenie/hooks/opsgenie_alert.py) | [contrib.hooks.opsgenie_alert_hook.OpsgenieAlertHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/opsgenie_alert_hook.py) |






## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                                                                                                            |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [4bde99f13](https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a) | 2020-03-23  | Make airflow/providers pylint compatible (#7802)                                                                                                                   |
| [305053cb7](https://github.com/apache/airflow/commit/305053cb7c72220fbe32e4fab7cb1da49d5a14e2) | 2020-02-23  | [AIRFLOW-6889] Change mutable argument value in OpsgenieAlertHook (#7512)                                                                                          |
| [4d03e33c1](https://github.com/apache/airflow/commit/4d03e33c115018e30fa413c42b16212481ad25cc) | 2020-02-22  | [AIRFLOW-6817] remove imports from `airflow/__init__.py`, replaced implicit imports with explicit imports, added entry to `UPDATING.MD` - squashed/rebased (#7456) |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                                                                                                           |
| [9a04013b0](https://github.com/apache/airflow/commit/9a04013b0e40b0d744ff4ac9f008491806d60df2) | 2020-01-27  | [AIRFLOW-6646][AIP-21] Move protocols classes to providers package (#7268)                                                                                         |
| [c42a375e7](https://github.com/apache/airflow/commit/c42a375e799e5adb3f9536616372dc90ff47e6c8) | 2020-01-27  | [AIRFLOW-6644][AIP-21] Move service classes to providers package (#7265)                                                                                           |

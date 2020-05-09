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


# Package apache-airflow-backport-providers-ssh

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
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 2020.05.19](#release-20200519)

## Backport package

This is a backport providers package for `ssh` provider. All classes for this provider package
are in `airflow.providers.ssh` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-ssh`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| paramiko      | &gt;=2.6.0            |
| pysftp        | &gt;=0.2.9            |
| sshtunnel     | &gt;=0.1.4,&lt;0.2       |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.ssh` package.


## Operators




### Moved operators

| Airflow 2.0 operators: `airflow.providers.ssh` package                                                            | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                |
|:------------------------------------------------------------------------------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.ssh.SSHOperator](https://github.com/apache/airflow/blob/master/airflow/providers/ssh/operators/ssh.py) | [contrib.operators.ssh_operator.SSHOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/ssh_operator.py) |





## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.ssh` package                                                    | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                            |
|:------------------------------------------------------------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------|
| [hooks.ssh.SSHHook](https://github.com/apache/airflow/blob/master/airflow/providers/ssh/hooks/ssh.py) | [contrib.hooks.ssh_hook.SSHHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/ssh_hook.py) |






## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                    |
|:-----------------------------------------------------------------------------------------------|:------------|:---------------------------------------------------------------------------|
| [21cc7d729](https://github.com/apache/airflow/commit/21cc7d729827e9f3af0698bf647b2d41fc87b11c) | 2020-05-10  | Document default timeout value for SSHOperator (#8744)                     |
| [4bde99f13](https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a) | 2020-03-23  | Make airflow/providers pylint compatible (#7802)                           |
| [74c2a6ded](https://github.com/apache/airflow/commit/74c2a6ded4d615de8e1b1c04a25146344138e920) | 2020-03-23  | Add call to Super class in &#39;ftp&#39; &amp; &#39;ssh&#39; providers (#7822)                 |
| [df24b4337](https://github.com/apache/airflow/commit/df24b43370ca5812273ecd91d35104e023a407e6) | 2020-02-14  | [AIRFLOW-6800] Close file object after parsing ssh config (#7415)          |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                   |
| [9a04013b0](https://github.com/apache/airflow/commit/9a04013b0e40b0d744ff4ac9f008491806d60df2) | 2020-01-27  | [AIRFLOW-6646][AIP-21] Move protocols classes to providers package (#7268) |

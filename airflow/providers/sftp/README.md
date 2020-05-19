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


# Package apache-airflow-backport-providers-sftp

Release: 2020.5.20

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Cross provider package dependencies](#cross-provider-package-dependencies)
- [Provider class summary](#provider-class-summary)
    - [Operators](#operators)
        - [Moved operators](#moved-operators)
    - [Sensors](#sensors)
        - [Moved sensors](#moved-sensors)
    - [Hooks](#hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 2020.5.20](#release-2020520)

## Backport package

This is a backport providers package for `sftp` provider. All classes for this provider package
are in `airflow.providers.sftp` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-sftp`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| paramiko      | &gt;=2.6.0            |
| pysftp        | &gt;=0.2.9            |
| sshtunnel     | &gt;=0.1.4,&lt;0.2       |

## Cross provider package dependencies

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified backport providers package in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

```bash
pip install apache-airflow-backport-providers-sftp[ssh]
```

| Dependent package                                                                                            | Extra   |
|:-------------------------------------------------------------------------------------------------------------|:--------|
| [apache-airflow-backport-providers-ssh](https://github.com/apache/airflow/tree/master/airflow/providers/ssh) | ssh     |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.sftp` package.


## Operators




### Moved operators

| Airflow 2.0 operators: `airflow.providers.sftp` package                                                               | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                   |
|:----------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.sftp.SFTPOperator](https://github.com/apache/airflow/blob/master/airflow/providers/sftp/operators/sftp.py) | [contrib.operators.sftp_operator.SFTPOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/sftp_operator.py) |




## Sensors



### Moved sensors

| Airflow 2.0 sensors: `airflow.providers.sftp` package                                                           | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                         |
|:----------------------------------------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------|
| [sensors.sftp.SFTPSensor](https://github.com/apache/airflow/blob/master/airflow/providers/sftp/sensors/sftp.py) | [contrib.sensors.sftp_sensor.SFTPSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/sftp_sensor.py) |



## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.sftp` package                                                       | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                               |
|:----------------------------------------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------|
| [hooks.sftp.SFTPHook](https://github.com/apache/airflow/blob/master/airflow/providers/sftp/hooks/sftp.py) | [contrib.hooks.sftp_hook.SFTPHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/sftp_hook.py) |






## Releases

### Release 2020.5.20

| Commit                                                                                         | Committed   | Subject                                                                    |
|:-----------------------------------------------------------------------------------------------|:------------|:---------------------------------------------------------------------------|
| [f3521fb0e](https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca) | 2020-05-16  | Regenerate readme files for backport package release (#8886)               |
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807)    |
| [bac0ab27c](https://github.com/apache/airflow/commit/bac0ab27cfc89e715efddc97214fcd7738084361) | 2020-03-30  | close sftp connection without error (#7953)                                |
| [42eef3821](https://github.com/apache/airflow/commit/42eef38217e709bc7a7f71bf0286e9e61293a43e) | 2020-03-07  | [AIRFLOW-6877] Add cross-provider dependencies as extras (#7506)           |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                   |
| [ceea293c1](https://github.com/apache/airflow/commit/ceea293c1652240e7e856c201e4341a87ef97a0f) | 2020-01-28  | [AIRFLOW-6656] Fix AIP-21 moving (#7272)                                   |
| [9a04013b0](https://github.com/apache/airflow/commit/9a04013b0e40b0d744ff4ac9f008491806d60df2) | 2020-01-27  | [AIRFLOW-6646][AIP-21] Move protocols classes to providers package (#7268) |
| [69629a5a9](https://github.com/apache/airflow/commit/69629a5a948ab2c4ac04a4a4dca6ac86d19c11bd) | 2019-12-09  | [AIRFLOW-5807] Move SFTP from contrib to providers. (#6464)                |

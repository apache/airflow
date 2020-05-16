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


# Package apache-airflow-backport-providers-ftp

Release: 2020.05.19

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [Provider class summary](#provider-class-summary)
    - [Sensors](#sensors)
        - [Moved sensors](#moved-sensors)
    - [Hooks](#hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 2020.05.19](#release-20200519)

## Backport package

This is a backport providers package for `ftp` provider. All classes for this provider package
are in `airflow.providers.ftp` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-ftp`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.ftp` package.




## Sensors



### Moved sensors

| Airflow 2.0 sensors: `airflow.providers.ftp` package                                                         | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                       |
|:-------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------|
| [sensors.ftp.FTPSSensor](https://github.com/apache/airflow/blob/master/airflow/providers/ftp/sensors/ftp.py) | [contrib.sensors.ftp_sensor.FTPSSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/ftp_sensor.py) |
| [sensors.ftp.FTPSensor](https://github.com/apache/airflow/blob/master/airflow/providers/ftp/sensors/ftp.py)  | [contrib.sensors.ftp_sensor.FTPSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/ftp_sensor.py)  |



## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.ftp` package                                                     | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                             |
|:-------------------------------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------|
| [hooks.ftp.FTPHook](https://github.com/apache/airflow/blob/master/airflow/providers/ftp/hooks/ftp.py)  | [contrib.hooks.ftp_hook.FTPHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/ftp_hook.py)  |
| [hooks.ftp.FTPSHook](https://github.com/apache/airflow/blob/master/airflow/providers/ftp/hooks/ftp.py) | [contrib.hooks.ftp_hook.FTPSHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/ftp_hook.py) |






## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                    |
|:-----------------------------------------------------------------------------------------------|:------------|:---------------------------------------------------------------------------|
| [87969a350](https://github.com/apache/airflow/commit/87969a350ddd41e9e77776af6d780b31e363eaca) | 2020-04-09  | [AIRFLOW-6515] Change Log Levels from Info/Warn to Error (#8170)           |
| [4bde99f13](https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a) | 2020-03-23  | Make airflow/providers pylint compatible (#7802)                           |
| [74c2a6ded](https://github.com/apache/airflow/commit/74c2a6ded4d615de8e1b1c04a25146344138e920) | 2020-03-23  | Add call to Super class in &#39;ftp&#39; &amp; &#39;ssh&#39; providers (#7822)                 |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                   |
| [9a04013b0](https://github.com/apache/airflow/commit/9a04013b0e40b0d744ff4ac9f008491806d60df2) | 2020-01-27  | [AIRFLOW-6646][AIP-21] Move protocols classes to providers package (#7268) |

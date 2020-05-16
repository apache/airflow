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


# Package apache-airflow-backport-providers-apache-hdfs

Release: 2020.05.19

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-class-summary)
    - [Sensors](#sensors)
        - [Moved sensors](#moved-sensors)
    - [Hooks](#hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 2020.05.19](#release-20200519)

## Backport package

This is a backport providers package for `apache.hdfs` provider. All classes for this provider package
are in `airflow.providers.apache.hdfs` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-apache-hdfs`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| snakebite     | &gt;=2.7.8            |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.apache.hdfs` package.




## Sensors



### Moved sensors

| Airflow 2.0 sensors: `airflow.providers.apache.hdfs` package                                                                      | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                               |
|:----------------------------------------------------------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------|
| [sensors.hdfs.HdfsSensor](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hdfs/sensors/hdfs.py)            | [sensors.hdfs_sensor.HdfsSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/sensors/hdfs_sensor.py)                       |
| [sensors.hdfs.HdfsSensorFolder](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hdfs/sensors/hdfs.py)      | [contrib.sensors.hdfs_sensor.HdfsSensorFolder](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/hdfs_sensor.py) |
| [sensors.hdfs.HdfsSensorRegex](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hdfs/sensors/hdfs.py)       | [contrib.sensors.hdfs_sensor.HdfsSensorRegex](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/hdfs_sensor.py)  |
| [sensors.web_hdfs.WebHdfsSensor](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hdfs/sensors/web_hdfs.py) | [sensors.web_hdfs_sensor.WebHdfsSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/sensors/web_hdfs_sensor.py)            |



## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.apache.hdfs` package                                                                | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                        |
|:--------------------------------------------------------------------------------------------------------------------------|:--------------------------------------------------------------------------------------------------------------------|
| [hooks.hdfs.HDFSHook](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hdfs/hooks/hdfs.py)          | [hooks.hdfs_hook.HDFSHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/hooks/hdfs_hook.py)          |
| [hooks.webhdfs.WebHDFSHook](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hdfs/hooks/webhdfs.py) | [hooks.webhdfs_hook.WebHDFSHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/hooks/webhdfs_hook.py) |






## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                        |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------------------|
| [87969a350](https://github.com/apache/airflow/commit/87969a350ddd41e9e77776af6d780b31e363eaca) | 2020-04-09  | [AIRFLOW-6515] Change Log Levels from Info/Warn to Error (#8170)               |
| [6c670870a](https://github.com/apache/airflow/commit/6c670870aa6ea5d82a86f912bb6de8b88e711ca5) | 2020-03-25  | [AIRFLOW-6833] HA for webhdfs connection (#7454)                               |
| [7e6372a68](https://github.com/apache/airflow/commit/7e6372a681a2a543f4710b083219aeb53b074388) | 2020-03-23  | Add call to Super call in apache providers (#7820)                             |
| [f3ad5cf61](https://github.com/apache/airflow/commit/f3ad5cf6185b9d406d0fb0a4ecc0b5536f79217a) | 2020-02-03  | [AIRFLOW-4681] Make sensors module pylint compatible (#7309)                   |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                       |
| [cf141506a](https://github.com/apache/airflow/commit/cf141506a25dbba279b85500d781f7e056540721) | 2020-02-02  | [AIRFLOW-6708] Set unique logger names (#7330)                                 |
| [0481b9a95](https://github.com/apache/airflow/commit/0481b9a95786a62de4776a735ae80e746583ef2b) | 2020-01-12  | [AIRFLOW-6539][AIP-21] Move Apache classes to providers.apache package (#7142) |

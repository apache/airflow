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


# Package apache-airflow-backport-providers-celery

Release: 2020.05.19

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-class-summary)
    - [Sensors](#sensors)
        - [Moved sensors](#moved-sensors)
- [Releases](#releases)
    - [Release 2020.05.19](#release-20200519)

## Backport package

This is a backport providers package for `celery` provider. All classes for this provider package
are in `airflow.providers.celery` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-celery`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| celery        | ~=4.4.2            |
| flower        | &gt;=0.7.3, &lt;1.0      |
| tornado       | &gt;=4.2.0, &lt;6.0      |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.celery` package.




## Sensors



### Moved sensors

| Airflow 2.0 sensors: `airflow.providers.celery` package                                                                                  | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                |
|:-----------------------------------------------------------------------------------------------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [sensors.celery_queue.CeleryQueueSensor](https://github.com/apache/airflow/blob/master/airflow/providers/celery/sensors/celery_queue.py) | [contrib.sensors.celery_queue_sensor.CeleryQueueSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/celery_queue_sensor.py) |







## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                 |
|:-----------------------------------------------------------------------------------------------|:------------|:------------------------------------------------------------------------|
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807) |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                |
| [059eda05f](https://github.com/apache/airflow/commit/059eda05f82fefce4410f44f761f945a27d83daf) | 2020-01-21  | [AIRFLOW-6610] Move software classes to providers package (#7231)       |

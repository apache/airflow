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


# Package apache-airflow-backport-providers-redis

Release: 2020.05.19

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-class-summary)
    - [Operators](#operators)
        - [Moved operators](#moved-operators)
    - [Sensors](#sensors)
        - [Moved sensors](#moved-sensors)
    - [Hooks](#hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 2020.05.19](#release-20200519)

## Backport package

This is a backport providers package for `redis` provider. All classes for this provider package
are in `airflow.providers.redis` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-redis`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| redis         | ~=3.2              |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.redis` package.


## Operators




### Moved operators

| Airflow 2.0 operators: `airflow.providers.redis` package                                                                                         | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                             |
|:-------------------------------------------------------------------------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.redis_publish.RedisPublishOperator](https://github.com/apache/airflow/blob/master/airflow/providers/redis/operators/redis_publish.py) | [contrib.operators.redis_publish_operator.RedisPublishOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/redis_publish_operator.py) |




## Sensors



### Moved sensors

| Airflow 2.0 sensors: `airflow.providers.redis` package                                                                                    | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                  |
|:------------------------------------------------------------------------------------------------------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [sensors.redis_key.RedisKeySensor](https://github.com/apache/airflow/blob/master/airflow/providers/redis/sensors/redis_key.py)            | [contrib.sensors.redis_key_sensor.RedisKeySensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/redis_key_sensor.py)            |
| [sensors.redis_pub_sub.RedisPubSubSensor](https://github.com/apache/airflow/blob/master/airflow/providers/redis/sensors/redis_pub_sub.py) | [contrib.sensors.redis_pub_sub_sensor.RedisPubSubSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/redis_pub_sub_sensor.py) |



## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.redis` package                                                          | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                  |
|:--------------------------------------------------------------------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------|
| [hooks.redis.RedisHook](https://github.com/apache/airflow/blob/master/airflow/providers/redis/hooks/redis.py) | [contrib.hooks.redis_hook.RedisHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/redis_hook.py) |






## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                |
|:-----------------------------------------------------------------------------------------------|:------------|:-----------------------------------------------------------------------|
| [05443c6dc](https://github.com/apache/airflow/commit/05443c6dc8100e791446bbcc0df04de6e34017bb) | 2020-03-23  | Add missing call to Super class in remaining providers (#7828)         |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)               |
| [a3f02c462](https://github.com/apache/airflow/commit/a3f02c4627c28ad524cca73031670722cd6d8253) | 2020-01-24  | [AIRFLOW-6493] Add SSL configuration to Redis hook connections (#7234) |
| [059eda05f](https://github.com/apache/airflow/commit/059eda05f82fefce4410f44f761f945a27d83daf) | 2020-01-21  | [AIRFLOW-6610] Move software classes to providers package (#7231)      |

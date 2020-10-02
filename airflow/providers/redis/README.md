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

Release: 2020.10.5

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-classes-summary)
    - [Operators](#operators)
        - [Moved operators](#moved-operators)
    - [Sensors](#sensors)
        - [Moved sensors](#moved-sensors)
    - [Hooks](#hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 2020.10.5](#release-2020105)
    - [Release 2020.6.24](#release-2020624)

## Backport package

This is a backport providers package for `redis` provider. All classes for this provider package
are in `airflow.providers.redis` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.



## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-redis`

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| redis         | ~=3.2              |

# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `redis` provider
are in the `airflow.providers.redis` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)


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

### Release 2020.10.5

| Commit                                                                                         | Committed   | Subject                                                              |
|:-----------------------------------------------------------------------------------------------|:------------|:---------------------------------------------------------------------|
| [5220e4c38](https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5) | 2020-10-02  | Prepare Backport release 2020.09.07 (#11238)                         |
| [f3e87c503](https://github.com/apache/airflow/commit/f3e87c503081a3085dff6c7352640d7f08beb5bc) | 2020-09-22  | Add D202 pydocstyle check (#11032)                                   |
| [fdd9b6f65](https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3) | 2020-08-25  | Enable Black on Providers Packages (#10543)                          |
| [3696c34c2](https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34) | 2020-08-24  | Fix typo in the word &#34;release&#34; (#10528)                              |
| [2f2d8dbfa](https://github.com/apache/airflow/commit/2f2d8dbfafefb4be3dd80f22f31c649c8498f148) | 2020-08-25  | Remove all &#34;noinspection&#34; comments native to IntelliJ (#10525)       |
| [ee7ca128a](https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94) | 2020-08-22  | Fix broken Markdown refernces in Providers README (#10483)           |
| [cdec30125](https://github.com/apache/airflow/commit/cdec3012542b45d23a05f62d69110944ba542e2a) | 2020-08-07  | Add correct signature to all operators and sensors (#10205)          |
| [24c8e4c2d](https://github.com/apache/airflow/commit/24c8e4c2d6e359ecc2c7d6275dccc68de4a82832) | 2020-08-06  | Changes to all the constructors to remove the args argument (#10163) |
| [aeea71274](https://github.com/apache/airflow/commit/aeea71274d4527ff2351102e94aa38bda6099e7f) | 2020-08-02  | Remove `args` parameter from provider operator constructors (#10097) |
| [0a2acf0b6](https://github.com/apache/airflow/commit/0a2acf0b6542b717f87dee6bbff43397bbb0e83b) | 2020-07-14  | Add type annotations for redis provider (#9815)                      |
| [e13a14c87](https://github.com/apache/airflow/commit/e13a14c8730f4f633d996dd7d3468fe827136a84) | 2020-06-21  | Enable &amp; Fix Whitespace related PyDocStyle Checks (#9458)            |
| [d0e7db402](https://github.com/apache/airflow/commit/d0e7db4024806af35e3c9a2cae460fdeedd4d2ec) | 2020-06-19  | Fixed release number for fresh release (#9408)                       |


### Release 2020.6.24

| Commit                                                                                         | Committed   | Subject                                                                 |
|:-----------------------------------------------------------------------------------------------|:------------|:------------------------------------------------------------------------|
| [12af6a080](https://github.com/apache/airflow/commit/12af6a08009b8776e00d8a0aab92363eb8c4e8b1) | 2020-06-19  | Final cleanup for 2020.6.23rc1 release preparation (#9404)              |
| [c7e5bce57](https://github.com/apache/airflow/commit/c7e5bce57fe7f51cefce4f8a41ce408ac5675d13) | 2020-06-19  | Prepare backport release candidate for 2020.6.23rc1 (#9370)             |
| [f6bd817a3](https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac) | 2020-06-16  | Introduce &#39;transfers&#39; packages (#9320)                                  |
| [0b0e4f7a4](https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34) | 2020-05-26  | Preparing for RC3 release of backports (#9026)                           |
| [00642a46d](https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c) | 2020-05-26  | Fixed name of 20 remaining wrongly named operators. (#8994)             |
| [375d1ca22](https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f) | 2020-05-19  | Release candidate 2 for backport packages 2020.05.20 (#8898)            |
| [12c5e5d8a](https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79) | 2020-05-17  | Prepare release candidate for backport packages (#8891)                 |
| [f3521fb0e](https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca) | 2020-05-16  | Regenerate readme files for backport package release (#8886)            |
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807) |
| [05443c6dc](https://github.com/apache/airflow/commit/05443c6dc8100e791446bbcc0df04de6e34017bb) | 2020-03-23  | Add missing call to Super class in remaining providers (#7828)          |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                |
| [a3f02c462](https://github.com/apache/airflow/commit/a3f02c4627c28ad524cca73031670722cd6d8253) | 2020-01-24  | [AIRFLOW-6493] Add SSL configuration to Redis hook connections (#7234)  |
| [059eda05f](https://github.com/apache/airflow/commit/059eda05f82fefce4410f44f761f945a27d83daf) | 2020-01-21  | [AIRFLOW-6610] Move software classes to providers package (#7231)       |

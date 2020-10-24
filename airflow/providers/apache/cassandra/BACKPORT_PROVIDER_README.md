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


# Package apache-airflow-backport-providers-apache-cassandra

Release: 2020.10.29

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-classes-summary)
    - [Sensors](#sensors)
        - [Moved sensors](#moved-sensors)
    - [Hooks](#hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 2020.10.29](#release-20201029)
    - [Release 2020.10.5](#release-2020105)
    - [Release 2020.6.24](#release-2020624)

## Backport package

This is a backport providers package for `apache.cassandra` provider. All classes for this provider package
are in `airflow.providers.apache.cassandra` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.



## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-apache-cassandra`

## PIP requirements

| PIP package      | Version required   |
|:-----------------|:-------------------|
| cassandra-driver | &gt;=3.13.0,&lt;3.21.0   |

# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `apache.cassandra` provider
are in the `airflow.providers.apache.cassandra` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)


## Sensors



### Moved sensors

| Airflow 2.0 sensors: `airflow.providers.apache.cassandra` package                                                                          | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                            |
|:-------------------------------------------------------------------------------------------------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [sensors.record.CassandraRecordSensor](https://github.com/apache/airflow/blob/master/airflow/providers/apache/cassandra/sensors/record.py) | [contrib.sensors.cassandra_record_sensor.CassandraRecordSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/cassandra_record_sensor.py) |
| [sensors.table.CassandraTableSensor](https://github.com/apache/airflow/blob/master/airflow/providers/apache/cassandra/sensors/table.py)    | [contrib.sensors.cassandra_table_sensor.CassandraTableSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/cassandra_table_sensor.py)    |


## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.apache.cassandra` package                                                                      | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                              |
|:-------------------------------------------------------------------------------------------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------|
| [hooks.cassandra.CassandraHook](https://github.com/apache/airflow/blob/master/airflow/providers/apache/cassandra/hooks/cassandra.py) | [contrib.hooks.cassandra_hook.CassandraHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/cassandra_hook.py) |



## Releases

### Release 2020.10.29

| Commit                                                                                         | Committed   | Subject                                                        |
|:-----------------------------------------------------------------------------------------------|:------------|:---------------------------------------------------------------|
| [b680bbc0b](https://github.com/apache/airflow/commit/b680bbc0b05ad71d403a5d58bc7023a2453b9a48) | 2020-10-24  | Generated backport providers readmes/setup for 2020.10.29      |
| [349b0811c](https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a) | 2020-10-20  | Add D200 pydocstyle check (#11688)                             |
| [0646849e3](https://github.com/apache/airflow/commit/0646849e3dacdc2bc62705ae136f3ad3b16232e9) | 2020-10-14  | Add protocol_version to conn_config for Cassandrahook (#11036) |
| [16e712971](https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746) | 2020-10-13  | Added support for provider packages for Airflow 2.0 (#11487)   |
| [0a0e1af80](https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa) | 2020-10-03  | Fix Broken Markdown links in Providers README TOC (#11249)     |


### Release 2020.10.5

| Commit                                                                                         | Committed   | Subject                                                           |
|:-----------------------------------------------------------------------------------------------|:------------|:------------------------------------------------------------------|
| [ca4238eb4](https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13) | 2020-10-02  | Fixed month in backport packages to October (#11242)              |
| [5220e4c38](https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5) | 2020-10-02  | Prepare Backport release 2020.09.07 (#11238)                      |
| [fdd9b6f65](https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3) | 2020-08-25  | Enable Black on Providers Packages (#10543)                       |
| [3696c34c2](https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34) | 2020-08-24  | Fix typo in the word &#34;release&#34; (#10528)                           |
| [ee7ca128a](https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94) | 2020-08-22  | Fix broken Markdown refernces in Providers README (#10483)        |
| [3b3287d7a](https://github.com/apache/airflow/commit/3b3287d7acc76430f12b758d52cec61c7f74e726) | 2020-08-05  | Enforce keyword only arguments on apache operators (#10170)       |
| [7d24b088c](https://github.com/apache/airflow/commit/7d24b088cd736cfa18f9214e4c9d6ce2d5865f3d) | 2020-07-25  | Stop using start_date in default_args in example_dags (2) (#9985) |
| [33f0cd265](https://github.com/apache/airflow/commit/33f0cd2657b2e77ea3477e0c93f13f1474be628e) | 2020-07-22  | apply_default keeps the function signature for mypy (#9784)       |
| [4d74ac211](https://github.com/apache/airflow/commit/4d74ac2111862186598daf92cbf2c525617061c2) | 2020-07-19  | Increase typing for Apache and http provider package (#9729)      |
| [750555f26](https://github.com/apache/airflow/commit/750555f261616d809d24b8550b9482a713ba3171) | 2020-07-19  | Add guide for Cassandra Operators (#9877)                         |
| [d0e7db402](https://github.com/apache/airflow/commit/d0e7db4024806af35e3c9a2cae460fdeedd4d2ec) | 2020-06-19  | Fixed release number for fresh release (#9408)                    |


### Release 2020.6.24

| Commit                                                                                         | Committed   | Subject                                                                          |
|:-----------------------------------------------------------------------------------------------|:------------|:---------------------------------------------------------------------------------|
| [12af6a080](https://github.com/apache/airflow/commit/12af6a08009b8776e00d8a0aab92363eb8c4e8b1) | 2020-06-19  | Final cleanup for 2020.6.23rc1 release preparation (#9404)                       |
| [c7e5bce57](https://github.com/apache/airflow/commit/c7e5bce57fe7f51cefce4f8a41ce408ac5675d13) | 2020-06-19  | Prepare backport release candidate for 2020.6.23rc1 (#9370)                      |
| [f6bd817a3](https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac) | 2020-06-16  | Introduce &#39;transfers&#39; packages (#9320)                                           |
| [0b0e4f7a4](https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34) | 2020-05-26  | Preparing for RC3 release of backports (#9026)                                    |
| [00642a46d](https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c) | 2020-05-26  | Fixed name of 20 remaining wrongly named operators. (#8994)                      |
| [375d1ca22](https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f) | 2020-05-19  | Release candidate 2 for backport packages 2020.05.20 (#8898)                     |
| [12c5e5d8a](https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79) | 2020-05-17  | Prepare release candidate for backport packages (#8891)                          |
| [f3521fb0e](https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca) | 2020-05-16  | Regenerate readme files for backport package release (#8886)                     |
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807)          |
| [7e6372a68](https://github.com/apache/airflow/commit/7e6372a681a2a543f4710b083219aeb53b074388) | 2020-03-23  | Add call to Super call in apache providers (#7820)                               |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                         |
| [2f2f89c14](https://github.com/apache/airflow/commit/2f2f89c148e2b694aee9402707f68065ee7320f8) | 2019-12-01  | [AIRFLOW-6139] Consistent spaces in pylint enable/disable (#6701)                |
| [f88f06c86](https://github.com/apache/airflow/commit/f88f06c862b6096e974871decd14b86811cc4bc6) | 2019-11-30  | [AIRFLOW-6131] Make Cassandra hooks/sensors pylint compatible (#6693)            |
| [f987646d7](https://github.com/apache/airflow/commit/f987646d7d85683cdc73ae9438a2a8c4a2992c7f) | 2019-11-22  | [AIRFLOW-5950] AIP-21 Change import paths for &#34;apache/cassandra&#34; modules (#6609) |

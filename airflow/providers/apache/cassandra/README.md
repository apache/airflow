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

Release: 2020.5.20

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
    - [Release 2020.5.20](#release-2020520)

## Backport package

This is a backport providers package for `apache.cassandra` provider. All classes for this provider package
are in `airflow.providers.apache.cassandra` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-apache-cassandra`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package      | Version required   |
|:-----------------|:-------------------|
| cassandra-driver | &gt;=3.13.0,&lt;3.21.0   |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.apache.cassandra` package.




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

### Release 2020.5.20

| Commit                                                                                         | Committed   | Subject                                                                          |
|:-----------------------------------------------------------------------------------------------|:------------|:---------------------------------------------------------------------------------|
| [f3521fb0e](https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca) | 2020-05-16  | Regenerate readme files for backport package release (#8886)                     |
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807)          |
| [7e6372a68](https://github.com/apache/airflow/commit/7e6372a681a2a543f4710b083219aeb53b074388) | 2020-03-23  | Add call to Super call in apache providers (#7820)                               |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                         |
| [2f2f89c14](https://github.com/apache/airflow/commit/2f2f89c148e2b694aee9402707f68065ee7320f8) | 2019-12-01  | [AIRFLOW-6139] Consistent spaces in pylint enable/disable (#6701)                |
| [f88f06c86](https://github.com/apache/airflow/commit/f88f06c862b6096e974871decd14b86811cc4bc6) | 2019-11-30  | [AIRFLOW-6131] Make Cassandra hooks/sensors pylint compatible (#6693)            |
| [f987646d7](https://github.com/apache/airflow/commit/f987646d7d85683cdc73ae9438a2a8c4a2992c7f) | 2019-11-22  | [AIRFLOW-5950] AIP-21 Change import paths for &#34;apache/cassandra&#34; modules (#6609) |

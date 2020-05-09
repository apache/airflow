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


# Package apache-airflow-backport-providers-apache-spark

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

This is a backport providers package for `apache.spark` provider. All classes for this provider package
are in `airflow.providers.apache.spark` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-apache-spark`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| pyspark       |                    |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.apache.spark` package.


## Operators




### Moved operators

| Airflow 2.0 operators: `airflow.providers.apache.spark` package                                                                                      | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                          |
|:-----------------------------------------------------------------------------------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.spark_jdbc.SparkJDBCOperator](https://github.com/apache/airflow/blob/master/airflow/providers/apache/spark/operators/spark_jdbc.py)       | [contrib.operators.spark_jdbc_operator.SparkJDBCOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/spark_jdbc_operator.py)       |
| [operators.spark_sql.SparkSqlOperator](https://github.com/apache/airflow/blob/master/airflow/providers/apache/spark/operators/spark_sql.py)          | [contrib.operators.spark_sql_operator.SparkSqlOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/spark_sql_operator.py)          |
| [operators.spark_submit.SparkSubmitOperator](https://github.com/apache/airflow/blob/master/airflow/providers/apache/spark/operators/spark_submit.py) | [contrib.operators.spark_submit_operator.SparkSubmitOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/spark_submit_operator.py) |





## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.apache.spark` package                                                                              | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                      |
|:-----------------------------------------------------------------------------------------------------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------|
| [hooks.spark_jdbc.SparkJDBCHook](https://github.com/apache/airflow/blob/master/airflow/providers/apache/spark/hooks/spark_jdbc.py)       | [contrib.hooks.spark_jdbc_hook.SparkJDBCHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/spark_jdbc_hook.py)       |
| [hooks.spark_sql.SparkSqlHook](https://github.com/apache/airflow/blob/master/airflow/providers/apache/spark/hooks/spark_sql.py)          | [contrib.hooks.spark_sql_hook.SparkSqlHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/spark_sql_hook.py)          |
| [hooks.spark_submit.SparkSubmitHook](https://github.com/apache/airflow/blob/master/airflow/providers/apache/spark/hooks/spark_submit.py) | [contrib.hooks.spark_submit_hook.SparkSubmitHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/spark_submit_hook.py) |






## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                                          |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------------------------------------|
| [7506c73f1](https://github.com/apache/airflow/commit/7506c73f1721151e9c50ef8bdb70d2136a16190b) | 2020-05-10  | Add default `conf` parameter to Spark JDBC Hook (#8787)                                          |
| [487b5cc50](https://github.com/apache/airflow/commit/487b5cc50c5b28a045cb12a1527a5453b0a6a7af) | 2020-05-06  | Add guide for Apache Spark operators (#8305)                                                     |
| [87969a350](https://github.com/apache/airflow/commit/87969a350ddd41e9e77776af6d780b31e363eaca) | 2020-04-09  | [AIRFLOW-6515] Change Log Levels from Info/Warn to Error (#8170)                                 |
| [be1451b0e](https://github.com/apache/airflow/commit/be1451b0e1b7e33f4621e24649f6a4fa87c34e01) | 2020-04-02  | [AIRFLOW-7026] Improve SparkSqlHook&#39;s error message (#7749)                                      |
| [4bde99f13](https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a) | 2020-03-23  | Make airflow/providers pylint compatible (#7802)                                                 |
| [7e6372a68](https://github.com/apache/airflow/commit/7e6372a681a2a543f4710b083219aeb53b074388) | 2020-03-23  | Add call to Super call in apache providers (#7820)                                               |
| [2327aa5a2](https://github.com/apache/airflow/commit/2327aa5a263f25beeaf4ba79670f10f001daf0bf) | 2020-03-12  | [AIRFLOW-7025] Fix SparkSqlHook.run_query to handle its parameter properly (#7677)               |
| [024b4bf96](https://github.com/apache/airflow/commit/024b4bf962bc30ecb70da9650e68b523a0dbcff8) | 2020-03-10  | [AIRFLOW-7024] Add the verbose parameter support to SparkSqlOperator (#7676)                     |
| [b59042b5a](https://github.com/apache/airflow/commit/b59042b5ab083c77ba08ba804df76b7c728815dc) | 2020-02-28  | [AIRFLOW-6949] Respect explicit `spark.kubernetes.namespace` conf to SparkSubmitOperator (#7575) |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                                         |
| [0481b9a95](https://github.com/apache/airflow/commit/0481b9a95786a62de4776a735ae80e746583ef2b) | 2020-01-12  | [AIRFLOW-6539][AIP-21] Move Apache classes to providers.apache package (#7142)                   |

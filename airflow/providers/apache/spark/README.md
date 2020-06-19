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

Release: 2020-06-23

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
    - [Release 2020-06-23](#release-2020-06-23)

## Backport package

This is a backport providers package for `apache.spark` provider. All classes for this provider package
are in `airflow.providers.apache.spark` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.



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

# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `apache.spark` provider
are in the `airflow.providers.apache.spark` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)


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

### Release 2020-06-23

| Commit                                                                                         | Committed   | Subject                                                                                          |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------------------------------------|
| [c7e5bce57](https://github.com/apache/airflow/commit/c7e5bce57fe7f51cefce4f8a41ce408ac5675d13) | 2020-06-19  | Prepare backport release candidate for 2020.6.23rc1 (#9370)                                      |
| [40bf8f28f](https://github.com/apache/airflow/commit/40bf8f28f97f17f40d993d207ea740eba54593ee) | 2020-06-18  | Detect automatically the lack of reference to the guide in the operator descriptions (#9290)     |
| [f6bd817a3](https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac) | 2020-06-16  | Introduce &#39;transfers&#39; packages (#9320)                                                           |
| [0b0e4f7a4](https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34) | 2020-05-26  | Preparing for RC3 relase of backports (#9026)                                                    |
| [00642a46d](https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c) | 2020-05-26  | Fixed name of 20 remaining wrongly named operators. (#8994)                                      |
| [375d1ca22](https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f) | 2020-05-19  | Release candidate 2 for backport packages 2020.05.20 (#8898)                                     |
| [12c5e5d8a](https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79) | 2020-05-17  | Prepare release candidate for backport packages (#8891)                                          |
| [f3521fb0e](https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca) | 2020-05-16  | Regenerate readme files for backport package release (#8886)                                     |
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807)                          |
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

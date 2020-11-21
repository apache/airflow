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


# Package apache-airflow-backport-providers-apache-hive

Release: 2020.10.29

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [PIP requirements](#pip-requirements)
- [Cross provider package dependencies](#cross-provider-package-dependencies)
- [Provider class summary](#provider-classes-summary)
    - [Operators](#operators)
        - [Moved operators](#moved-operators)
    - [Transfer operators](#transfer-operators)
        - [Moved transfer operators](#moved-transfer-operators)
    - [Sensors](#sensors)
        - [Moved sensors](#moved-sensors)
    - [Hooks](#hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 2020.10.29](#release-20201029)
    - [Release 2020.10.5](#release-2020105)
    - [Release 2020.6.24](#release-2020624)

## Backport package

This is a backport providers package for `apache.hive` provider. All classes for this provider package
are in `airflow.providers.apache.hive` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.



## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-apache-hive`

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| hmsclient     | &gt;=0.1.0            |
| pyhive[hive]  | &gt;=0.6.0            |

## Cross provider package dependencies

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified backport providers package in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

```bash
pip install apache-airflow-backport-providers-apache-hive[amazon]
```

| Dependent package                                                                                                                    | Extra           |
|:-------------------------------------------------------------------------------------------------------------------------------------|:----------------|
| [apache-airflow-backport-providers-amazon](https://github.com/apache/airflow/tree/master/airflow/providers/amazon)                   | amazon          |
| [apache-airflow-backport-providers-microsoft-mssql](https://github.com/apache/airflow/tree/master/airflow/providers/microsoft/mssql) | microsoft.mssql |
| [apache-airflow-backport-providers-mysql](https://github.com/apache/airflow/tree/master/airflow/providers/mysql)                     | mysql           |
| [apache-airflow-backport-providers-presto](https://github.com/apache/airflow/tree/master/airflow/providers/presto)                   | presto          |
| [apache-airflow-backport-providers-samba](https://github.com/apache/airflow/tree/master/airflow/providers/samba)                     | samba           |
| [apache-airflow-backport-providers-vertica](https://github.com/apache/airflow/tree/master/airflow/providers/vertica)                 | vertica         |

# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `apache.hive` provider
are in the `airflow.providers.apache.hive` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)


## Operators



### Moved operators

| Airflow 2.0 operators: `airflow.providers.apache.hive` package                                                                                          | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                              |
|:--------------------------------------------------------------------------------------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.hive.HiveOperator](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hive/operators/hive.py)                            | [operators.hive_operator.HiveOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/operators/hive_operator.py)                            |
| [operators.hive_stats.HiveStatsCollectionOperator](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hive/operators/hive_stats.py) | [operators.hive_stats_operator.HiveStatsCollectionOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/operators/hive_stats_operator.py) |


## Transfer operators



### Moved transfer operators

| Airflow 2.0 transfers: `airflow.providers.apache.hive` package                                                                                              | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                |
|:------------------------------------------------------------------------------------------------------------------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [transfers.hive_to_mysql.HiveToMySqlOperator](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hive/transfers/hive_to_mysql.py)       | [operators.hive_to_mysql.HiveToMySqlTransfer](https://github.com/apache/airflow/blob/v1-10-stable/airflow/operators/hive_to_mysql.py)                       |
| [transfers.hive_to_samba.HiveToSambaOperator](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hive/transfers/hive_to_samba.py)       | [operators.hive_to_samba_operator.HiveToSambaOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/operators/hive_to_samba_operator.py)     |
| [transfers.mssql_to_hive.MsSqlToHiveOperator](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hive/transfers/mssql_to_hive.py)       | [operators.mssql_to_hive.MsSqlToHiveTransfer](https://github.com/apache/airflow/blob/v1-10-stable/airflow/operators/mssql_to_hive.py)                       |
| [transfers.mysql_to_hive.MySqlToHiveOperator](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hive/transfers/mysql_to_hive.py)       | [operators.mysql_to_hive.MySqlToHiveTransfer](https://github.com/apache/airflow/blob/v1-10-stable/airflow/operators/mysql_to_hive.py)                       |
| [transfers.s3_to_hive.S3ToHiveOperator](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hive/transfers/s3_to_hive.py)                | [operators.s3_to_hive_operator.S3ToHiveTransfer](https://github.com/apache/airflow/blob/v1-10-stable/airflow/operators/s3_to_hive_operator.py)              |
| [transfers.vertica_to_hive.VerticaToHiveOperator](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hive/transfers/vertica_to_hive.py) | [contrib.operators.vertica_to_hive.VerticaToHiveTransfer](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/vertica_to_hive.py) |


## Sensors



### Moved sensors

| Airflow 2.0 sensors: `airflow.providers.apache.hive` package                                                                                                         | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                       |
|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [sensors.hive_partition.HivePartitionSensor](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hive/sensors/hive_partition.py)                  | [sensors.hive_partition_sensor.HivePartitionSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/sensors/hive_partition_sensor.py)                  |
| [sensors.metastore_partition.MetastorePartitionSensor](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hive/sensors/metastore_partition.py)   | [sensors.metastore_partition_sensor.MetastorePartitionSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/sensors/metastore_partition_sensor.py)   |
| [sensors.named_hive_partition.NamedHivePartitionSensor](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hive/sensors/named_hive_partition.py) | [sensors.named_hive_partition_sensor.NamedHivePartitionSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/sensors/named_hive_partition_sensor.py) |


## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.apache.hive` package                                                                | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                          |
|:--------------------------------------------------------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------|
| [hooks.hive.HiveCliHook](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hive/hooks/hive.py)       | [hooks.hive_hooks.HiveCliHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/hooks/hive_hooks.py)       |
| [hooks.hive.HiveMetastoreHook](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hive/hooks/hive.py) | [hooks.hive_hooks.HiveMetastoreHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/hooks/hive_hooks.py) |
| [hooks.hive.HiveServer2Hook](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hive/hooks/hive.py)   | [hooks.hive_hooks.HiveServer2Hook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/hooks/hive_hooks.py)   |



## Releases

### Release 2020.10.29

| Commit                                                                                         | Committed   | Subject                                                      |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------|
| [b680bbc0b](https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574) | 2020-10-24  | Generated backport providers readmes/setup for 2020.10.29    |
| [349b0811c](https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a) | 2020-10-20  | Add D200 pydocstyle check (#11688)                           |
| [16e712971](https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746) | 2020-10-13  | Added support for provider packages for Airflow 2.0 (#11487) |
| [0a0e1af80](https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa) | 2020-10-03  | Fix Broken Markdown links in Providers README TOC (#11249)   |


### Release 2020.10.5

| Commit                                                                                         | Committed   | Subject                                                                               |
|:-----------------------------------------------------------------------------------------------|:------------|:--------------------------------------------------------------------------------------|
| [ca4238eb4](https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13) | 2020-10-02  | Fixed month in backport packages to October (#11242)                                  |
| [5220e4c38](https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5) | 2020-10-02  | Prepare Backport release 2020.09.07 (#11238)                                          |
| [e3f96ce7a](https://github.com/apache/airflow/commit/e3f96ce7a8ac098aeef5e9930e6de6c428274d57) | 2020-09-24  | Fix incorrect Usage of Optional[bool] (#11138)                                        |
| [f3e87c503](https://github.com/apache/airflow/commit/f3e87c503081a3085dff6c7352640d7f08beb5bc) | 2020-09-22  | Add D202 pydocstyle check (#11032)                                                    |
| [9549274d1](https://github.com/apache/airflow/commit/9549274d110f689a0bd709db829a4d69e274eed9) | 2020-09-09  | Upgrade black to 20.8b1 (#10818)                                                      |
| [ac943c9e1](https://github.com/apache/airflow/commit/ac943c9e18f75259d531dbda8c51e650f57faa4c) | 2020-09-08  | [AIRFLOW-3964][AIP-17] Consolidate and de-dup sensor tasks using Smart Sensor (#5499) |
| [fdd9b6f65](https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3) | 2020-08-25  | Enable Black on Providers Packages (#10543)                                           |
| [d76026545](https://github.com/apache/airflow/commit/d7602654526fdd2876466371404784bd17cfe0d2) | 2020-08-25  | PyDocStyle: No whitespaces allowed surrounding docstring text (#10533)                |
| [3696c34c2](https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34) | 2020-08-24  | Fix typo in the word &#34;release&#34; (#10528)                                               |
| [ee7ca128a](https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94) | 2020-08-22  | Fix broken Markdown refernces in Providers README (#10483)                            |
| [27339a5a0](https://github.com/apache/airflow/commit/27339a5a0f9e382dbc7d32a128f0831a48ef9a12) | 2020-08-22  | Remove mentions of Airflow Gitter (#10460)                                            |
| [7c206a82a](https://github.com/apache/airflow/commit/7c206a82a6f074abcc4898a005ecd2c84a920054) | 2020-08-22  | Replace assigment with Augmented assignment (#10468)                                  |
| [8f8db8959](https://github.com/apache/airflow/commit/8f8db8959e526be54d700845d36ee9f315bae2ea) | 2020-08-12  | DbApiHook: Support kwargs in get_pandas_df (#9730)                                    |
| [b43f90abf](https://github.com/apache/airflow/commit/b43f90abf4c7219d5d59cccb0514256bd3f2fdc7) | 2020-08-09  | Fix various typos in the repo (#10263)                                                |
| [3b3287d7a](https://github.com/apache/airflow/commit/3b3287d7acc76430f12b758d52cec61c7f74e726) | 2020-08-05  | Enforce keyword only arguments on apache operators (#10170)                           |
| [7d24b088c](https://github.com/apache/airflow/commit/7d24b088cd736cfa18f9214e4c9d6ce2d5865f3d) | 2020-07-25  | Stop using start_date in default_args in example_dags (2) (#9985)                     |
| [33f0cd265](https://github.com/apache/airflow/commit/33f0cd2657b2e77ea3477e0c93f13f1474be628e) | 2020-07-22  | apply_default keeps the function signature for mypy (#9784)                           |
| [c2db0dfeb](https://github.com/apache/airflow/commit/c2db0dfeb13ee679bf4d7b57874f0fcb39c0f0ed) | 2020-07-22  | More strict rules in mypy (#9705) (#9906)                                             |
| [5013fda8f](https://github.com/apache/airflow/commit/5013fda8f072e633c114fb39fb59a22f60200b40) | 2020-07-20  | Add drop_partition functionality for HiveMetastoreHook (#9472)                        |
| [4d74ac211](https://github.com/apache/airflow/commit/4d74ac2111862186598daf92cbf2c525617061c2) | 2020-07-19  | Increase typing for Apache and http provider package (#9729)                          |
| [44d4ae809](https://github.com/apache/airflow/commit/44d4ae809c1e3784ff95b6a5e95113c3412e56b3) | 2020-07-06  | Upgrade to latest pre-commit checks (#9686)                                           |
| [e13a14c87](https://github.com/apache/airflow/commit/e13a14c8730f4f633d996dd7d3468fe827136a84) | 2020-06-21  | Enable &amp; Fix Whitespace related PyDocStyle Checks (#9458)                             |
| [d0e7db402](https://github.com/apache/airflow/commit/d0e7db4024806af35e3c9a2cae460fdeedd4d2ec) | 2020-06-19  | Fixed release number for fresh release (#9408)                                        |


### Release 2020.6.24

| Commit                                                                                         | Committed   | Subject                                                                                                                                                            |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [12af6a080](https://github.com/apache/airflow/commit/12af6a08009b8776e00d8a0aab92363eb8c4e8b1) | 2020-06-19  | Final cleanup for 2020.6.23rc1 release preparation (#9404)                                                                                                         |
| [c7e5bce57](https://github.com/apache/airflow/commit/c7e5bce57fe7f51cefce4f8a41ce408ac5675d13) | 2020-06-19  | Prepare backport release candidate for 2020.6.23rc1 (#9370)                                                                                                        |
| [f6bd817a3](https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac) | 2020-06-16  | Introduce &#39;transfers&#39; packages (#9320)                                                                                                                             |
| [c78e2a5fe](https://github.com/apache/airflow/commit/c78e2a5feae15e84b05430cfc5935f0e289fb6b4) | 2020-06-16  | Make hive macros py3 compatible (#8598)                                                                                                                            |
| [6350fd6eb](https://github.com/apache/airflow/commit/6350fd6ebb9958982cb3fa1d466168fc31708035) | 2020-06-08  | Don&#39;t use the term &#34;whitelist&#34; - language matters (#9174)                                                                                                          |
| [10796cb7c](https://github.com/apache/airflow/commit/10796cb7ce52c8ac2f68024e531fdda779547bdf) | 2020-06-03  | Remove Hive/Hadoop/Java dependency from unit tests (#9029)                                                                                                         |
| [0b0e4f7a4](https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34) | 2020-05-26  | Preparing for RC3 release of backports (#9026)                                                                                                                      |
| [00642a46d](https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c) | 2020-05-26  | Fixed name of 20 remaining wrongly named operators. (#8994)                                                                                                        |
| [cdb3f2545](https://github.com/apache/airflow/commit/cdb3f25456e49d0199cd7ccd680626dac01c9be6) | 2020-05-26  | All classes in backport providers are now importable in Airflow 1.10 (#8991)                                                                                       |
| [375d1ca22](https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f) | 2020-05-19  | Release candidate 2 for backport packages 2020.05.20 (#8898)                                                                                                       |
| [12c5e5d8a](https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79) | 2020-05-17  | Prepare release candidate for backport packages (#8891)                                                                                                            |
| [f3521fb0e](https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca) | 2020-05-16  | Regenerate readme files for backport package release (#8886)                                                                                                       |
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807)                                                                                            |
| [93ea05880](https://github.com/apache/airflow/commit/93ea05880283a56e3d42ab07db7453977a3de8ec) | 2020-04-21  | [AIRFLOW-7059] pass hive_conf to get_pandas_df in HiveServer2Hook (#8380)                                                                                          |
| [87969a350](https://github.com/apache/airflow/commit/87969a350ddd41e9e77776af6d780b31e363eaca) | 2020-04-09  | [AIRFLOW-6515] Change Log Levels from Info/Warn to Error (#8170)                                                                                                   |
| [cb0bf4a14](https://github.com/apache/airflow/commit/cb0bf4a142656ee40b43a01660b6f6b08a9840fa) | 2020-03-30  | Remove sql like function in base_hook (#7901)                                                                                                                      |
| [4bde99f13](https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a) | 2020-03-23  | Make airflow/providers pylint compatible (#7802)                                                                                                                   |
| [7e6372a68](https://github.com/apache/airflow/commit/7e6372a681a2a543f4710b083219aeb53b074388) | 2020-03-23  | Add call to Super call in apache providers (#7820)                                                                                                                 |
| [3320e432a](https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc) | 2020-02-24  | [AIRFLOW-6817] Lazy-load `airflow.DAG` to keep user-facing API untouched (#7517)                                                                                   |
| [4d03e33c1](https://github.com/apache/airflow/commit/4d03e33c115018e30fa413c42b16212481ad25cc) | 2020-02-22  | [AIRFLOW-6817] remove imports from `airflow/__init__.py`, replaced implicit imports with explicit imports, added entry to `UPDATING.MD` - squashed/rebased (#7456) |
| [f3ad5cf61](https://github.com/apache/airflow/commit/f3ad5cf6185b9d406d0fb0a4ecc0b5536f79217a) | 2020-02-03  | [AIRFLOW-4681] Make sensors module pylint compatible (#7309)                                                                                                       |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                                                                                                           |
| [83c037873](https://github.com/apache/airflow/commit/83c037873ff694eed67ba8b30f2d9c88b2c7c6f2) | 2020-01-30  | [AIRFLOW-6674] Move example_dags in accordance with AIP-21 (#7287)                                                                                                 |
| [057f3ae3a](https://github.com/apache/airflow/commit/057f3ae3a4afedf6d462ecf58b01dd6304d3e135) | 2020-01-29  | [AIRFLOW-6670][depends on AIRFLOW-6669] Move contrib operators to providers package (#7286)                                                                        |
| [059eda05f](https://github.com/apache/airflow/commit/059eda05f82fefce4410f44f761f945a27d83daf) | 2020-01-21  | [AIRFLOW-6610] Move software classes to providers package (#7231)                                                                                                  |
| [0481b9a95](https://github.com/apache/airflow/commit/0481b9a95786a62de4776a735ae80e746583ef2b) | 2020-01-12  | [AIRFLOW-6539][AIP-21] Move Apache classes to providers.apache package (#7142)                                                                                     |

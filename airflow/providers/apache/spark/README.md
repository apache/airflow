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


# Package apache-airflow-providers-apache-spark

Release: 1.0.0

**Table of contents**

- [Provider package](#provider-package)
- [Installation](#installation)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-classes-summary)
    - [Operators](#operators)
        - [Moved operators](#moved-operators)
    - [Hooks](#hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 1.0.0](#release-100)

## Provider package

This is a provider package for `apache.spark` provider. All classes for this provider package
are in `airflow.providers.apache.spark` python package.



## Installation

NOTE!

On November 2020, new version of PIP (20.3) has been released with a new, 2020 resolver. This resolver
does not yet work with Apache Airflow and might leads to errors in installation - depends on your choice
of extras. In order to install Airflow you need to either downgrade pip to version 20.2.4
`pip upgrade --pip==20.2.4` or, in case you use Pip 20.3, you need to add option
`--use-deprecated legacy-resolver` to your pip install command.

You can install this package on top of an existing airflow 2.* installation via
`pip install apache-airflow-providers-apache-spark`

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

### Release 1.0.0

| Commit                                                                                         | Committed   | Subject                                                                                          |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------------------------------------|
| [b40dffa08](https://github.com/apache/airflow/commit/b40dffa08547b610162f8cacfa75847f3c4ca364) | 2020-12-08  | Rename remaing modules to match AIP-21 (#12917)                                                  |
| [9b39f2478](https://github.com/apache/airflow/commit/9b39f24780e85f859236672e9060b2fbeee81b36) | 2020-12-08  | Add support for dynamic connection form fields per provider (#12558)                             |
| [bd90136aa](https://github.com/apache/airflow/commit/bd90136aaf5035e3234fe545b79a3e4aad21efe2) | 2020-11-30  | Move operator guides to provider documentation packages (#12681)                                 |
| [c34ef853c](https://github.com/apache/airflow/commit/c34ef853c890e08f5468183c03dc8f3f3ce84af2) | 2020-11-20  | Separate out documentation building per provider  (#12444)                                       |
| [008035450](https://github.com/apache/airflow/commit/00803545023b096b8db4fbd6eb473843096d7ce4) | 2020-11-18  | Update provider READMEs for 1.0.0b2 batch release (#12449)                                       |
| [ae7cb4a1e](https://github.com/apache/airflow/commit/ae7cb4a1e2a96351f1976cf5832615e24863e05d) | 2020-11-17  | Update wrong commit hash in backport provider changes (#12390)                                   |
| [6889a333c](https://github.com/apache/airflow/commit/6889a333cff001727eb0a66e375544a28c9a5f03) | 2020-11-15  | Improvements for operators and hooks ref docs (#12366)                                           |
| [7825e8f59](https://github.com/apache/airflow/commit/7825e8f59034645ab3247229be83a3aa90baece1) | 2020-11-13  | Docs installation improvements (#12304)                                                          |
| [85a18e13d](https://github.com/apache/airflow/commit/85a18e13d9dec84275283ff69e34704b60d54a75) | 2020-11-09  | Point at pypi project pages for cross-dependency of provider packages (#12212)                   |
| [59eb5de78](https://github.com/apache/airflow/commit/59eb5de78c70ee9c7ae6e4cba5c7a2babb8103ca) | 2020-11-09  | Update provider READMEs for up-coming 1.0.0beta1 releases (#12206)                               |
| [b2a28d159](https://github.com/apache/airflow/commit/b2a28d1590410630d66966aa1f2b2a049a8c3b32) | 2020-11-09  | Moves provider packages scripts to dev (#12082)                                                  |
| [4e8f9cc8d](https://github.com/apache/airflow/commit/4e8f9cc8d02b29c325b8a5a76b4837671bdf5f68) | 2020-11-03  | Enable Black - Python Auto Formmatter (#9550)                                                    |
| [8c42cf1b0](https://github.com/apache/airflow/commit/8c42cf1b00c90f0d7f11b8a3a455381de8e003c5) | 2020-11-03  | Use PyUpgrade to use Python 3.6 features (#11447)                                                |
| [5a439e84e](https://github.com/apache/airflow/commit/5a439e84eb6c0544dc6c3d6a9f4ceeb2172cd5d0) | 2020-10-26  | Prepare providers release 0.0.2a1 (#11855)                                                       |
| [872b1566a](https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574) | 2020-10-25  | Generated backport providers readmes/setup for 2020.10.29 (#11826)                               |
| [349b0811c](https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a) | 2020-10-20  | Add D200 pydocstyle check (#11688)                                                               |
| [16e712971](https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746) | 2020-10-13  | Added support for provider packages for Airflow 2.0 (#11487)                                     |
| [d305876be](https://github.com/apache/airflow/commit/d305876bee328287ff391a29cc1cd632468cc731) | 2020-10-12  | Remove redundant None provided as default to dict.get() (#11448)                                 |
| [0a0e1af80](https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa) | 2020-10-03  | Fix Broken Markdown links in Providers README TOC (#11249)                                       |
| [ca4238eb4](https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13) | 2020-10-02  | Fixed month in backport packages to October (#11242)                                             |
| [5220e4c38](https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5) | 2020-10-02  | Prepare Backport release 2020.09.07 (#11238)                                                     |
| [f3e87c503](https://github.com/apache/airflow/commit/f3e87c503081a3085dff6c7352640d7f08beb5bc) | 2020-09-22  | Add D202 pydocstyle check (#11032)                                                               |
| [fdd9b6f65](https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3) | 2020-08-25  | Enable Black on Providers Packages (#10543)                                                      |
| [d76026545](https://github.com/apache/airflow/commit/d7602654526fdd2876466371404784bd17cfe0d2) | 2020-08-25  | PyDocStyle: No whitespaces allowed surrounding docstring text (#10533)                           |
| [d1bce91bb](https://github.com/apache/airflow/commit/d1bce91bb21d5a468fa6a0207156c28fe1ca6513) | 2020-08-25  | PyDocStyle: Enable D403: Capitalized first word of docstring (#10530)                            |
| [3696c34c2](https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34) | 2020-08-24  | Fix typo in the word &#34;release&#34; (#10528)                                                          |
| [ee7ca128a](https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94) | 2020-08-22  | Fix broken Markdown refernces in Providers README (#10483)                                       |
| [7c206a82a](https://github.com/apache/airflow/commit/7c206a82a6f074abcc4898a005ecd2c84a920054) | 2020-08-22  | Replace assigment with Augmented assignment (#10468)                                             |
| [3b3287d7a](https://github.com/apache/airflow/commit/3b3287d7acc76430f12b758d52cec61c7f74e726) | 2020-08-05  | Enforce keyword only arguments on apache operators (#10170)                                      |
| [7d24b088c](https://github.com/apache/airflow/commit/7d24b088cd736cfa18f9214e4c9d6ce2d5865f3d) | 2020-07-25  | Stop using start_date in default_args in example_dags (2) (#9985)                                |
| [33f0cd265](https://github.com/apache/airflow/commit/33f0cd2657b2e77ea3477e0c93f13f1474be628e) | 2020-07-22  | apply_default keeps the function signature for mypy (#9784)                                      |
| [1427e4acb](https://github.com/apache/airflow/commit/1427e4acb4a1dc5be28cfeef75c90032d515aab6) | 2020-07-22  | Update Spark submit operator for Spark 3 support (#8730)                                         |
| [4d74ac211](https://github.com/apache/airflow/commit/4d74ac2111862186598daf92cbf2c525617061c2) | 2020-07-19  | Increase typing for Apache and http provider package (#9729)                                     |
| [0873070e0](https://github.com/apache/airflow/commit/0873070e08f7216b6949e7de4e2329175a764321) | 2020-07-11  | Mask other forms of password arguments in SparkSubmitOperator (#9615)                            |
| [13a827d80](https://github.com/apache/airflow/commit/13a827d80fef738e25f30ea20c095ad4dbd401f6) | 2020-07-09  | Ensure Kerberos token is valid in SparkSubmitOperator before running `yarn kill` (#9044)         |
| [067806d59](https://github.com/apache/airflow/commit/067806d5985301f21da78f0a81056dbec348e6ba) | 2020-06-29  | Add tests for spark_jdbc_script (#9491)                                                          |
| [d0e7db402](https://github.com/apache/airflow/commit/d0e7db4024806af35e3c9a2cae460fdeedd4d2ec) | 2020-06-19  | Fixed release number for fresh release (#9408)                                                   |
| [12af6a080](https://github.com/apache/airflow/commit/12af6a08009b8776e00d8a0aab92363eb8c4e8b1) | 2020-06-19  | Final cleanup for 2020.6.23rc1 release preparation (#9404)                                       |
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

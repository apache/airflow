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


# Package apache-airflow-providers-apache-hdfs

Release: 1.0.0b2

**Table of contents**

- [Provider package](#provider-package)
- [Installation](#installation)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-classes-summary)
    - [Sensors](#sensors)
        - [Moved sensors](#moved-sensors)
    - [Hooks](#hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 1.0.0b2](#release-100b2)
    - [Release 1.0.0b1](#release-100b1)
    - [Release 0.0.2a1](#release-002a1)
    - [Release 0.0.1](#release-001)

## Provider package

This is a provider package for `apache.hdfs` provider. All classes for this provider package
are in `airflow.providers.apache.hdfs` python package.



## Installation

You can install this package on top of an existing airflow 2.* installation via
`pip install apache-airflow-providers-apache-hdfs`

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| snakebite-py3 |                    |

# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `apache.hdfs` provider
are in the `airflow.providers.apache.hdfs` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)


## Sensors



### Moved sensors

| Airflow 2.0 sensors: `airflow.providers.apache.hdfs` package                                                                      | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                               |
|:----------------------------------------------------------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------|
| [sensors.hdfs.HdfsFolderSensor](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hdfs/sensors/hdfs.py)      | [contrib.sensors.hdfs_sensor.HdfsSensorFolder](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/hdfs_sensor.py) |
| [sensors.hdfs.HdfsRegexSensor](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hdfs/sensors/hdfs.py)       | [contrib.sensors.hdfs_sensor.HdfsSensorRegex](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/hdfs_sensor.py)  |
| [sensors.hdfs.HdfsSensor](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hdfs/sensors/hdfs.py)            | [sensors.hdfs_sensor.HdfsSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/sensors/hdfs_sensor.py)                       |
| [sensors.web_hdfs.WebHdfsSensor](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hdfs/sensors/web_hdfs.py) | [sensors.web_hdfs_sensor.WebHdfsSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/sensors/web_hdfs_sensor.py)            |


## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.apache.hdfs` package                                                                | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                        |
|:--------------------------------------------------------------------------------------------------------------------------|:--------------------------------------------------------------------------------------------------------------------|
| [hooks.hdfs.HDFSHook](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hdfs/hooks/hdfs.py)          | [hooks.hdfs_hook.HDFSHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/hooks/hdfs_hook.py)          |
| [hooks.webhdfs.WebHDFSHook](https://github.com/apache/airflow/blob/master/airflow/providers/apache/hdfs/hooks/webhdfs.py) | [hooks.webhdfs_hook.WebHDFSHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/hooks/webhdfs_hook.py) |



## Releases

### Release 1.0.0b2

| Commit                                                                                         | Committed   | Subject                                                                        |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------------------|
| [ae7cb4a1e](https://github.com/apache/airflow/commit/ae7cb4a1e2a96351f1976cf5832615e24863e05d) | 2020-11-17  | Update wrong commit hash in backport provider changes (#12390)                 |
| [6889a333c](https://github.com/apache/airflow/commit/6889a333cff001727eb0a66e375544a28c9a5f03) | 2020-11-15  | Improvements for operators and hooks ref docs (#12366)                         |
| [7825e8f59](https://github.com/apache/airflow/commit/7825e8f59034645ab3247229be83a3aa90baece1) | 2020-11-13  | Docs installation improvements (#12304)                                        |
| [85a18e13d](https://github.com/apache/airflow/commit/85a18e13d9dec84275283ff69e34704b60d54a75) | 2020-11-09  | Point at pypi project pages for cross-dependency of provider packages (#12212) |


### Release 1.0.0b1

| Commit                                                                                         | Committed   | Subject                                                            |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------|
| [59eb5de78](https://github.com/apache/airflow/commit/59eb5de78c70ee9c7ae6e4cba5c7a2babb8103ca) | 2020-11-09  | Update provider READMEs for up-coming 1.0.0beta1 releases (#12206) |
| [b2a28d159](https://github.com/apache/airflow/commit/b2a28d1590410630d66966aa1f2b2a049a8c3b32) | 2020-11-09  | Moves provider packages scripts to dev (#12082)                    |
| [41bf172c1](https://github.com/apache/airflow/commit/41bf172c1dc75099f4f9d8b3f3350b4b1f523ef9) | 2020-11-04  | Simplify string expressions (#12093)                               |
| [4e8f9cc8d](https://github.com/apache/airflow/commit/4e8f9cc8d02b29c325b8a5a76b4837671bdf5f68) | 2020-11-03  | Enable Black - Python Auto Formmatter (#9550)                      |
| [8c42cf1b0](https://github.com/apache/airflow/commit/8c42cf1b00c90f0d7f11b8a3a455381de8e003c5) | 2020-11-03  | Use PyUpgrade to use Python 3.6 features (#11447)                  |
| [5a439e84e](https://github.com/apache/airflow/commit/5a439e84eb6c0544dc6c3d6a9f4ceeb2172cd5d0) | 2020-10-26  | Prepare providers release 0.0.2a1 (#11855)                         |


### Release 0.0.2a1

| Commit                                                                                         | Committed   | Subject                                                            |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------|
| [872b1566a](https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574) | 2020-10-25  | Generated backport providers readmes/setup for 2020.10.29 (#11826) |
| [349b0811c](https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a) | 2020-10-20  | Add D200 pydocstyle check (#11688)                                 |
| [16e712971](https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746) | 2020-10-13  | Added support for provider packages for Airflow 2.0 (#11487)       |


### Release 0.0.1

| Commit                                                                                         | Committed   | Subject                                                                        |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------------------|
| [0a0e1af80](https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa) | 2020-10-03  | Fix Broken Markdown links in Providers README TOC (#11249)                     |
| [ca4238eb4](https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13) | 2020-10-02  | Fixed month in backport packages to October (#11242)                           |
| [5220e4c38](https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5) | 2020-10-02  | Prepare Backport release 2020.09.07 (#11238)                                   |
| [fdd9b6f65](https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3) | 2020-08-25  | Enable Black on Providers Packages (#10543)                                    |
| [d1bce91bb](https://github.com/apache/airflow/commit/d1bce91bb21d5a468fa6a0207156c28fe1ca6513) | 2020-08-25  | PyDocStyle: Enable D403: Capitalized first word of docstring (#10530)          |
| [3696c34c2](https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34) | 2020-08-24  | Fix typo in the word &#34;release&#34; (#10528)                                        |
| [2f2d8dbfa](https://github.com/apache/airflow/commit/2f2d8dbfafefb4be3dd80f22f31c649c8498f148) | 2020-08-25  | Remove all &#34;noinspection&#34; comments native to IntelliJ (#10525)                 |
| [ee7ca128a](https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94) | 2020-08-22  | Fix broken Markdown refernces in Providers README (#10483)                     |
| [3b3287d7a](https://github.com/apache/airflow/commit/3b3287d7acc76430f12b758d52cec61c7f74e726) | 2020-08-05  | Enforce keyword only arguments on apache operators (#10170)                    |
| [33f0cd265](https://github.com/apache/airflow/commit/33f0cd2657b2e77ea3477e0c93f13f1474be628e) | 2020-07-22  | apply_default keeps the function signature for mypy (#9784)                    |
| [4d74ac211](https://github.com/apache/airflow/commit/4d74ac2111862186598daf92cbf2c525617061c2) | 2020-07-19  | Increase typing for Apache and http provider package (#9729)                   |
| [44d4ae809](https://github.com/apache/airflow/commit/44d4ae809c1e3784ff95b6a5e95113c3412e56b3) | 2020-07-06  | Upgrade to latest pre-commit checks (#9686)                                    |
| [d0e7db402](https://github.com/apache/airflow/commit/d0e7db4024806af35e3c9a2cae460fdeedd4d2ec) | 2020-06-19  | Fixed release number for fresh release (#9408)                                 |
| [12af6a080](https://github.com/apache/airflow/commit/12af6a08009b8776e00d8a0aab92363eb8c4e8b1) | 2020-06-19  | Final cleanup for 2020.6.23rc1 release preparation (#9404)                     |
| [c7e5bce57](https://github.com/apache/airflow/commit/c7e5bce57fe7f51cefce4f8a41ce408ac5675d13) | 2020-06-19  | Prepare backport release candidate for 2020.6.23rc1 (#9370)                    |
| [f6bd817a3](https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac) | 2020-06-16  | Introduce &#39;transfers&#39; packages (#9320)                                         |
| [0b0e4f7a4](https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34) | 2020-05-26  | Preparing for RC3 relase of backports (#9026)                                  |
| [00642a46d](https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c) | 2020-05-26  | Fixed name of 20 remaining wrongly named operators. (#8994)                    |
| [375d1ca22](https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f) | 2020-05-19  | Release candidate 2 for backport packages 2020.05.20 (#8898)                   |
| [12c5e5d8a](https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79) | 2020-05-17  | Prepare release candidate for backport packages (#8891)                        |
| [f3521fb0e](https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca) | 2020-05-16  | Regenerate readme files for backport package release (#8886)                   |
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807)        |
| [87969a350](https://github.com/apache/airflow/commit/87969a350ddd41e9e77776af6d780b31e363eaca) | 2020-04-09  | [AIRFLOW-6515] Change Log Levels from Info/Warn to Error (#8170)               |
| [6c670870a](https://github.com/apache/airflow/commit/6c670870aa6ea5d82a86f912bb6de8b88e711ca5) | 2020-03-25  | [AIRFLOW-6833] HA for webhdfs connection (#7454)                               |
| [7e6372a68](https://github.com/apache/airflow/commit/7e6372a681a2a543f4710b083219aeb53b074388) | 2020-03-23  | Add call to Super call in apache providers (#7820)                             |
| [f3ad5cf61](https://github.com/apache/airflow/commit/f3ad5cf6185b9d406d0fb0a4ecc0b5536f79217a) | 2020-02-03  | [AIRFLOW-4681] Make sensors module pylint compatible (#7309)                   |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                       |
| [cf141506a](https://github.com/apache/airflow/commit/cf141506a25dbba279b85500d781f7e056540721) | 2020-02-02  | [AIRFLOW-6708] Set unique logger names (#7330)                                 |
| [0481b9a95](https://github.com/apache/airflow/commit/0481b9a95786a62de4776a735ae80e746583ef2b) | 2020-01-12  | [AIRFLOW-6539][AIP-21] Move Apache classes to providers.apache package (#7142) |

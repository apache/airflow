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


# Package apache-airflow-backport-providers-elasticsearch

Release: 2020.11.13

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Provider class summary](#provider-classes-summary)
    - [Hooks](#hooks)
        - [New hooks](#new-hooks)
- [Releases](#releases)
    - [Release 2020.11.13](#release-20201113)
    - [Release 2020.10.29](#release-20201029)
    - [Release 2020.10.5](#release-2020105)
    - [Release 2020.6.24](#release-2020624)

## Backport package

This is a backport providers package for `elasticsearch` provider. All classes for this provider package
are in `airflow.providers.elasticsearch` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.



## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-elasticsearch`

# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `elasticsearch` provider
are in the `airflow.providers.elasticsearch` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)


## Hooks


### New hooks

| New Airflow 2.0 hooks: `airflow.providers.elasticsearch` package                                                                              |
|:----------------------------------------------------------------------------------------------------------------------------------------------|
| [hooks.elasticsearch.ElasticsearchHook](https://github.com/apache/airflow/blob/master/airflow/providers/elasticsearch/hooks/elasticsearch.py) |




## Releases

### Release 2020.11.13

| Commit                                                                                         | Committed   | Subject                                                            |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------|
| [b2a28d159](https://github.com/apache/airflow/commit/b2a28d1590410630d66966aa1f2b2a049a8c3b32) | 2020-11-09  | Moves provider packages scripts to dev (#12082)                    |
| [4e8f9cc8d](https://github.com/apache/airflow/commit/4e8f9cc8d02b29c325b8a5a76b4837671bdf5f68) | 2020-11-03  | Enable Black - Python Auto Formmatter (#9550)                      |
| [8c42cf1b0](https://github.com/apache/airflow/commit/8c42cf1b00c90f0d7f11b8a3a455381de8e003c5) | 2020-11-03  | Use PyUpgrade to use Python 3.6 features (#11447)                  |
| [5a439e84e](https://github.com/apache/airflow/commit/5a439e84eb6c0544dc6c3d6a9f4ceeb2172cd5d0) | 2020-10-26  | Prepare providers release 0.0.2a1 (#11855)                         |
| [872b1566a](https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574) | 2020-10-25  | Generated backport providers readmes/setup for 2020.10.29 (#11826) |
| [b680bbc0b](https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574) | 2020-10-24  | Generated backport providers readmes/setup for 2020.10.29          |


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
| [f3e87c503](https://github.com/apache/airflow/commit/f3e87c503081a3085dff6c7352640d7f08beb5bc) | 2020-09-22  | Add D202 pydocstyle check (#11032)                                                    |
| [ac943c9e1](https://github.com/apache/airflow/commit/ac943c9e18f75259d531dbda8c51e650f57faa4c) | 2020-09-08  | [AIRFLOW-3964][AIP-17] Consolidate and de-dup sensor tasks using Smart Sensor (#5499) |
| [70f05ac67](https://github.com/apache/airflow/commit/70f05ac6775152d856d212f845e9561282232844) | 2020-09-01  | Add `log_id` field to log lines on ES handler (#10411)                                |
| [fdd9b6f65](https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3) | 2020-08-25  | Enable Black on Providers Packages (#10543)                                           |
| [d76026545](https://github.com/apache/airflow/commit/d7602654526fdd2876466371404784bd17cfe0d2) | 2020-08-25  | PyDocStyle: No whitespaces allowed surrounding docstring text (#10533)                |
| [3696c34c2](https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34) | 2020-08-24  | Fix typo in the word &#34;release&#34; (#10528)                                               |
| [ee7ca128a](https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94) | 2020-08-22  | Fix broken Markdown refernces in Providers README (#10483)                            |
| [d5d119bab](https://github.com/apache/airflow/commit/d5d119babc97bbe3f3f690ad4a93e3b73bd3b172) | 2020-07-21  | Increase typing coverage for Elasticsearch (#9911)                                    |
| [a79e2d4c4](https://github.com/apache/airflow/commit/a79e2d4c4aa105f3fac5ae6a28e29af9cd572407) | 2020-07-06  | Move provider&#39;s log task handlers to the provider package (#9604)                     |
| [e13a14c87](https://github.com/apache/airflow/commit/e13a14c8730f4f633d996dd7d3468fe827136a84) | 2020-06-21  | Enable &amp; Fix Whitespace related PyDocStyle Checks (#9458)                             |
| [d0e7db402](https://github.com/apache/airflow/commit/d0e7db4024806af35e3c9a2cae460fdeedd4d2ec) | 2020-06-19  | Fixed release number for fresh release (#9408)                                        |


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
| [65dd28eb7](https://github.com/apache/airflow/commit/65dd28eb77d996ec8306c67d5ce1ccee2c14cc9d) | 2020-02-18  | [AIRFLOW-1202] Create Elasticsearch Hook (#7358)                        |

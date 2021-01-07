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


# Package apache-airflow-providers-presto

Release: 1.0.0

**Table of contents**

- [Provider package](#provider-package)
- [Installation](#installation)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-classes-summary)
    - [Hooks](#hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 1.0.0](#release-100)

## Provider package

This is a provider package for `presto` provider. All classes for this provider package
are in `airflow.providers.presto` python package.


## Additional limitations

The presto provider might not work correctly with Kerberos + SSL when it is installed together with the
Snowflake provider. It&#39;s because of Snowflake monkeypatching the urllib3
library as described in [this issue](https://github.com/snowflakedb/snowflake-connector-python/issues/324)
the offending code is [here](https://github.com/snowflakedb/snowflake-connector-python/blob/133d6215f7920d304c5f2d466bae38127c1b836d/src/snowflake/connector/network.py#L89-L92)

In the future Snowflake plans to get rid of the monkeypatching.

You can keep track of [the issue](https://github.com/apache/airflow/issues/12881) in order to know when the
issue will be resolved.


## Installation

NOTE!

On November 2020, new version of PIP (20.3) has been released with a new, 2020 resolver. This resolver
does not yet work with Apache Airflow and might lead to errors in installation - depends on your choice
of extras. In order to install Airflow you need to either downgrade pip to version 20.2.4
`pip install --upgrade pip==20.2.4` or, in case you use Pip 20.3, you need to add option
`--use-deprecated legacy-resolver` to your pip install command.

You can install this package on top of an existing airflow 2.* installation via
`pip install apache-airflow-providers-presto`

## PIP requirements

| PIP package          | Version required   |
|:---------------------|:-------------------|
| presto-python-client | &gt;=0.7.0,&lt;0.8       |

# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `presto` provider
are in the `airflow.providers.presto` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)


## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.presto` package                                                             | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                     |
|:------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------|
| [hooks.presto.PrestoHook](https://github.com/apache/airflow/blob/master/airflow/providers/presto/hooks/presto.py) | [hooks.presto_hook.PrestoHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/hooks/presto_hook.py) |



## Releases

### Release 1.0.0

| Commit                                                                                         | Committed   | Subject                                                                        |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------------------|
| [5c74c3a5c](https://github.com/apache/airflow/commit/5c74c3a5c1bc6424a068f1dd21a2d999b92cd8c5) | 2020-12-09  | Add explanation of the Presto Kerberos + SSL + snowflake problem (#12939)      |
| [b40dffa08](https://github.com/apache/airflow/commit/b40dffa08547b610162f8cacfa75847f3c4ca364) | 2020-12-08  | Rename remaing modules to match AIP-21 (#12917)                                |
| [9b39f2478](https://github.com/apache/airflow/commit/9b39f24780e85f859236672e9060b2fbeee81b36) | 2020-12-08  | Add support for dynamic connection form fields per provider (#12558)           |
| [2037303ee](https://github.com/apache/airflow/commit/2037303eef93fd36ab13746b045d1c1fee6aa143) | 2020-11-29  | Adds support for Connection/Hook discovery from providers (#12466)             |
| [c34ef853c](https://github.com/apache/airflow/commit/c34ef853c890e08f5468183c03dc8f3f3ce84af2) | 2020-11-20  | Separate out documentation building per provider  (#12444)                     |
| [008035450](https://github.com/apache/airflow/commit/00803545023b096b8db4fbd6eb473843096d7ce4) | 2020-11-18  | Update provider READMEs for 1.0.0b2 batch release (#12449)                     |
| [ae7cb4a1e](https://github.com/apache/airflow/commit/ae7cb4a1e2a96351f1976cf5832615e24863e05d) | 2020-11-17  | Update wrong commit hash in backport provider changes (#12390)                 |
| [6889a333c](https://github.com/apache/airflow/commit/6889a333cff001727eb0a66e375544a28c9a5f03) | 2020-11-15  | Improvements for operators and hooks ref docs (#12366)                         |
| [7825e8f59](https://github.com/apache/airflow/commit/7825e8f59034645ab3247229be83a3aa90baece1) | 2020-11-13  | Docs installation improvements (#12304)                                        |
| [85a18e13d](https://github.com/apache/airflow/commit/85a18e13d9dec84275283ff69e34704b60d54a75) | 2020-11-09  | Point at pypi project pages for cross-dependency of provider packages (#12212) |
| [59eb5de78](https://github.com/apache/airflow/commit/59eb5de78c70ee9c7ae6e4cba5c7a2babb8103ca) | 2020-11-09  | Update provider READMEs for up-coming 1.0.0beta1 releases (#12206)             |
| [b2a28d159](https://github.com/apache/airflow/commit/b2a28d1590410630d66966aa1f2b2a049a8c3b32) | 2020-11-09  | Moves provider packages scripts to dev (#12082)                                |
| [4e8f9cc8d](https://github.com/apache/airflow/commit/4e8f9cc8d02b29c325b8a5a76b4837671bdf5f68) | 2020-11-03  | Enable Black - Python Auto Formmatter (#9550)                                  |
| [5a439e84e](https://github.com/apache/airflow/commit/5a439e84eb6c0544dc6c3d6a9f4ceeb2172cd5d0) | 2020-10-26  | Prepare providers release 0.0.2a1 (#11855)                                     |
| [872b1566a](https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574) | 2020-10-25  | Generated backport providers readmes/setup for 2020.10.29 (#11826)             |
| [349b0811c](https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a) | 2020-10-20  | Add D200 pydocstyle check (#11688)                                             |
| [1543923c1](https://github.com/apache/airflow/commit/1543923c197f658533ca0a0bb259b59a002cce43) | 2020-10-20  | Add Kerberos Auth for PrestoHook (#10488)                                      |
| [16e712971](https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746) | 2020-10-13  | Added support for provider packages for Airflow 2.0 (#11487)                   |
| [0a0e1af80](https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa) | 2020-10-03  | Fix Broken Markdown links in Providers README TOC (#11249)                     |
| [ca4238eb4](https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13) | 2020-10-02  | Fixed month in backport packages to October (#11242)                           |
| [5220e4c38](https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5) | 2020-10-02  | Prepare Backport release 2020.09.07 (#11238)                                   |
| [720912f67](https://github.com/apache/airflow/commit/720912f67b3af0bdcbac64d6b8bf6d51c6247e26) | 2020-10-02  | Strict type check for multiple providers (#11229)                              |
| [fdd9b6f65](https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3) | 2020-08-25  | Enable Black on Providers Packages (#10543)                                    |
| [3696c34c2](https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34) | 2020-08-24  | Fix typo in the word &#34;release&#34; (#10528)                                        |
| [ee7ca128a](https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94) | 2020-08-22  | Fix broken Markdown refernces in Providers README (#10483)                     |
| [8f8db8959](https://github.com/apache/airflow/commit/8f8db8959e526be54d700845d36ee9f315bae2ea) | 2020-08-12  | DbApiHook: Support kwargs in get_pandas_df (#9730)                             |
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
| [1100cea1f](https://github.com/apache/airflow/commit/1100cea1fb9e010e6f4acc699c6d54d056c0541c) | 2020-05-03  | Remove _get_pretty_exception_message in PrestoHook                             |
| [35834c380](https://github.com/apache/airflow/commit/35834c3809ce6f5f1dcff130d0e68cabed7f72de) | 2020-03-26  | Remove Presto check operators (#7884)                                          |
| [4bde99f13](https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a) | 2020-03-23  | Make airflow/providers pylint compatible (#7802)                               |
| [029c84e55](https://github.com/apache/airflow/commit/029c84e5527b6db6bdbdbe026f455da325bedef3) | 2020-03-18  | [AIRFLOW-5421] Add Presto to GCS transfer operator (#7718)                     |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                       |
| [059eda05f](https://github.com/apache/airflow/commit/059eda05f82fefce4410f44f761f945a27d83daf) | 2020-01-21  | [AIRFLOW-6610] Move software classes to providers package (#7231)              |

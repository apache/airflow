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


# Package apache-airflow-providers-sendgrid

Release: 1.0.0

**Table of contents**

- [Provider package](#provider-package)
- [Installation](#installation)
- [PIP requirements](#pip-requirements)
- [Provider class summary](#provider-classes-summary)
- [Releases](#releases)
    - [Release 1.0.0](#release-100)

## Provider package

This is a provider package for `sendgrid` provider. All classes for this provider package
are in `airflow.providers.sendgrid` python package.



## Installation

NOTE!

On November 2020, new version of PIP (20.3) has been released with a new, 2020 resolver. This resolver
does not yet work with Apache Airflow and might leads to errors in installation - depends on your choice
of extras. In order to install Airflow you need to either downgrade pip to version 20.2.4
`pip upgrade --pip==20.2.4` or, in case you use Pip 20.3, you need to add option
`--use-deprecated legacy-resolver` to your pip install command.

You can install this package on top of an existing airflow 2.* installation via
`pip install apache-airflow-providers-sendgrid`

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| sendgrid      | &gt;=6.0.0,&lt;7         |

# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `sendgrid` provider
are in the `airflow.providers.sendgrid` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)



## Releases

### Release 1.0.0

| Commit                                                                                         | Committed   | Subject                                                          |
|:-----------------------------------------------------------------------------------------------|:------------|:-----------------------------------------------------------------|
| [c34ef853c](https://github.com/apache/airflow/commit/c34ef853c890e08f5468183c03dc8f3f3ce84af2) | 2020-11-20  | Separate out documentation building per provider  (#12444)       |
| [008035450](https://github.com/apache/airflow/commit/00803545023b096b8db4fbd6eb473843096d7ce4) | 2020-11-18  | Update provider READMEs for 1.0.0b2 batch release (#12449)       |
| [7825e8f59](https://github.com/apache/airflow/commit/7825e8f59034645ab3247229be83a3aa90baece1) | 2020-11-13  | Docs installation improvements (#12304)                          |
| [c5806efb5](https://github.com/apache/airflow/commit/c5806efb54ad06049e13a5fc7df2f03846fe566e) | 2020-11-10  | Added missing sendgrid readme (#12245)                           |
| [349b0811c](https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a) | 2020-10-20  | Add D200 pydocstyle check (#11688)                               |
| [d305876be](https://github.com/apache/airflow/commit/d305876bee328287ff391a29cc1cd632468cc731) | 2020-10-12  | Remove redundant None provided as default to dict.get() (#11448) |
| [f3e87c503](https://github.com/apache/airflow/commit/f3e87c503081a3085dff6c7352640d7f08beb5bc) | 2020-09-22  | Add D202 pydocstyle check (#11032)                               |
| [fdd9b6f65](https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3) | 2020-08-25  | Enable Black on Providers Packages (#10543)                      |
| [7269d15ad](https://github.com/apache/airflow/commit/7269d15adfb74188359757b1705485f5d368486a) | 2020-08-03  | [GH-9708] Add type coverage to Sendgrid module (#10134)          |
| [a97400d0d](https://github.com/apache/airflow/commit/a97400d0d89ccd6de0cab3a50c58a2969d164a0d) | 2020-06-28  | Move out sendgrid emailer from airflow.contrib (#9355)           |

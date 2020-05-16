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

Release: 2020.05.19

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [Provider class summary](#provider-class-summary)
    - [Hooks](#hooks)
        - [New hooks](#new-hooks)
- [Releases](#releases)
    - [Release 2020.05.19](#release-20200519)

## Backport package

This is a backport providers package for `elasticsearch` provider. All classes for this provider package
are in `airflow.providers.elasticsearch` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-elasticsearch`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.elasticsearch` package.





## Hooks


### New hooks

| New Airflow 2.0 hooks: `airflow.providers.elasticsearch` package                                                                              |
|:----------------------------------------------------------------------------------------------------------------------------------------------|
| [hooks.elasticsearch.ElasticsearchHook](https://github.com/apache/airflow/blob/master/airflow/providers/elasticsearch/hooks/elasticsearch.py) |







## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                 |
|:-----------------------------------------------------------------------------------------------|:------------|:------------------------------------------------------------------------|
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807) |
| [65dd28eb7](https://github.com/apache/airflow/commit/65dd28eb77d996ec8306c67d5ce1ccee2c14cc9d) | 2020-02-18  | [AIRFLOW-1202] Create Elasticsearch Hook (#7358)                        |

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


# Package apache-airflow-backport-providers-slack

Release: 2020.05.19

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Cross provider package dependencies](#cross-provider-package-dependencies)
- [Provider class summary](#provider-class-summary)
    - [Operators](#operators)
        - [Moved operators](#moved-operators)
    - [Hooks](#hooks)
        - [Moved hooks](#moved-hooks)
- [Releases](#releases)
    - [Release 2020.05.19](#release-20200519)

## Backport package

This is a backport providers package for `slack` provider. All classes for this provider package
are in `airflow.providers.slack` python package.

## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-slack`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| slackclient   | &gt;=2.0.0,&lt;3.0.0     |

## Cross provider package dependencies

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified backport providers package in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

```bash
pip install apache-airflow-backport-providers-slack[http]
```

| Dependent package                                                                                              | Extra   |
|:---------------------------------------------------------------------------------------------------------------|:--------|
| [apache-airflow-backport-providers-http](https://github.com/apache/airflow/tree/master/airflow/providers/http) | http    |

# Provider class summary

All classes in Airflow 2.0 are in `airflow.providers.slack` package.


## Operators




### Moved operators

| Airflow 2.0 operators: `airflow.providers.slack` package                                                                                         | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                             |
|:-------------------------------------------------------------------------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.slack.SlackAPIOperator](https://github.com/apache/airflow/blob/master/airflow/providers/slack/operators/slack.py)                     | [operators.slack_operator.SlackAPIOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/operators/slack_operator.py)                                     |
| [operators.slack.SlackAPIPostOperator](https://github.com/apache/airflow/blob/master/airflow/providers/slack/operators/slack.py)                 | [operators.slack_operator.SlackAPIPostOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/operators/slack_operator.py)                                 |
| [operators.slack_webhook.SlackWebhookOperator](https://github.com/apache/airflow/blob/master/airflow/providers/slack/operators/slack_webhook.py) | [contrib.operators.slack_webhook_operator.SlackWebhookOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/slack_webhook_operator.py) |





## Hooks



### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.slack` package                                                                                 | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                         |
|:-------------------------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------|
| [hooks.slack.SlackHook](https://github.com/apache/airflow/blob/master/airflow/providers/slack/hooks/slack.py)                        | [hooks.slack_hook.SlackHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/hooks/slack_hook.py)                                        |
| [hooks.slack_webhook.SlackWebhookHook](https://github.com/apache/airflow/blob/master/airflow/providers/slack/hooks/slack_webhook.py) | [contrib.hooks.slack_webhook_hook.SlackWebhookHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/slack_webhook_hook.py) |






## Releases

### Release 2020.05.19

| Commit                                                                                         | Committed   | Subject                                                                       |
|:-----------------------------------------------------------------------------------------------|:------------|:------------------------------------------------------------------------------|
| [578fc514c](https://github.com/apache/airflow/commit/578fc514cd325b7d190bdcfb749a384d101238fa) | 2020-05-12  | [AIRFLOW-4543] Update slack operator to support slackclient v2 (#5519)        |
| [4bde99f13](https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a) | 2020-03-23  | Make airflow/providers pylint compatible (#7802)                              |
| [be2b2baa7](https://github.com/apache/airflow/commit/be2b2baa7c5f53c2d73646e4623cdb6731551b70) | 2020-03-23  | Add missing call to Super class in &#39;http&#39;, &#39;grpc&#39; &amp; &#39;slack&#39; providers (#7826) |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                      |
| [9a04013b0](https://github.com/apache/airflow/commit/9a04013b0e40b0d744ff4ac9f008491806d60df2) | 2020-01-27  | [AIRFLOW-6646][AIP-21] Move protocols classes to providers package (#7268)    |
| [c42a375e7](https://github.com/apache/airflow/commit/c42a375e799e5adb3f9536616372dc90ff47e6c8) | 2020-01-27  | [AIRFLOW-6644][AIP-21] Move service classes to providers package (#7265)      |

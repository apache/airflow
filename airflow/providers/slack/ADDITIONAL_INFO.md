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

# Migration Guide

## 3.0.0

Reference: https://slack.dev/python-slack-sdk/v3-migration/index.html#from-slackclient-2-x

### Use  ``AsyncWebClient`` for async options

With v3 upgrade, `WebClient` no longer has `run_async` and `aiohttp` specific options. If you still need the option or other `aiohttp` specific options, use `LegacyWebClient` (slackclient v2 compatible) or `AsyncWebClient`


### Include ``aiohttp`` in requirements

`aiohttp` is no longer automatically resolved with `slack_sdk`(earlier `slackclient`). So, if working with `AsyncWebClient`, `AsyncWebhookClient`, and `LegacyWebClient`, it needs separate installation.

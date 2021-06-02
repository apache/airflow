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

# Updating the Airflow Helm Chart

This file documents any backwards-incompatible changes in the Airflow Helm chart and
assists users migrating to a new version.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of contents**

- [Main](#main)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Main

<!--

I'm glad you want to write a new note. Remember that this note is intended for users.
Make sure it contains the following information:

- [ ] Previous behaviors
- [ ] New behaviors
- [ ] If possible, a simple example of how to migrate. This may include a simple code example.
- [ ] If possible, the benefit for the user after migration e.g. "we want to make these changes to unify class names."
- [ ] If possible, the reason for the change, which adds more context to that interested, e.g. reference for Airflow Improvement Proposal.

More tips can be found in the guide:
https://developers.google.com/style/inclusive-documentation

-->

### Removed `dags.gitSync.root`, `dags.gitSync.dest`, and `dags.gitSync.excludeWebserver` parameters

The `dags.gitSync.root` and `dags.gitSync.dest` parameters didn't provide any useful behaviors to chart users so they have been removed.
If you have them set in your values file you can safely remove them.

The `dags.gitSync.excludeWebserver` parameter was mistakenly included in the charts `values.schema.json`. If you have it set in your values file,
you can safely remove it.

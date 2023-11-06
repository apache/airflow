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

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [13. Get rid of submodules](#13-get-rid-of-submodules)
  - [Status](#status)
  - [Context](#context)
  - [Decision](#decision)
  - [Consequences](#consequences)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 13. Get rid of submodules

Date: 2023-11-04

## Status

Accepted

Builds on [5. Prevents using contributed code when building images](0005-preventing-using-contributed-code-when-building-images.md)

## Context

Airflow Build system has to be secure and follow Apache Software Foundation guidelines in order to prevent
Supply chain attacks, we want to avoid possibility where 3rd-party contributed code is executed during the
build system in order to inject code into the artifacts we release to our users.

While we have a number of safeguards in place (protected branches, releasing only source code, reviews etc.)
we are also using 3rd-party GitHub Actions in our build and we need to make sure unknown/potentially
malicious code is not injected by people who manage the actions without our knowledge.

Previously in [0005-preventing-using-contributed-code-when-building-images.md](0005-preventing-using-contributed-code-when-building-images.md)
we decided to use ``submodules`` to keep references to those actions as submodule links
the repository including a specific commit and when you review the code when adding submodule the code cannot
change without running "submodule update" and committing resulting change.

However submodules are rarely used in general, are very arcane knowledge and if you use them very rarely,
it is not well known or intuitive how to keep such repository updated. And over time it is also important
to have an easy way to keep such 3rd-party actions updated, because of security updates of their dependencies
and retention policies of GitHub. Some of our actions raised deprecation warnings about old Nodejs version
for example and the only way was to update the actions.

Updating the submodules then becomes a bit of "arcane chore" - which is quite a bit of contradiction. Chores
by definitions should be easy to do, otherwise they are not getting done regularly.

## Decision

The decision is to use the approach suggested by
[Github Actions Security Hardening](https://docs.github.com/en/actions/security-guides/security-hardening-for-github-actions#using-third-party-actions)
by simply pinning the potentially insecure GitHub Actions via full-length SHA of the commit

The point 2) about submodules should take the following form

2) For security reason for non-GitHub owned actions (which are heavily protected asset of Github Actions
   and compromising them would mean compromising the whole GitHub Actions infrastructure) we use full-length
   SHA of the action to refer to the action code. We do it after careful review of the action code - with the
   comment explaining version of the action the SHA refers to. When we upgrade the action we update the SHA
   and the version after careful review of the new action code - ideally comparing the code to the SHA we had
   in the past, to make sure that no new unwanted code has been added.

Example:

```
uses: aws-actions/configure-aws-credentials@010d0da01d0b5a38af31e9c3470dbfdabdecca3a  # v4.0.1
```

## Consequences

It should be easier to keep our actions updated, we are also going to get rid of the submodules from Airflow
repository and can remove any usage of submodules when checking out the code.

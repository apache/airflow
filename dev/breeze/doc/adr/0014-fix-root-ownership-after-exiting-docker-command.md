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

- [14. Fix root ownership after exiting docker command](#14-fix-root-ownership-after-exiting-docker-command)
  - [Status](#status)
  - [Context](#context)
  - [Decision](#decision)
  - [Consequences](#consequences)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 14. Fix root ownership after exiting docker command

Date: 2023-11-20

## Status

Accepted

Builds on [6. Using root user and fixing ownership for-ci-container](0006-using-root-user-and-fixing-ownership-for-ci-container.md)

## Context

As discussed in [6. Using root user and fixing ownership for-ci-container](0006-using-root-user-and-fixing-ownership-for-ci-container.md)
we run Breeze CI container as root user. We used to use TRAP to fix the ownership of files created in
the container by root user. However, this is not fool-proof and using TRAP has its caveats (for example
it can be overridden by another TRAP command). Also TRAP might be invalidated by signal handling
or abrupt killing the docker container. Running the cleanup command after docker command completed
seems to do be much more robust solution.

## Decision

Instead of running the command as TRAP when exiting we simply attempt to clean the ownership whenever
we exit the container using `breeze`, `breeze shell`, `tests` or `breeze start-airflow` commands that run
commands in the CI image.

## Consequences

Users running Breeze on Linux will have less problems with root owned files and we can also remove
dedicated `ci fix-ownership` command in CI.

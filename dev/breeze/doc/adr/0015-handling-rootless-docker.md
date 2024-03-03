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

- [15. Handling rootless docker](#15-handling-rootless-docker)
  - [Status](#status)
  - [Context](#context)
  - [Decision](#decision)
  - [Consequences](#consequences)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 15. Handling rootless docker

Date: 2023-11-29

## Status

Accepted

Builds on [6. Using root user and fixing ownership for-ci-container](0006-using-root-user-and-fixing-ownership-for-ci-container.md)
Builds on [14. Fix root ownership after exiting docker command](0014-fix-root-ownership-after-exiting-docker-command.md)

## Context

[Rootless docker](https://docs.docker.com/engine/security/rootless/) solutions are becoming more and
more popular. They are more secure to run and they allow to run docker containers without root privileges.
It is expected that in the near future, rootless docker will become the default way of running docker.

In case of rootless docker, the assumptions from both 6. and 14. ADRs are not valid. The user running
docker is re-mapped from the original user in the host that run the container. This means that the
ownership of files created in the container does not have to be fixed (the user ids will be re-mapped
from the container back to the host automatically) and that the dag folder owned by the user on the
host will be automatically owned by the mapped user inside the container.

This means that we do not need to neither fix the ownership nor change the ownership when the docker
is in rootless mode.

## Decision

When we enter breeze container we check if docker is running in rootless mode, and we
have a `DOCKER_IS_ROOTLESS` variable set to `true` when entering the container. This variable might
then be used to make decision on changing ownership of the files inside the container.

## Consequences

Users running Breeze on Linux will have less problems with root owned files and we can also remove
dedicated `ci fix-ownership` command in CI.

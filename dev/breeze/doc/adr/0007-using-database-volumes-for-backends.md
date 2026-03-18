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

- [7. Using database volumes for backends](#7-using-database-volumes-for-backends)
  - [Status](#status)
  - [Context](#context)
  - [Decision](#decision)
  - [Consequences](#consequences)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 7. Using database volumes for backends

Date: 2022-01-29

## Status

Accepted

## Context

Airflow breeze can use different backends as metadata db and those
backends are run as separate containers, and enabled via the
`--backend` switch when running Breeze. Those databases
store their database files locally in container normally, which means
that when the database container is stopped, the database is gone.

While this is convenient for development to start from scratch,
there is often a need to keep the data in the database across
DB container restarts. This can be achieved by having volumes.

On the other hand, we want to have possibility of wiping out
the environment totally - with removing all the database data.
This is especially useful when you switch branches and the
structure of the DB you locally have is newer than the
structure your code is aware of.

## Decision

Each Breeze's backend (including SQLite for consistency) stores
their database files in a volume created automatically when
appropriate `--backend` flag is used. This is done in
`backend-<BACKEND>.yml` docker-compose file.

This volume stays until it is manually deleted or until
`breeze down` command is called. The `breeze down` command
stops all the containers and removes all the volumes, in order
to provide an easy way to start breeze environment from
scratch.

## Consequences

Developers who need to restart their database will be able
to do it without losing the database data. However, there
is also an easy way to drop and recreate the databases
from the scratch.

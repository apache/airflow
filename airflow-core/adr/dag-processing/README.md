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

# Architectural Decision Records — Dag processing

These ADRs record the architecture decisions behind how the Dag processor discovers, schedules and
parses Dag sources ([AIP-85](https://cwiki.apache.org/confluence/x/_Q7OEg)): the importer extension
point, the process model around it, and the contracts an importer author has to hold.

They are kept separate from [`../lang-sdk/`](../lang-sdk), which records the cross-cutting decisions
behind running non-Python *tasks* ([AIP-108](https://cwiki.apache.org/confluence/x/pY4mGQ)). A
language SDK is one consumer of the importer interface, not its owner, so decisions that bind the
interface itself live here and decisions about a particular runtime live there.

- [ADR-0001](0001-dag-importer-process-model.md): Dag importer process model — who owns the parse
  process.

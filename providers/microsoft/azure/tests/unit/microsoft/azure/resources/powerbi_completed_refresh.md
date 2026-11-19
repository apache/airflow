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

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

`powerbi_completed_refresh.json` contains sanitized response bodies from a
user-supplied, post-completion observation on 2026-10-12. Both the refresh history
and execution-details requests returned HTTP 200. The matching enhanced refresh
has `status: Completed` and `extendedStatus: Completed` in both responses, with
no engine messages or service exceptions.

Workspace, request, history-entry, replica-version, and object identifiers are
replaced consistently. Statuses, refresh types, timestamps, attempts, and other
execution metrics are preserved. The dataset ID only appeared in the request
URLs, which are not included in the fixture.

This pair validates normal completion. It does not capture an active refresh,
a failed refresh, or history remaining `Unknown` after execution becomes terminal.
Those status-divergence and failure regressions use synthetic inputs.

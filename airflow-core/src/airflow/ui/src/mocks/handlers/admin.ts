/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { http, HttpResponse, type HttpHandler } from "msw";

// The Admin and Browse list pages, empty by default.
export const handlers: Array<HttpHandler> = [
  http.get("/api/v2/config", () => HttpResponse.json({ sections: [] })),
  http.get("/api/v2/connections", () => HttpResponse.json({ connections: [], total_entries: 0 })),
  http.get("/ui/connections/hook_meta", () => HttpResponse.json([])),
  http.get("/api/v2/dagBundles", () => HttpResponse.json({ dag_bundles: [], total_entries: 0 })),
  http.get("/api/v2/eventLogs", () => HttpResponse.json({ event_logs: [], total_entries: 0 })),
  http.get("/api/v2/jobs", () => HttpResponse.json({ jobs: [], total_entries: 0 })),
  http.get("/api/v2/pools", () => HttpResponse.json({ pools: [], total_entries: 0 })),
  http.get("/api/v2/providers", () => HttpResponse.json({ providers: [], total_entries: 0 })),
  http.get("/api/v2/variables", () => HttpResponse.json({ total_entries: 0, variables: [] })),
];

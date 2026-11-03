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

import type { MenuItem } from "openapi/requests/types.gen";

const ALL_MENU_ITEMS: Array<MenuItem> = [
  "Required Actions",
  "Assets",
  "Audit Log",
  "Config",
  "Connections",
  "Dags",
  "Dag Bundles",
  "Deadlines",
  "Docs",
  "Jobs",
  "Plugins",
  "Pools",
  "Providers",
  "Variables",
  "XComs",
];

// Requests every page shell makes (navigation, user menu, version, banners) plus the i18n bundles.
export const handlers: Array<HttpHandler> = [
  http.get("/ui/auth/me", () => HttpResponse.json({ id: "1", teams: null, username: "admin" })),
  http.get("/ui/auth/menus", () =>
    HttpResponse.json({ authorized_menu_items: ALL_MENU_ITEMS, extra_menu_items: [] }),
  ),
  http.get("/api/v2/version", () => HttpResponse.json({ git_version: null, version: "3.2.0" })),
  http.get("/api/v2/plugins", () => HttpResponse.json({ plugins: [], total_entries: 0 })),
  http.get("/api/v2/plugins/importErrors", () => HttpResponse.json({ import_errors: [], total_entries: 0 })),
  http.get("/api/v2/importErrors", () => HttpResponse.json({ import_errors: [], total_entries: 0 })),
  http.get("/api/v2/dagWarnings", () => HttpResponse.json({ dag_warnings: [], total_entries: 0 })),
  // Matched under any base path, since src/i18n/config prefixes these with the app's `<base href>`.
  http.get("*/static/i18n/languages.json", () => HttpResponse.json({ languages: [] })),
  // Tests render translation keys rather than English text: serve empty bundles so i18next settles
  // on the keys. A test that needs real strings adds the bundle itself with `addResourceBundle`.
  http.get("*/static/i18n/locales/:lng/:ns", () => HttpResponse.json({})),
];

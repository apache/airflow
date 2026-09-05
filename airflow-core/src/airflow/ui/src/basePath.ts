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
import { OpenAPI } from "openapi/requests/core/OpenAPI";

// Airflow can be served under a path prefix (`[api] base_url`), which the server renders
// into `<base href>`.
const baseHref = document.querySelector("head > base")?.getAttribute("href") ?? "";

export const basePath = new URL(baseHref, globalThis.location.origin).pathname.replace(/\/$/u, "");

// The generated client is configured here, in a module with no app dependencies, rather
// than next to the query client. Anything that issues a request while the app is still
// initializing has to observe the configured base -- i18n requests the version at module
// scope to build a cache buster -- and importing that request's module first would
// otherwise send it to the origin root instead of the prefix. Importing anything derived
// from the base href now orders this assignment ahead of the request.
OpenAPI.BASE = baseHref.endsWith("/") ? baseHref.slice(0, -1) : baseHref;

// Encode path params as full URI components so values containing "/" (e.g. a variable key
// like "/foo") become "%2Ffoo" rather than a literal "//", which proxies may collapse. The
// generated client otherwise defaults to encodeURI, which leaves "/" untouched.
// The backend automatically decodes path params.
OpenAPI.ENCODE_PATH = encodeURIComponent;

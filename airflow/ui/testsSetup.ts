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
import * as matchers from "@testing-library/jest-dom/matchers";
import "@testing-library/jest-dom/vitest";
import type { HttpHandler } from "msw";
import { setupServer, type SetupServerApi } from "msw/node";
import { beforeEach, expect, beforeAll, afterAll } from "vitest";

import { handlers } from "src/mocks/handlers";

let server: SetupServerApi;

// extends vitest matchers with react-testing-library's ones
expect.extend(matchers);

beforeAll(() => {
  server = setupServer(...(handlers as Array<HttpHandler>));
  server.listen({ onUnhandledRequest: "bypass" });
});

beforeEach(() => server.resetHandlers());
afterAll(() => server.close());

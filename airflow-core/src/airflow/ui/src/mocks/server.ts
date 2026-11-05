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
import { setupServer, type SetupServer } from "msw/node";

import { handlers } from "src/mocks/handlers";

// The one msw server of each test file, started by testsSetup.ts. Tests add their own handlers with
// `server.use(...)` instead of starting another server: concurrent servers share msw's request
// interception, so the first one consulted shadows the other's handlers, and closing either one
// lets the requests still in flight escape to the network.
export const server: SetupServer = setupServer(...handlers);

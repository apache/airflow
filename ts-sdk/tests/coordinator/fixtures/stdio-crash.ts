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

// Serves a bundle, then fails with an uncaught error after filling stdout and stderr past any
// socket buffer.
import { Bundle, Dag } from "../../../src/index.js";

const dag = new Dag("stdio_crash");
dag.task("t", async () => undefined)();
setTimeout(() => {
  process.stdout.write("o".repeat(1 << 20) + "\n");
  process.stderr.write("e".repeat(1 << 20) + "\n");
  throw new Error("final failure");
}, 100);
await new Bundle(dag).serve();

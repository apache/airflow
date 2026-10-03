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

import assert from "node:assert/strict";
import { after, test } from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import { createServer } from "vite";

const server = await createServer({ server: { middlewareMode: true } });
after(() => server.close());
const { default: Plugin } = await server.ssrLoadModule("src/main.tsx");

const taskProps = { dagId: "dag", runId: "run", taskId: "review", mapIndex: "-1" };

test("an unresolved modern host selection cannot open another task's session", () => {
  const html = renderToStaticMarkup(createElement(Plugin, { ...taskProps, taskInstance: undefined }));

  assert.match(html, /No Active HITL Review Session/);
});

test("an older host without task instance context still opens its review", () => {
  const html = renderToStaticMarkup(createElement(Plugin, taskProps));

  assert.doesNotMatch(html, /No Active HITL Review Session/);
});

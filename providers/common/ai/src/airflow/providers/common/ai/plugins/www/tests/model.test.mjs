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
import { after, afterEach, mock, test } from "node:test";
import { act, createElement } from "react";
import { createRoot } from "react-dom/client";
import { renderToStaticMarkup } from "react-dom/server";

import { Window } from "happy-dom";
import { createServer } from "vite";

const server = await createServer({ server: { middlewareMode: true, ws: false } });
after(() => server.close());
const { default: Plugin } = await server.ssrLoadModule("src/model.tsx");

const taskProps = { dagId: "dag", runId: "run", taskId: "refine.agent", mapIndex: "-1" };
const regionId = "00000000-0000-0000-0000-000000000123";

afterEach(() => mock.restoreAll());

/** Mount the plugin into a happy-dom document, as the host does, and return its controls. */
function mountPlugin(t) {
  const window = new Window({ url: "http://localhost/" });
  globalThis.window = window;
  globalThis.document = window.document;
  globalThis.IS_REACT_ACT_ENVIRONMENT = true;
  const root = createRoot(document.body.appendChild(document.createElement("div")));
  t.after(async () => {
    await act(async () => root.unmount());
    await window.happyDOM.close();
    delete globalThis.window;
    delete globalThis.document;
    delete globalThis.IS_REACT_ACT_ENVIRONMENT;
  });
  return {
    render: (props) => act(async () => root.render(createElement(Plugin, props))),
    text: () => document.body.textContent,
  };
}

/** Answer each XCom read with the model name for its loop pass, after `delay(url)` resolves. */
function serveXComs(delay = () => Promise.resolve()) {
  const calls = [];
  mock.method(globalThis, "fetch", async (input) => {
    const url = new URL(input, "http://localhost");
    calls.push(url);
    await delay(url);
    const key = url.pathname.split("/").at(-1);
    const value = key === "usage" ? { requests: 1 } : `model-for-pass-${url.searchParams.get("region_index")}`;
    return new Response(JSON.stringify({ value }), { status: 200 });
  });
  return calls;
}

test("an unresolved modern host selection waits instead of reading without a region", () => {
  const html = renderToStaticMarkup(createElement(Plugin, { ...taskProps, taskInstance: undefined }));

  assert.match(html, /Loading model info/);
});

test("an older host without task instance context reads without a region", async (t) => {
  const calls = serveXComs();
  const plugin = mountPlugin(t);

  await plugin.render(taskProps);

  assert.equal(calls.length, 2);
  for (const url of calls) assert.equal(url.searchParams.has("region_id"), false);
});

test("the selected loop pass reaches both XCom reads and a pass switch rereads", async (t) => {
  const calls = serveXComs();
  const plugin = mountPlugin(t);

  await plugin.render({ ...taskProps, taskInstance: { region_id: regionId, region_index: 2 } });
  await plugin.render({ ...taskProps, taskInstance: { region_id: regionId, region_index: 3 } });

  assert.deepEqual(
    calls.map((url) => [url.pathname.split("/").at(-1), url.searchParams.get("region_id"), url.searchParams.get("region_index")]),
    [
      ["__AIRFLOW__COMMON_AI_MODEL_NAME__", regionId, "2"],
      ["usage", regionId, "2"],
      ["__AIRFLOW__COMMON_AI_MODEL_NAME__", regionId, "3"],
      ["usage", regionId, "3"],
    ],
  );
  assert.match(plugin.text(), /model-for-pass-3/);
});

test("a slow read for the previous loop pass does not replace the current pass", async (t) => {
  let releasePass2;
  const pass2Held = new Promise((resolve) => {
    releasePass2 = resolve;
  });
  serveXComs((url) => (url.searchParams.get("region_index") === "2" ? pass2Held : Promise.resolve()));
  const plugin = mountPlugin(t);

  await plugin.render({ ...taskProps, taskInstance: { region_id: regionId, region_index: 2 } });
  await plugin.render({ ...taskProps, taskInstance: { region_id: regionId, region_index: 3 } });
  await act(async () => releasePass2());

  assert.match(plugin.text(), /model-for-pass-3/);
  assert.doesNotMatch(plugin.text(), /model-for-pass-2/);
});

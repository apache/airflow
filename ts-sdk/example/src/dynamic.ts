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

// Native Dags generated from the `dags.yaml` next to the bundle, one per entry.
//
// Both the Dag processor and the worker run the bundle, and each run reads the
// YAML again, so swapping only the YAML changes the Dags with no rebuild.
// Without a `dags.yaml` next to the bundle, as when `airflow-ts-pack` runs it to
// read its metadata, there are no Dags, and the bundle's other Dags still parse.

import { existsSync, readFileSync } from "node:fs";

import { Dag } from "apache-airflow-ts-sdk";
import { load } from "js-yaml";

interface DagsFile {
  readonly dags?: readonly { readonly dag_id: string; readonly greeting: string }[];
}

export function loadDynamicDags(): Dag[] {
  const path = new URL("dags.yaml", import.meta.url);
  if (!existsSync(path)) {
    return [];
  }
  const file = load(readFileSync(path, "utf-8")) as DagsFile;
  return (file.dags ?? []).map(({ dag_id, greeting }) => {
    const dag = new Dag(dag_id, {
      catchup: false,
      tags: ["typescript", "typescript_dynamic"],
      queue: "typescript",
    });
    dag.task("greet", async () => greeting)();
    return dag;
  });
}

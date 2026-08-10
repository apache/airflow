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

import { registerTask, startCoordinator, type TaskHandlerArgs } from "@apache-airflow/ts-sdk";

const DAG_ID = "typescript_example";

export async function buildMessage({ client }: TaskHandlerArgs) {
  const upstream = await client.getXCom<string>({
    key: "return_value",
    taskId: "python_start",
  });
  const greeting = await client.getVariable("typescript_example_greeting");
  const message = `${greeting ?? "hello from TypeScript"}; upstream=${upstream ?? "missing"}`;

  await client.setXCom({ key: "typescript_message", value: message });

  return {
    message,
    upstream,
  };
}

export async function readConnection({ client }: TaskHandlerArgs) {
  const connection = await client.getConnection("typescript_example_http");

  return {
    id: connection?.id ?? null,
    type: connection?.type ?? null,
    host: connection?.host ?? null,
    login: connection?.login ?? null,
    hasPassword: connection?.password != null,
  };
}

registerTask({ dagId: DAG_ID, taskId: "build_message" }, buildMessage);
registerTask({ dagId: DAG_ID, taskId: "read_connection" }, readConnection);

await startCoordinator();

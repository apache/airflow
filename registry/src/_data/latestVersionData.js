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

const path = require("path");
const providersData = require("./providers.json");
const { tryReadJson } = require("./utils");

module.exports = function () {
  const result = {};
  for (const provider of providersData.providers) {
    const vDir = path.join(__dirname, "versions", provider.id, provider.version);
    result[provider.id] = {
      parameters: tryReadJson(path.join(vDir, "parameters.json")),
      connections: tryReadJson(path.join(vDir, "connections.json")),
    };
  }
  return result;
};

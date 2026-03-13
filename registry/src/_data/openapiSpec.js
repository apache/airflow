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

const fs = require("fs");
const path = require("path");

const OPENAPI_SCHEMA_PATH = path.join(__dirname, "..", "..", "schemas", "openapi.json");

module.exports = function() {
  if (!fs.existsSync(OPENAPI_SCHEMA_PATH)) {
    throw new Error(
      `Missing OpenAPI schema artifact at ${OPENAPI_SCHEMA_PATH}. ` +
      "Run `pnpm build` (prebuild generates it) or `uv run python ../dev/registry/export_registry_schemas.py`.",
    );
  }
  return JSON.parse(fs.readFileSync(OPENAPI_SCHEMA_PATH, "utf8"));
};

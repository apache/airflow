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

// Shared by the pagefind index builder and the `externalServices` Eleventy
// filter (used for the /providers/ filter box), so the two search paths read
// the same field off the same shape and can't drift apart.

// The services a provider's connection types reach -- for Common AI these are
// the LLM providers behind each `pydanticai` / `langchain` connection, which are
// the names people actually search for ("openai", "ollama") even though no
// provider is called that.
function collectExternalServices(provider) {
  const seen = new Set();
  const services = [];
  for (const connection of provider.connection_types || []) {
    for (const service of connection.external_services || []) {
      const key = service.toLowerCase();
      if (!seen.has(key)) {
        seen.add(key);
        services.push(service);
      }
    }
  }
  return services;
}

module.exports = { collectExternalServices };

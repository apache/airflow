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

// Groups every module that is durable or deferrable, across all providers,
// for the /durable/ page: the same union the provider page's
// "Durable (includes deferrable)" toggle shows.
const providersData = require("./providers.json");
const modulesData = require("./modules.json");

module.exports = function () {
  const modules = modulesData.modules.filter(
    (m) => m.supports_durable_execution || m.supports_deferrable,
  );

  const groupsById = new Map();
  for (const provider of providersData.providers) {
    groupsById.set(provider.id, {
      id: provider.id,
      name: provider.name,
      version: provider.version,
      logo: provider.logo,
      modules: [],
    });
  }
  for (const m of modules) {
    const group = groupsById.get(m.provider_id);
    if (!group) throw new Error(`Module ${m.id} references unknown provider ${m.provider_id}`);
    group.modules.push(m);
  }

  // Durable modules lead each group: there are far fewer of them, and they are
  // what someone looking for "durable operators" usually means.
  const groups = Array.from(groupsById.values())
    .filter((g) => g.modules.length > 0)
    .sort((a, b) => a.name.localeCompare(b.name));
  for (const g of groups) {
    g.modules.sort(
      (a, b) =>
        Number(Boolean(b.supports_durable_execution)) - Number(Boolean(a.supports_durable_execution)) ||
        a.name.localeCompare(b.name),
    );
  }

  const typeCounts = {};
  for (const m of modules) typeCounts[m.type] = (typeCounts[m.type] || 0) + 1;

  return {
    groups,
    typeCounts,
    counts: {
      all: modules.length,
      durable: modules.filter((m) => m.supports_durable_execution).length,
      deferrable: modules.filter((m) => m.supports_deferrable).length,
    },
  };
};

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

const providersData = require("./providers.json");
const buildProviderVersions = require("./providerVersions");

module.exports = function () {
  const providerVersions = buildProviderVersions();
  const payloads = {};

  for (const provider of providersData.providers) {
    payloads[provider.id] = {
      latest: provider.version,
      versions: [provider.version],
    };
  }

  for (const pv of providerVersions) {
    if (!pv || !pv.provider || !pv.provider.id || !pv.isLatest) {
      continue;
    }
    const versions = Array.isArray(pv.availableVersions) && pv.availableVersions.length > 0
      ? pv.availableVersions
      : [pv.provider.version];
    payloads[pv.provider.id] = {
      latest: pv.provider.version,
      versions,
    };
  }

  return payloads;
};

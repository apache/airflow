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
const providersData = require("./providers.json");
const modulesData = require("./modules.json");
const { tryReadJson } = require("./utils");

function parseMinAirflow(dependencies) {
  if (!dependencies) return null;
  for (const dep of dependencies) {
    if (dep.startsWith("apache-airflow>=")) {
      const ver = dep.split(">=")[1].split(",")[0].trim();
      const parts = ver.split(".");
      if (parts.length >= 2) return parts[0] + "." + parts[1] + "+";
    }
  }
  return null;
}

function getAvailableVersions(provider) {
  const versionsDir = path.join(__dirname, "versions", provider.id);
  const builtOlderVersions = fs.existsSync(versionsDir)
    ? fs.readdirSync(versionsDir).filter((entry) =>
        fs.existsSync(path.join(versionsDir, entry, "metadata.json")),
      )
    : [];

  const availableSet = new Set([provider.version, ...builtOlderVersions]);

  if (Array.isArray(provider.versions) && provider.versions.length > 0) {
    return provider.versions.filter((version) => availableSet.has(version));
  }

  return [provider.version, ...builtOlderVersions];
}

module.exports = function () {
  const result = [];

  // Index modules by provider_id for O(1) lookup
  const modulesByProvider = {};
  for (const m of modulesData.modules) {
    if (!modulesByProvider[m.provider_id]) modulesByProvider[m.provider_id] = [];
    modulesByProvider[m.provider_id].push(m);
  }

  for (const provider of providersData.providers) {
    const availableVersions = getAvailableVersions(provider);
    const latestModules = modulesByProvider[provider.id] || [];
    const latestDir = path.join(__dirname, "versions", provider.id, provider.version);

    // Latest version entry -- data comes from providers.json + modules.json,
    // with optional parameters/connections from versions/{id}/{version}/
    const latestAirflow = provider.airflow_versions && provider.airflow_versions.length > 0
      ? provider.airflow_versions[provider.airflow_versions.length - 1]
      : null;

    result.push({
      provider,
      version: provider.version,
      isLatest: true,
      versionData: null,
      modules: latestModules,
      minAirflowVersion: latestAirflow,
      parameters: tryReadJson(path.join(latestDir, "parameters.json")),
      connections: tryReadJson(path.join(latestDir, "connections.json")),
      // Only versions with actual built data are included in static HTML.
      // In CI this is usually just the latest version, which avoids linking
      // to pages that were never generated. For local preview/backfills,
      // extract_versions.py adds older versions under _data/versions/, and
      // those are safe to include here.
      availableVersions,
    });

    // Older versions from _data/versions/{id}/{version}/metadata.json
    // These are produced by extract_versions.py for backfill or targeted builds.
    const versionsDir = path.join(__dirname, "versions", provider.id);
    if (fs.existsSync(versionsDir)) {
      for (const entry of availableVersions) {
        if (entry === provider.version) continue; // skip latest, already added
        const metadata = tryReadJson(path.join(versionsDir, entry, "metadata.json"));
        if (!metadata) continue;

        result.push({
          provider,
          version: entry,
          isLatest: false,
          versionData: metadata,
          modules: metadata.modules || [],
          minAirflowVersion: parseMinAirflow(metadata.dependencies),
          parameters: tryReadJson(path.join(versionsDir, entry, "parameters.json")),
          connections: tryReadJson(path.join(versionsDir, entry, "connections.json")),
          availableVersions,
        });
      }
    }
  }

  return result;
};

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
const exploreCategories = require("./exploreCategories");
const { providerMatchesKeyword } = require("./providerKeywordMatch");

module.exports = function () {
  const map = {};
  for (const category of exploreCategories) {
    const matched = [];
    for (const provider of providersData.providers) {
      for (const keyword of category.keywords) {
        if (providerMatchesKeyword(provider, keyword)) {
          matched.push(provider);
          break;
        }
      }
    }
    // explore.njk shows only the first six as badges, so rank before slicing
    // (as its Top/Incubating rows already do) — otherwise the row is whatever
    // providers.json happened to list first, and widening a category's
    // membership silently pushes the well-known names off it.
    matched.sort(
      (a, b) => (b.pypi_downloads?.monthly || 0) - (a.pypi_downloads?.monthly || 0),
    );
    map[category.id] = matched;
  }
  return map;
};

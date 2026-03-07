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

const MAX_PROVIDERS_PER_CATEGORY = 6;

module.exports = function () {
  const map = {};
  for (const category of exploreCategories) {
    const matched = [];
    for (const provider of providersData.providers) {
      if (matched.length >= MAX_PROVIDERS_PER_CATEGORY) break;
      for (const keyword of category.keywords) {
        if (
          provider.id.includes(keyword) ||
          keyword.includes(provider.id)
        ) {
          matched.push(provider);
          break;
        }
      }
    }
    map[category.id] = matched;
  }
  return map;
};

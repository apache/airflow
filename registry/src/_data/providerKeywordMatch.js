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

// Shared by providerCategoryMap.js and exploreCategoryProviders.js so the
// two directions of the same provider <-> explore-category matching (used
// respectively by the category dropdown on /providers/ and the Explore landing
// page's per-category provider listing) can't drift apart.

// A value "matches" a keyword if the value contains the keyword,
// case-insensitively, after collapsing runs of "-_\s" to a single space
// (so 'pydantic-ai' matches "Pydantic AI"). Collapse, don't strip:
// stripping would turn "Microsoft Power BI" into "microsoftpowerbi",
// which contains "ftp" and would falsely match the orchestration category.
function normalize(text) {
  return text.toLowerCase().replace(/[-_\s]+/g, ' ');
}

function fuzzyIncludes(value, keyword) {
  if (!value) {
    return false;
  }
  return normalize(value).includes(normalize(keyword));
}

// Every string a provider is searchable by: its id/slug and its declared
// integration names (provider.categories[].name — e.g. "LangChain",
// "Pydantic AI").
function collectSearchableValues(provider) {
  const values = [provider.id];
  for (const category of provider.categories || []) {
    if (category.name) {
      values.push(category.name);
    }
  }
  return values;
}

function providerMatchesKeyword(provider, keyword) {
  return collectSearchableValues(provider).some((value) => fuzzyIncludes(value, keyword));
}

module.exports = { providerMatchesKeyword };

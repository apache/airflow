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
// respectively by the per-provider page's category chips and the Explore
// landing page's per-category provider listing) can't drift apart.

// Two arbitrary strings "match" a keyword if either contains the other
// (case-insensitively), mirroring the original id-only substring check.
function fuzzyIncludes(value, keyword) {
  if (!value) {
    return false;
  }
  const normalizedValue = value.toLowerCase();
  const normalizedKeyword = keyword.toLowerCase();
  return normalizedValue.includes(normalizedKeyword) || normalizedKeyword.includes(normalizedValue);
}

// Every string a provider is searchable by: its id/slug, its declared
// integration names (provider.categories[].name — e.g. "LangChain",
// "Pydantic AI"), and, once populated, each connection type's declared
// external integrations. The latter field doesn't exist in the generated
// data yet, so this reads as an empty list until a future change in
// dev/registry/extract_metadata.py starts populating it — no further
// changes needed here when that lands.
function collectSearchableValues(provider) {
  const values = [provider.id];
  for (const category of provider.categories || []) {
    if (category.name) {
      values.push(category.name);
    }
  }
  for (const connectionType of provider.connection_types || []) {
    for (const externalIntegration of connectionType.external_integrations || []) {
      values.push(externalIntegration);
    }
  }
  return values;
}

function providerMatchesKeyword(provider, keyword) {
  return collectSearchableValues(provider).some((value) => fuzzyIncludes(value, keyword));
}

module.exports = { providerMatchesKeyword };

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

// Shared by the pagefind index builder, the `externalServices` Eleventy
// filter (used for the /providers/ filter box), and providerKeywordMatch.js
// (used by the `ai-ml` explore category's opt-in keyword matching), so all
// three search paths read the same field off the same shape and can't drift
// apart.

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

// Generic tech/AI words that don't distinguish one vendor's name from another
// ("Mistral AI" vs "Mistral"); stripped before collision-checking a service
// name against another provider's own category name.
const SEARCH_TEXT_STOPWORDS = new Set(['ai', 'ml', 'llm', 'gpt', 'api', 'sdk']);
const SEARCH_TEXT_MIN_TOKEN_LENGTH = 3;

// Small, curated cloud-vendor abbreviations, not general synonym resolution --
// only added when load-bearing for an actual collision (see providerExternalServices
// design notes in the collision-detection PR).
const SEARCH_TEXT_VENDOR_ALIASES = { aws: 'amazon', gcp: 'google' };

function tokenizeForCollisionCheck(name) {
  const tokens = new Set();
  for (let token of name.toLowerCase().split(/\s+/)) {
    token = SEARCH_TEXT_VENDOR_ALIASES[token] || token;
    if (!SEARCH_TEXT_STOPWORDS.has(token) && token.length >= SEARCH_TEXT_MIN_TOKEN_LENGTH) {
      tokens.add(token);
    }
  }
  return tokens;
}

// A service name collides with another provider's own identity when every
// one of its (stopword-stripped, alias-normalised) tokens appears together in
// a single category string of some *other* provider -- e.g. "AWS Bedrock"
// collides with Amazon's own "Amazon Bedrock" category. Pagefind normalises
// scores by document length, so a bare mention of a colliding name in Common
// AI's much shorter record can outrank the provider that actually implements
// it; single-token bare vendor names (e.g. "OpenAI", "Anthropic") are never
// eligible for collision, since kaxil's own numbers show those queries are
// unaffected and the provider they'd "collide" with is themselves.
function findSearchTextCollision(serviceName, provider, allProviders) {
  const serviceTokens = tokenizeForCollisionCheck(serviceName);
  if (serviceTokens.size < 2) {
    return false;
  }
  for (const otherProvider of allProviders) {
    if (otherProvider.id === provider.id) {
      continue;
    }
    for (const category of otherProvider.categories || []) {
      const categoryTokens = tokenizeForCollisionCheck(category.name || '');
      let allPresent = true;
      for (const token of serviceTokens) {
        if (!categoryTokens.has(token)) {
          allPresent = false;
          break;
        }
      }
      if (allPresent) {
        return true;
      }
    }
  }
  return false;
}

// Excludes service names from search-indexed text when they collide with
// another provider's own category (see findSearchTextCollision); the caller
// still gets the full, unfiltered list for the badge and the /providers/
// filter (collectExternalServices() itself is untouched).
// A collision can only be seen against the providers in `allProviders`, so a
// partial dataset filters less than a full one; the index is built after the
// merge step, where the set is complete.
function filterServicesForSearchText(services, provider, allProviders) {
  return services.filter((service) => !findSearchTextCollision(service, provider, allProviders));
}

module.exports = { collectExternalServices, filterServicesForSearchText };

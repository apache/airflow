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

// Builds { provider_id: { type: count } } from modules.json.
// Used by provider-card.njk for module count badges on the listing page.
const modulesData = require("./modules.json");

module.exports = function () {
  const counts = {};
  for (const m of modulesData.modules) {
    if (!counts[m.provider_id]) counts[m.provider_id] = {};
    counts[m.provider_id][m.type] = (counts[m.provider_id][m.type] || 0) + 1;
  }
  return counts;
};

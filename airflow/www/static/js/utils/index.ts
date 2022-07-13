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

// Delay in ms for various hover actions
const hoverDelay = 200;

function getMetaValue(name: string) {
  const elem = document.querySelector(`meta[name="${name}"]`);
  if (!elem) {
    return null;
  }
  return elem.getAttribute('content');
}

const finalStatesMap = () => new Map([
  ['success', 0],
  ['failed', 0],
  ['upstream_failed', 0],
  ['up_for_retry', 0],
  ['up_for_reschedule', 0],
  ['running', 0],
  ['deferred', 0],
  ['sensing', 0],
  ['queued', 0],
  ['scheduled', 0],
  ['skipped', 0],
  ['no_status', 0],
]);

const appendSearchParams = (url: string | null, params: URLSearchParams | string) => {
  if (!url) return '';
  const separator = url.includes('?') ? '&' : '?';
  return `${url}${separator}${params}`;
};

export {
  hoverDelay,
  finalStatesMap,
  getMetaValue,
  appendSearchParams,
};

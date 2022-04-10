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

import camelcaseKeys from 'camelcase-keys';

export const areActiveRuns = (runs = []) => runs.filter((run) => ['queued', 'running', 'scheduled'].includes(run.state)).length > 0;

export const formatData = (data, emptyData) => {
  if (!data || !Object.keys(data).length) {
    return emptyData;
  }
  let formattedData = data;
  // Convert to json if needed
  if (typeof data === 'string') formattedData = JSON.parse(data);
  // change from pascal to camelcase
  formattedData = camelcaseKeys(formattedData, { deep: true });
  return formattedData;
};

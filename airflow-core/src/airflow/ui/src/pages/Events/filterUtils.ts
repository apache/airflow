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
export type FilterOptions = {
  after: string | null;
  before: string | null;
  dagId: string | null;
  eventType: string | null;
  mapIndex: string | null;
  runId: string | null;
  taskId: string | null;
  tryNumber: string | null;
  user: string | null;
};

export const getFilterCount = ({
  after,
  before,
  dagId,
  eventType,
  mapIndex,
  runId,
  taskId,
  tryNumber,
  user,
}: FilterOptions) => {
  let count = 0;

  if (after !== null) {
    count += 1;
  }
  if (before !== null) {
    count += 1;
  }
  if (dagId !== null) {
    count += 1;
  }
  if (eventType !== null) {
    count += 1;
  }
  if (mapIndex !== null) {
    count += 1;
  }
  if (runId !== null) {
    count += 1;
  }
  if (taskId !== null) {
    count += 1;
  }
  if (tryNumber !== null) {
    count += 1;
  }
  if (user !== null) {
    count += 1;
  }

  return count;
};

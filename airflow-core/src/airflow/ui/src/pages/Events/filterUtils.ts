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
  eventType: ReadonlyArray<string>;
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

  if (Boolean(after)) {
    count += 1;
  }
  if (Boolean(before)) {
    count += 1;
  }
  if (Boolean(dagId)) {
    count += 1;
  }
  if (eventType.length > 0) {
    count += 1;
  }

  if (Boolean(mapIndex)) {
    count += 1;
  }
  if (Boolean(runId)) {
    count += 1;
  }
  if (Boolean(taskId)) {
    count += 1;
  }
  if (Boolean(tryNumber)) {
    count += 1;
  }
  if (Boolean(user)) {
    count += 1;
  }

  return count;
};

export const formatDateTimeLocalValue = (isoString: string | null) => {
  if (isoString === null || isoString === "") {
    return "";
  }

  try {
    // Parse the ISO string and convert to local datetime-local format
    const date = new Date(isoString);
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, "0");
    const day = String(date.getDate()).padStart(2, "0");
    const hours = String(date.getHours()).padStart(2, "0");
    const minutes = String(date.getMinutes()).padStart(2, "0");

    return `${year}-${month}-${day}T${hours}:${minutes}`;
  } catch {
    return "";
  }
};

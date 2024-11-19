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
import { useState } from "react";

const useToggleGroups = ({ dagId }: { dagId: string }) => {
  const openGroupsKey = `${dagId}/open-groups`;
  let storedGroups: Array<string> = [];

  try {
    const storageGroups = localStorage.getItem(openGroupsKey) ?? "[]";
    const parsed = JSON.parse(storageGroups) as unknown;

    if (Array.isArray(parsed) && typeof parsed[0] === "string") {
      storedGroups = parsed as Array<string>;
    }
    // eslint-disable-next-line no-empty
  } catch {}

  const [openGroupIds, setOpenGroupIds] = useState(storedGroups);

  const onToggleGroups = (groupIds: Array<string>) => {
    localStorage.setItem(openGroupsKey, JSON.stringify(groupIds));
    setOpenGroupIds(groupIds);
  };

  return {
    onToggleGroups,
    openGroupIds,
  };
};

export default useToggleGroups;

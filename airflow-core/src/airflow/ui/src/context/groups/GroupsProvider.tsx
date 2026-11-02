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
import { useEffect, useRef, useState, type PropsWithChildren } from "react";

import { useDebouncedCallback } from "use-debounce";
import { useLocalStorage } from "usehooks-ts";

import { useStructureServiceStructureData } from "openapi/queries";

import { flattenGraphNodes } from "src/layouts/Details/Grid/utils";

import { allGroupsKey, openGroupsKey } from "src/constants/localStorage";
import useSelectedVersion from "src/hooks/useSelectedVersion";
import { useDefaultTaskGroupsExpanded } from "src/hooks/useUserSettings";

import { GroupsContext, type GroupsContextType } from "./Context";

type Props = {
  readonly dagId: string;
} & PropsWithChildren;

export const GroupsProvider = ({ children, dagId }: Props) => {
  const [storedOpenGroupIds, setStoredOpenGroupIds] = useLocalStorage<Array<string> | null>(
    openGroupsKey(dagId),
    null,
  );
  const [sessionOpenGroupIds, setSessionOpenGroupIds] = useState<{
    dagId: string;
    groupIds: Array<string>;
  }>();
  const [defaultTaskGroupsExpanded] = useDefaultTaskGroupsExpanded();
  const [allGroupIds, setAllGroupIds] = useLocalStorage<Array<string>>(allGroupsKey(dagId), []);

  const allGroupIdsRef = useRef(allGroupIds);

  useEffect(() => {
    allGroupIdsRef.current = allGroupIds;
  }, [allGroupIds]);

  const selectedVersion = useSelectedVersion();

  const { data: structure = { edges: [], nodes: [] } } = useStructureServiceStructureData(
    {
      dagId,
      versionNumber: selectedVersion,
    },
    undefined,
    { enabled: Boolean(dagId) && selectedVersion !== undefined },
  );

  const { allGroupIds: observedGroupIds, allOperators } = flattenGraphNodes(structure.nodes);
  const savedOpenGroupIds =
    sessionOpenGroupIds?.dagId === dagId ? sessionOpenGroupIds.groupIds : storedOpenGroupIds;
  const openGroupIds = savedOpenGroupIds ?? (defaultTaskGroupsExpanded ? observedGroupIds : []);

  // Keep the selection in memory too, so groups still toggle when localStorage is full.
  const setOpenGroupIds = (groupIds: Array<string>) => {
    setSessionOpenGroupIds({ dagId, groupIds });
    setStoredOpenGroupIds(groupIds);
  };

  useEffect(() => {
    if (JSON.stringify(observedGroupIds) !== JSON.stringify(allGroupIdsRef.current)) {
      setAllGroupIds(observedGroupIds);
    }
  }, [observedGroupIds, setAllGroupIds]);

  const debouncedSetOpenGroupIds = useDebouncedCallback((newGroupIds: Array<string>) => {
    setOpenGroupIds(newGroupIds);
  }, 100);

  const toggleGroupId = (groupId: string) => {
    if (openGroupIds.includes(groupId)) {
      debouncedSetOpenGroupIds(openGroupIds.filter((id) => id !== groupId));
    } else {
      debouncedSetOpenGroupIds([...openGroupIds, groupId]);
    }
  };

  const setOpenGroupIdsImmediate = (newGroupIds: Array<string>) => {
    debouncedSetOpenGroupIds.cancel();
    setOpenGroupIds(newGroupIds);
  };

  const value: GroupsContextType = {
    allGroupIds,
    allOperators,
    openGroupIds,
    setAllGroupIds,
    setOpenGroupIds: setOpenGroupIdsImmediate,
    toggleGroupId,
  };

  return <GroupsContext.Provider value={value}>{children}</GroupsContext.Provider>;
};

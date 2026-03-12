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
import { useEffect, useRef, type PropsWithChildren } from "react";
import { useDebouncedCallback } from "use-debounce";
import { useLocalStorage } from "usehooks-ts";

import { useStructureServiceStructureData } from "openapi/queries";
import { allGroupsKey, dependenciesKey, openGroupsKey } from "src/constants/localStorage";
import useSelectedVersion from "src/hooks/useSelectedVersion";
import { flattenGraphNodes } from "src/layouts/Details/Grid/utils";

import { OpenGroupsContext, type OpenGroupsContextType } from "./Context";

type Props = {
  readonly dagId: string;
} & PropsWithChildren;

export const OpenGroupsProvider = ({ children, dagId }: Props) => {
  const [openGroupIds, setOpenGroupIds] = useLocalStorage<Array<string>>(openGroupsKey(dagId), []);
  const [allGroupIds, setAllGroupIds] = useLocalStorage<Array<string>>(allGroupsKey(dagId), []);

  // use a ref to track the current allGroupIds without causing re-renders
  const allGroupIdsRef = useRef(allGroupIds);

  useEffect(() => {
    allGroupIdsRef.current = allGroupIds;
  }, [allGroupIds]);

  // For Graph view support: dependencies + selected version
  const selectedVersion = useSelectedVersion();
  const [dependencies] = useLocalStorage<"all" | "immediate" | "tasks">(dependenciesKey(dagId), "tasks");

  // Fetch structure (minimal params if you want it lightweight for Grid)
  const { data: structure = { edges: [], nodes: [] } } = useStructureServiceStructureData(
    {
      dagId,
      externalDependencies: dependencies === "immediate",
      versionNumber: selectedVersion,
    },
    undefined,
    { enabled: Boolean(dagId) && selectedVersion !== undefined },
  );

  // Update allGroupIds whenever structure changes
  useEffect(() => {
    const observedGroupIds = flattenGraphNodes(structure.nodes).allGroupIds;

    if (JSON.stringify(observedGroupIds) !== JSON.stringify(allGroupIdsRef.current)) {
      setAllGroupIds(observedGroupIds);
    }
  }, [structure.nodes, setAllGroupIds]);

  const debouncedSetOpenGroupIds = useDebouncedCallback(
    (newGroupIds: Array<string>) => {
      setOpenGroupIds(newGroupIds);
    },
    100, // 100ms debounce for batch operations
  );

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

  const value: OpenGroupsContextType = {
    allGroupIds,
    openGroupIds,
    setAllGroupIds,
    setOpenGroupIds: setOpenGroupIdsImmediate,
    toggleGroupId,
  };

  return <OpenGroupsContext.Provider value={value}>{children}</OpenGroupsContext.Provider>;
};

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
import { useCallback, useMemo, useEffect, type PropsWithChildren } from "react";
import { useDebouncedCallback } from "use-debounce";
import { useLocalStorage } from "usehooks-ts";

import { useStructureServiceStructureData } from "openapi/queries";
import useSelectedVersion from "src/hooks/useSelectedVersion";
import { flattenGraphNodes } from "src/layouts/Details/Grid/utils";

import { OpenGroupsContext, type OpenGroupsContextType } from "./Context";

type Props = {
  readonly dagId: string;
} & PropsWithChildren;

export const OpenGroupsProvider = ({ children, dagId }: Props) => {
  const openGroupsKey = `${dagId}/open-groups`;
  const allGroupsKey = `${dagId}/all-groups`;
  const [openGroupIds, setOpenGroupIds] = useLocalStorage<Array<string>>(openGroupsKey, []);
  const [allGroupIds, setAllGroupIds] = useLocalStorage<Array<string>>(allGroupsKey, []);

  // For Graph view support: dependencies + selected version
  const selectedVersion = useSelectedVersion();
  const [dependencies] = useLocalStorage<"all" | "immediate" | "tasks">(`dependencies-${dagId}`, "tasks");

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

    if (JSON.stringify(observedGroupIds) !== JSON.stringify(allGroupIds)) {
      setAllGroupIds(observedGroupIds);
    }
  }, [structure.nodes, allGroupIds, setAllGroupIds]);

  const debouncedSetOpenGroupIds = useDebouncedCallback(
    (newGroupIds: Array<string>) => {
      setOpenGroupIds(newGroupIds);
    },
    100, // 100ms debounce for batch operations
  );

  const toggleGroupId = useCallback(
    (groupId: string) => {
      if (openGroupIds.includes(groupId)) {
        debouncedSetOpenGroupIds(openGroupIds.filter((id) => id !== groupId));
      } else {
        debouncedSetOpenGroupIds([...openGroupIds, groupId]);
      }
    },
    [openGroupIds, debouncedSetOpenGroupIds],
  );

  const setOpenGroupIdsImmediate = useCallback(
    (newGroupIds: Array<string>) => {
      debouncedSetOpenGroupIds.cancel();
      setOpenGroupIds(newGroupIds);
    },
    [debouncedSetOpenGroupIds, setOpenGroupIds],
  );

  const value = useMemo<OpenGroupsContextType>(
    () => ({
      allGroupIds,
      openGroupIds,
      setAllGroupIds,
      setOpenGroupIds: setOpenGroupIdsImmediate,
      toggleGroupId,
    }),
    [allGroupIds, openGroupIds, setAllGroupIds, setOpenGroupIdsImmediate, toggleGroupId],
  );

  return <OpenGroupsContext.Provider value={value}>{children}</OpenGroupsContext.Provider>;
};

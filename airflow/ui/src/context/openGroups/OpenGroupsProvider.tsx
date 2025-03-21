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
import { createContext, useCallback, useMemo, type PropsWithChildren } from "react";
import { useLocalStorage } from "usehooks-ts";

export type OpenGroupsContextType = {
  collapseAllGroups: () => void;
  expandAllGroups: (allGroupIds: Array<string>) => void;
  openGroupIds: Array<string>;
  setOpenGroupIds: (groupIds: Array<string>) => void;
  toggleGroupId: (groupId: string) => void;
};

export const OpenGroupsContext = createContext<OpenGroupsContextType | undefined>(undefined);

type Props = {
  readonly dagId: string;
} & PropsWithChildren;

export const OpenGroupsProvider = ({ children, dagId }: Props) => {
  const openGroupsKey = `${dagId}/open-groups`;
  const [openGroupIds, setOpenGroupIds] = useLocalStorage<Array<string>>(openGroupsKey, []);

  const toggleGroupId = useCallback(
    (groupId: string) => {
      if (openGroupIds.includes(groupId)) {
        setOpenGroupIds(openGroupIds.filter((id) => id !== groupId));
      } else {
        setOpenGroupIds([...openGroupIds, groupId]);
      }
    },
    [openGroupIds, setOpenGroupIds],
  );

  const expandAllGroups = useCallback(
    (allGroupIds: Array<string>) => {
      const uniqueGroupIds = [...new Set([...openGroupIds, ...allGroupIds])];

      setOpenGroupIds(uniqueGroupIds);
    },
    [openGroupIds, setOpenGroupIds],
  );

  const collapseAllGroups = useCallback(() => {
    setOpenGroupIds([]);
  }, [setOpenGroupIds]);

  const value = useMemo<OpenGroupsContextType>(
    () => ({ collapseAllGroups, expandAllGroups, openGroupIds, setOpenGroupIds, toggleGroupId }),
    [collapseAllGroups, expandAllGroups, openGroupIds, setOpenGroupIds, toggleGroupId],
  );

  return <OpenGroupsContext.Provider value={value}>{children}</OpenGroupsContext.Provider>;
};

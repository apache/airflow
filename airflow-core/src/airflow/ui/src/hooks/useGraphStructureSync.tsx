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
import { useEffect } from "react";
import { useParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import { useStructureServiceStructureData } from "openapi/queries";
import { useOpenGroups } from "src/context/openGroups";
import useSelectedVersion from "src/hooks/useSelectedVersion";
import { flattenGraphNodes } from "src/layouts/Details/Grid/utils.ts";

export const useGraphStructureSync = () => {
  const { dagId = "" } = useParams();
  const selectedVersion = useSelectedVersion();
  const { allGroupIds, setAllGroupIds } = useOpenGroups();
  const [dependencies] = useLocalStorage<"all" | "immediate" | "tasks">(`dependencies-${dagId}`, "tasks");

  const { data: graphData = { edges: [], nodes: [] } } = useStructureServiceStructureData(
    {
      dagId,
      externalDependencies: dependencies === "immediate",
      versionNumber: selectedVersion,
    },
    undefined,
    { enabled: selectedVersion !== undefined },
  );

  useEffect(() => {
    const observedGroupIds = flattenGraphNodes(graphData.nodes).allGroupIds;

    if (
      observedGroupIds.length !== allGroupIds.length ||
      observedGroupIds.some((element, index) => allGroupIds[index] !== element)
    ) {
      setAllGroupIds(observedGroupIds);
    }
  }, [allGroupIds, graphData.nodes, setAllGroupIds]);

  return graphData;
};

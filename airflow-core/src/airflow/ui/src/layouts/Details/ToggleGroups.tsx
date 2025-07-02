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
import { type ButtonGroupProps, IconButton, ButtonGroup } from "@chakra-ui/react";
import { useMemo } from "react";
import { MdExpand, MdCompress } from "react-icons/md";
import { useParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import { useOpenGroups } from "src/context/openGroups";
import { useGridStructure } from "src/queries/useGridStructure.ts";

import { flattenNodes } from "./Grid/utils";

export const ToggleGroups = (props: ButtonGroupProps) => {
  const { openGroupIds, setOpenGroupIds } = useOpenGroups();
  const { dagId = "" } = useParams();
  const [limit] = useLocalStorage<number>(`dag_runs_limit-${dagId}`, 10);
  const { data: dagStructure } = useGridStructure({ limit });
  const { allGroupIds } = useMemo(
    () => flattenNodes(dagStructure, openGroupIds),
    [dagStructure, openGroupIds],
  );

  // Don't show button if the DAG has no task groups
  if (!allGroupIds.length) {
    return undefined;
  }

  const isExpandDisabled = allGroupIds.length === openGroupIds.length;
  const isCollapseDisabled = !openGroupIds.length;

  const onExpand = () => {
    setOpenGroupIds(allGroupIds);
  };

  const onCollapse = () => {
    setOpenGroupIds([]);
  };

  return (
    <ButtonGroup attached size="sm" variant="surface" {...props}>
      <IconButton
        aria-label="Expand all task groups"
        disabled={isExpandDisabled}
        onClick={onExpand}
        size="sm"
        title="Expand all task groups"
        variant="surface"
      >
        <MdExpand />
      </IconButton>
      <IconButton
        aria-label="Collapse all task groups"
        disabled={isCollapseDisabled}
        onClick={onCollapse}
        size="sm"
        title="Collapse all task groups"
        variant="surface"
      >
        <MdCompress />
      </IconButton>
    </ButtonGroup>
  );
};

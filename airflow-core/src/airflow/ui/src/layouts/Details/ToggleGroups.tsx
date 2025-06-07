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
import { useTranslation } from "react-i18next";
import { MdExpand, MdCompress } from "react-icons/md";
import { useParams } from "react-router-dom";

import { useStructureServiceStructureData } from "openapi/queries";
import { useOpenGroups } from "src/context/openGroups";

import { flattenNodes } from "./Grid/utils";

export const ToggleGroups = (props: ButtonGroupProps) => {
  const { t: translate } = useTranslation();
  const { dagId = "" } = useParams();
  const { data: structure } = useStructureServiceStructureData({
    dagId,
  });
  const { openGroupIds, setOpenGroupIds } = useOpenGroups();

  const { allGroupIds } = useMemo(
    () => flattenNodes(structure?.nodes ?? [], openGroupIds),
    [structure?.nodes, openGroupIds],
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

  const expandLabel = translate("dag:taskGroups.expandAll");
  const collapseLabel = translate("dag:taskGroups.collapseAll");

  return (
    <ButtonGroup attached size="sm" variant="surface" {...props}>
      <IconButton
        aria-label={expandLabel}
        disabled={isExpandDisabled}
        onClick={onExpand}
        size="sm"
        title={expandLabel}
        variant="surface"
      >
        <MdExpand />
      </IconButton>
      <IconButton
        aria-label={collapseLabel}
        disabled={isCollapseDisabled}
        onClick={onCollapse}
        size="sm"
        title={collapseLabel}
        variant="surface"
      >
        <MdCompress />
      </IconButton>
    </ButtonGroup>
  );
};

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
import { type ReactNode, useState } from "react";

import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

import { useTaskInstanceServiceGetMappedTaskInstances } from "openapi/queries";

import { CrumbSwitcher, type CrumbShape } from "src/components/Breadcrumb";

import { useTaskInstanceCoordinates } from "src/hooks/useTaskInstanceCoordinates";
import { useAutoRefresh } from "src/utils";
import type { MapIndexSearchOption } from "src/utils/option";

import { SearchMapIndexes } from "./SearchMapIndexes";
import { BY_MAP_INDEX, SEARCH_LIMIT, buildMapIndexOptions } from "./searchOptions";

const NO_INSTANCES: Array<MapIndexSearchOption> = [];

type Props = {
  readonly children: ReactNode;
  readonly dagId: string;
  readonly shape: CrumbShape;
};

/**
 * The map index level of the breadcrumb, with a search over the task's expanded instances behind
 * its chevron. Like the run level, what the panel lists is loaded here so that opening it shows
 * the instances straight away, and polling is held to while it is open.
 *
 * The naming half goes nowhere: every destination it could have is the page already on screen,
 * and the link would reach it without the region the url carries, which a looped task needs to
 * say which pass it means.
 */
export const MapIndexSwitcherButton = ({ children, dagId, shape }: Props) => {
  const { t: translate } = useTranslation();
  const { runId = "", taskId = "" } = useParams();
  // Not a filter: the iteration level decides which pass, so every index is offered whatever pass
  // it came from. This only says which pass to keep a row from when an index spans several.
  const { regionId } = useTaskInstanceCoordinates();
  const [open, setOpen] = useState(false);
  const refetchInterval = useAutoRefresh({ dagId });
  const { data, refetch } = useTaskInstanceServiceGetMappedTaskInstances(
    { dagId, dagRunId: runId, limit: SEARCH_LIMIT, orderBy: BY_MAP_INDEX, taskId },
    undefined,
    { refetchInterval: open && refetchInterval },
  );
  const instances = data === undefined ? NO_INSTANCES : buildMapIndexOptions(data.task_instances, regionId);

  const onOpenChange = (next: boolean) => {
    setOpen(next);

    if (next) {
      void refetch();
    }
  };

  return (
    <CrumbSwitcher
      // A rendered map index and its badge are a fraction of a run id; the default suits those.
      contentWidth="3xs"
      label={translate("switchMapIndex")}
      onOpenChange={onOpenChange}
      open={open}
      search={
        <SearchMapIndexes
          currentRegionId={regionId}
          dagId={dagId}
          instances={instances}
          onClose={() => setOpen(false)}
          runId={runId}
          taskId={taskId}
        />
      }
      shape={shape}
      testId="switch-map-index"
    >
      {children}
    </CrumbSwitcher>
  );
};

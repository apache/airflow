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

import { useDagRunServiceGetDagRuns } from "openapi/queries";

import { CrumbSwitcher, type CrumbShape } from "src/components/Breadcrumb";

import { useAutoRefresh } from "src/utils";
import type { DagRunSearchOption } from "src/utils/option";

import { SearchDagRuns } from "./SearchDagRuns";
import { NEWEST_FIRST, SEARCH_LIMIT, buildDagRunOption } from "./searchOptions";

const NO_RUNS: Array<DagRunSearchOption> = [];

type Props = {
  readonly children: ReactNode;
  readonly dagId: string;
  readonly shape: CrumbShape;
  readonly to: string;
};

/**
 * The Dag run level of the breadcrumb, with a search over the Dag's runs behind its chevron. It
 * stands in for both a named run and the "all runs" level, so `to` comes from the crumb itself.
 *
 * The runs the panel lists are loaded here rather than inside it: a query that only starts when
 * the panel opens has nothing to show until it answers. Here it is already loaded by the time
 * anyone opens the panel, and the page's auto-refresh keeps it that way. Unlike the grid, this
 * list has to notice runs that do not exist yet, so `checkPendingRuns` scales the interval back
 * once they have all finished rather than stopping.
 */
export const DagRunSwitcherButton = ({ children, dagId, shape, to }: Props) => {
  const { t: translate } = useTranslation();
  const [open, setOpen] = useState(false);
  const refetchInterval = useAutoRefresh({ checkPendingRuns: true, dagId });
  const { data } = useDagRunServiceGetDagRuns(
    { dagId, limit: SEARCH_LIMIT, orderBy: NEWEST_FIRST },
    undefined,
    { refetchInterval },
  );
  const runs = data === undefined ? NO_RUNS : data.dag_runs.map(buildDagRunOption);

  return (
    <CrumbSwitcher
      label={translate("switchDagRun")}
      onOpenChange={setOpen}
      open={open}
      search={<SearchDagRuns dagId={dagId} onClose={() => setOpen(false)} runs={runs} />}
      shape={shape}
      testId="switch-dag-run"
      to={to}
    >
      {children}
    </CrumbSwitcher>
  );
};

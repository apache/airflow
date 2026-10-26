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
  readonly isMapped: boolean;
  readonly shape: CrumbShape;
  readonly to: string;
};

/**
 * The Dag run level of the breadcrumb, with a search over the Dag's runs behind its chevron. It
 * stands in for both a named run and the "all runs" level, so `to` comes from the crumb itself.
 *
 * The runs the panel lists are loaded here rather than inside it: a query that only starts when
 * the panel opens has nothing to show until it answers, so opening would mean a spinner over an
 * empty list. Here it is loaded ahead of that, and opening only asks for a fresh answer over the
 * one already on screen.
 *
 * Polling is held to while the panel is open. It cannot be held to while runs are pending the way
 * the grid does, because this list exists to notice runs that do not exist yet; left on, it would
 * poll from every page of a Dag for a panel nobody opened.
 */
export const DagRunSwitcherButton = ({ children, dagId, isMapped, shape, to }: Props) => {
  const { t: translate } = useTranslation();
  const [open, setOpen] = useState(false);
  const refetchInterval = useAutoRefresh({ dagId });
  const { data, refetch } = useDagRunServiceGetDagRuns(
    { dagId, limit: SEARCH_LIMIT, orderBy: NEWEST_FIRST },
    undefined,
    { refetchInterval: open && refetchInterval },
  );
  const runs = data === undefined ? NO_RUNS : data.dag_runs.map(buildDagRunOption);

  const onOpenChange = (next: boolean) => {
    setOpen(next);

    // The cached runs are shown straight away; this only catches up what changed while closed.
    if (next) {
      void refetch();
    }
  };

  return (
    <CrumbSwitcher
      label={translate("switchDagRun")}
      onOpenChange={onOpenChange}
      open={open}
      search={<SearchDagRuns dagId={dagId} isMapped={isMapped} onClose={() => setOpen(false)} runs={runs} />}
      shape={shape}
      testId="switch-dag-run"
      to={to}
    >
      {children}
    </CrumbSwitcher>
  );
};

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
import type { PropsWithChildren } from "react";

import { Box } from "@chakra-ui/react";
import { Outlet } from "react-router-dom";

import { ProgressBar } from "src/system-components";

import { NavTabs, type NavTab } from "./NavTabs";
import { useReportDetailsError } from "./detailsError";

type Props = {
  readonly error?: unknown;
  readonly isLoading?: boolean;
  /** Value exposed to the active tab via ``useOutletContext`` (so tabs can reuse this level's data). */
  readonly outletContext?: unknown;
  readonly tabs: Array<NavTab>;
} & PropsWithChildren;

/**
 * The contents of the details panel for one level of a Dag — its header, tab bar and open tab.
 *
 * Each level renders this from its own route, nested inside the ``DetailsLayout`` route that owns
 * the Grid, so that moving between runs and task instances swaps only the panel. Keeping it on the
 * level's route is also what lets ``NavTabs`` go on addressing its tabs relatively: the tab values
 * resolve against the path of the route that rendered them.
 */
export const DetailsPanel = ({ children, error, isLoading, outletContext, tabs }: Props) => {
  useReportDetailsError(error);

  return (
    <>
      {children}
      <ProgressBar size="xs" visibility={isLoading ? "visible" : "hidden"} />
      <NavTabs tabs={tabs} />
      <Box flexGrow={1} overflow="auto" pb={3} px={2}>
        <Outlet context={outletContext} />
      </Box>
    </>
  );
};

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
import { Box, Button, HStack } from "@chakra-ui/react";
import type { PropsWithChildren } from "react";
import { FaChartGantt } from "react-icons/fa6";
import { FiChevronsLeft, FiGrid } from "react-icons/fi";
import { Outlet, Link as RouterLink, useParams, useSearchParams } from "react-router-dom";

import type { DAGResponse } from "openapi/requests/types.gen";
import { DagIcon } from "src/assets/DagIcon";
import { ErrorAlert } from "src/components/ErrorAlert";
import { SearchDagsButton } from "src/components/SearchDags";
import { ProgressBar } from "src/components/ui";
import { Toaster } from "src/components/ui";
import { OpenGroupsProvider } from "src/context/openGroups";

import { DagVizModal } from "./DagVizModal";
import { NavTabs } from "./NavTabs";

type Props = {
  readonly dag?: DAGResponse;
  readonly error?: unknown;
  readonly isLoading?: boolean;
  readonly tabs: Array<{ label: string; value: string }>;
} & PropsWithChildren;

export const DetailsLayout = ({ children, dag, error, isLoading, tabs }: Props) => {
  const { dagId = "" } = useParams();

  const [searchParams, setSearchParams] = useSearchParams();

  const modal = searchParams.get("modal");

  const isModalOpen = modal !== null;
  const onClose = () => {
    searchParams.delete("modal");
    setSearchParams(searchParams);
  };

  return (
    <OpenGroupsProvider dagId={dagId}>
      <Box>
        <HStack justifyContent="space-between" mb={2}>
          <Button asChild colorPalette="blue" variant="ghost">
            <RouterLink to="/dags">
              <FiChevronsLeft />
              Back to all dags
            </RouterLink>
          </Button>
          <SearchDagsButton />
        </HStack>
        <Toaster />
        {isModalOpen ? undefined : children}
        <ErrorAlert error={error} />
        <ProgressBar size="xs" visibility={isLoading ? "visible" : "hidden"} />
        <NavTabs
          keepSearch
          rightButtons={
            <>
              <Button asChild colorPalette="blue" variant="ghost">
                <RouterLink to={{ search: `${searchParams.toString()}&modal=gantt` }}>
                  <FaChartGantt height={5} width={5} />
                  Gantt
                </RouterLink>
              </Button>
              <Button asChild colorPalette="blue" variant="ghost">
                <RouterLink to={{ search: `${searchParams.toString()}&modal=grid` }}>
                  <FiGrid height={5} width={5} />
                  Grid
                </RouterLink>
              </Button>
              <Button asChild colorPalette="blue" variant="ghost">
                <RouterLink to={{ search: `${searchParams.toString()}&modal=graph` }}>
                  <DagIcon height={5} width={5} />
                  Graph
                </RouterLink>
              </Button>
            </>
          }
          tabs={tabs}
        />
        <DagVizModal
          dagDisplayName={dag?.dag_display_name}
          dagId={dag?.dag_id}
          onClose={onClose}
          open={isModalOpen}
        />
      </Box>
      <Box overflow="auto">{isModalOpen ? undefined : <Outlet />}</Box>
    </OpenGroupsProvider>
  );
};

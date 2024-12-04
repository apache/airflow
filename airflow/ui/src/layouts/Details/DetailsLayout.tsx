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
import { Box, Button } from "@chakra-ui/react";
import type { PropsWithChildren } from "react";
import { FiChevronsLeft } from "react-icons/fi";
import {
  Outlet,
  Link as RouterLink,
  useParams,
  useSearchParams,
} from "react-router-dom";

import type { DAGResponse } from "openapi/requests/types.gen";
import { ErrorAlert } from "src/components/ErrorAlert";
import { ProgressBar } from "src/components/ui";
import { OpenGroupsProvider } from "src/context/openGroups";
import { DagVizModal } from "src/layouts/Details/DagVizModal";
import { NavTabs } from "src/layouts/Details/NavTabs";

type Props = {
  readonly dag?: DAGResponse;
  readonly error?: unknown;
  readonly isLoading?: boolean;
  readonly tabs: Array<{ label: string; value: string }>;
} & PropsWithChildren;

export const DetailsLayout = ({
  children,
  dag,
  error,
  isLoading,
  tabs,
}: Props) => {
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
        <Button asChild colorPalette="blue" variant="ghost">
          <RouterLink to="/dags">
            <FiChevronsLeft />
            Back to all dags
          </RouterLink>
        </Button>
        {children}
        <ErrorAlert error={error} />
        <ProgressBar size="xs" visibility={isLoading ? "visible" : "hidden"} />
        <NavTabs tabs={tabs} />
        <DagVizModal
          dagDisplayName={dag?.dag_display_name}
          dagId={dag?.dag_id}
          onClose={onClose}
          open={isModalOpen}
        />
      </Box>
      <Box overflow="auto">
        <Outlet />
      </Box>
    </OpenGroupsProvider>
  );
};

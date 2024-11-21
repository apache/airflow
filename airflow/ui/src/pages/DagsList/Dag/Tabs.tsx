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
import { Button, Flex, Tabs } from "@chakra-ui/react";
import {
  Link as RouterLink,
  useLocation,
  useParams,
  useSearchParams,
} from "react-router-dom";

import type { DAGResponse } from "openapi/requests/types.gen";
import { DagIcon } from "src/assets/DagIcon";
import { capitalize } from "src/utils";

import { DagVizModal } from "./DagVizModal";

const tabs = ["runs", "tasks", "events", "code"];

const MODAL = "modal";

export const DagTabs = ({ dag }: { readonly dag?: DAGResponse }) => {
  const { dagId } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();

  const modal = searchParams.get(MODAL);

  const isGraphOpen = modal === "graph";
  const onClose = () => {
    searchParams.delete(MODAL);
    setSearchParams(searchParams);
  };

  const onOpen = () => {
    searchParams.set(MODAL, "graph");
    setSearchParams(searchParams);
  };

  const { pathname } = useLocation();

  const activeTab =
    tabs.find((tab) => pathname.endsWith(`/${tab}`)) ?? "overview";

  return (
    <>
      <Tabs.Root value={activeTab}>
        <Tabs.List justifyContent="space-between" position="relative">
          <Flex>
            <Tabs.Trigger asChild value="overview">
              <RouterLink to={`/dags/${dagId}`}>Overview</RouterLink>
            </Tabs.Trigger>
            {tabs.map((tab) => (
              <Tabs.Trigger asChild key={tab} value={tab}>
                <RouterLink to={`/dags/${dagId}/${tab}`}>
                  {capitalize(tab)}
                </RouterLink>
              </Tabs.Trigger>
            ))}
          </Flex>
          <Flex alignSelf="flex-end">
            <Button colorPalette="blue" onClick={onOpen} variant="ghost">
              <DagIcon height={5} width={5} />
              Graph
            </Button>
          </Flex>
        </Tabs.List>
      </Tabs.Root>
      <DagVizModal
        dagDisplayName={dag?.dag_display_name ?? "graph"}
        onClose={onClose}
        open={isGraphOpen}
      />
    </>
  );
};

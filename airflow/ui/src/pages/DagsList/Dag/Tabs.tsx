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
import { Button, Center, Flex } from "@chakra-ui/react";
import { NavLink, useSearchParams } from "react-router-dom";

import type { DAGDetailsResponse } from "openapi/requests/types.gen";
import { DagIcon } from "src/assets/DagIcon";
import { capitalize } from "src/utils";

import { DagDocumentation } from "./DagDocumentation";
import { DagVizModal } from "./DagVizModal";

const tabs = ["overview", "runs", "tasks", "events", "code"];

const MODAL = "modal";

export const DagTabs = ({
  dag,
  isDocsOpen,
  setIsDocsOpen,
}: {
  readonly dag?: DAGDetailsResponse;
  readonly isDocsOpen: boolean;
  readonly setIsDocsOpen: React.Dispatch<React.SetStateAction<boolean>>;
}) => {
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

  return (
    <>
      <Flex
        alignItems="center"
        borderBottomWidth={1}
        justifyContent="space-between"
      >
        <Flex>
          {tabs.map((tab) => (
            <NavLink end key={tab} to={tab === "overview" ? "" : tab}>
              {({ isActive }) => (
                <Center
                  borderBottomColor="border.info"
                  borderBottomWidth={isActive ? 3 : 0}
                  fontWeight={isActive ? "bold" : undefined}
                  height="40px"
                  mb="-2px" // Show the border on top of its parent's border
                  pb={isActive ? 0 : "3px"}
                  px={2}
                  transition="all 0.2s ease"
                  width="100px"
                >
                  {capitalize(tab)}
                </Center>
              )}
            </NavLink>
          ))}
        </Flex>
        <Flex alignSelf="flex-end">
          <Button colorPalette="blue" onClick={onOpen} variant="ghost">
            <DagIcon height={5} width={5} />
            Graph
          </Button>
        </Flex>
      </Flex>
      <DagVizModal
        dagDisplayName={dag?.dag_display_name}
        dagId={dag?.dag_id}
        onClose={onClose}
        open={isGraphOpen}
      />
      <DagDocumentation
        docMd={dag?.doc_md ?? ""}
        onClose={() => setIsDocsOpen(false)}
        open={isDocsOpen}
      />
    </>
  );
};

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
import { Button } from "@chakra-ui/react";
import { useSearchParams } from "react-router-dom";

import type { DAGResponse } from "openapi/requests/types.gen";
import { DagIcon } from "src/assets/DagIcon";
import { DagVizModal } from "src/components/DagVizModal";
import { NavTabs } from "src/components/NavTabs";

const tabs = [
  { label: "Task Instances", value: "" },
  { label: "Events", value: "events" },
  { label: "Code", value: "code" },
];

const MODAL = "modal";

export const RunTabs = ({ dag }: { readonly dag?: DAGResponse }) => {
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
      <NavTabs
        rightButtons={
          <Button colorPalette="blue" onClick={onOpen} variant="ghost">
            <DagIcon height={5} width={5} />
            Graph
          </Button>
        }
        tabs={tabs}
      />
      <DagVizModal
        dagDisplayName={dag?.dag_display_name}
        dagId={dag?.dag_id}
        onClose={onClose}
        open={isGraphOpen}
      />
    </>
  );
};

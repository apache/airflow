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
import { Button, Heading, HStack } from "@chakra-ui/react";
import { FaChartGantt } from "react-icons/fa6";
import { FiGrid } from "react-icons/fi";
import { Link as RouterLink, useSearchParams } from "react-router-dom";

import type { DAGResponse } from "openapi/requests/types.gen";
import { DagIcon } from "src/assets/DagIcon";
import { Dialog } from "src/components/ui";
import { capitalize } from "src/utils";

import { Gantt } from "./Gantt";
import { Graph } from "./Graph";
import { Grid } from "./Grid";

type TriggerDAGModalProps = {
  dagDisplayName?: DAGResponse["dag_display_name"];
  dagId?: DAGResponse["dag_id"];
  onClose: () => void;
  open: boolean;
};

const visualizationOptions = [
  {
    component: <Gantt />,
    icon: <FaChartGantt height={5} width={5} />,
    value: "gantt",
  },
  {
    component: <Graph />,
    icon: <DagIcon height={5} width={5} />,
    value: "graph",
  },
  { component: <Grid />, icon: <FiGrid height={5} width={5} />, value: "grid" },
];

export const DagVizModal: React.FC<TriggerDAGModalProps> = ({
  dagDisplayName,
  dagId,
  onClose,
  open,
}) => {
  const [searchParams] = useSearchParams();

  const activeViz = searchParams.get("modal") ?? "graph";

  return (
    <Dialog.Root onOpenChange={onClose} open={open} size="full">
      <Dialog.Content backdrop>
        <Dialog.Header bg="blue.muted">
          <HStack>
            <Heading mr={3} size="xl">
              {dagDisplayName ?? dagId}
            </Heading>
            {visualizationOptions.map(({ icon, value }) => (
              <RouterLink
                key={value}
                to={{
                  search: `modal=${value}`,
                }}
              >
                <Button
                  borderColor="colorPalette.fg"
                  borderRadius={20}
                  colorPalette="blue"
                  variant={activeViz === value ? "solid" : "outline"}
                >
                  {icon}
                  {capitalize(value)}
                </Button>
              </RouterLink>
            ))}
          </HStack>
          <Dialog.CloseTrigger closeButtonProps={{ size: "xl" }} />
        </Dialog.Header>
        <Dialog.Body display="flex">
          {dagId === undefined
            ? undefined
            : visualizationOptions.find((viz) => viz.value === activeViz)
                ?.component}
        </Dialog.Body>
      </Dialog.Content>
    </Dialog.Root>
  );
};

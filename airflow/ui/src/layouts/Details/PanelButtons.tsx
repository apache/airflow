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
import { HStack, IconButton, ButtonGroup } from "@chakra-ui/react";
import { FiGrid } from "react-icons/fi";
import { MdOutlineAccountTree } from "react-icons/md";

import DagVersionSelect from "src/components/DagVersionSelect";

type Props = {
  readonly dagId: string;
  readonly dagView: string;
  readonly setDagView: (x: "graph" | "grid") => void;
};

export const PanelButtons = ({ dagId, dagView, setDagView }: Props) => (
  <HStack justifyContent="space-between" py={2}>
    <ButtonGroup attached left={0} size="sm" top={0} variant="outline" zIndex={1}>
      <IconButton
        aria-label="Show Grid"
        colorPalette="blue"
        onClick={() => setDagView("grid")}
        title="Show Grid"
        variant={dagView === "grid" ? "solid" : "outline"}
      >
        <FiGrid />
      </IconButton>
      <IconButton
        aria-label="Show Graph"
        colorPalette="blue"
        onClick={() => setDagView("graph")}
        title="Show Graph"
        variant={dagView === "graph" ? "solid" : "outline"}
      >
        <MdOutlineAccountTree />
      </IconButton>
    </ButtonGroup>
    <DagVersionSelect dagId={dagId} disabled={dagView !== "graph"} />
  </HStack>
);

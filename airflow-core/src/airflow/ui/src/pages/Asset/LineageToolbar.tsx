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
import { useTranslation } from "react-i18next";

import { SearchBar } from "src/components/SearchBar";

import type { LineageDirection } from "./lineageHighlightUtils";

type LineageMode = "asset_only" | "full";

type Props = {
  readonly lineageDirection: LineageDirection;
  readonly lineageMode: LineageMode;
  readonly lineageSearch: string;
  readonly setLineageDirection: (direction: LineageDirection) => void;
  readonly setLineageMode: (mode: LineageMode) => void;
  readonly setLineageSearch: (term: string) => void;
};

export const LineageToolbar = ({
  lineageDirection,
  lineageMode,
  lineageSearch,
  setLineageDirection,
  setLineageMode,
  setLineageSearch,
}: Props) => {
  const { t: translate } = useTranslation(["assets"]);

  return (
    <Box left={3} position="absolute" right={3} top={14} zIndex={5}>
      <HStack>
        <Box flex={1}>
          <SearchBar
            defaultValue={lineageSearch}
            hotkeyDisabled
            onChange={setLineageSearch}
            placeholder="Search lineage nodes"
          />
        </Box>
        <Button
          colorPalette={lineageMode === "full" ? "blue" : "gray"}
          onClick={() => {
            setLineageMode("full");
          }}
          size="sm"
          variant={lineageMode === "full" ? "solid" : "outline"}
        >
          {translate("assets:lineage_full", { defaultValue: "Full" })}
        </Button>
        <Button
          colorPalette={lineageMode === "asset_only" ? "blue" : "gray"}
          onClick={() => {
            setLineageMode("asset_only");
          }}
          size="sm"
          variant={lineageMode === "asset_only" ? "solid" : "outline"}
        >
          {translate("assets:lineage_asset_only", { defaultValue: "Asset Only" })}
        </Button>
        <Button
          colorPalette={lineageDirection === "upstream" ? "blue" : "gray"}
          onClick={() => {
            setLineageDirection("upstream");
          }}
          size="sm"
          variant={lineageDirection === "upstream" ? "solid" : "outline"}
        >
          {translate("assets:lineage_upstream", { defaultValue: "Upstream" })}
        </Button>
        <Button
          colorPalette={lineageDirection === "downstream" ? "blue" : "gray"}
          onClick={() => {
            setLineageDirection("downstream");
          }}
          size="sm"
          variant={lineageDirection === "downstream" ? "solid" : "outline"}
        >
          {translate("assets:lineage_downstream", { defaultValue: "Downstream" })}
        </Button>
      </HStack>
    </Box>
  );
};

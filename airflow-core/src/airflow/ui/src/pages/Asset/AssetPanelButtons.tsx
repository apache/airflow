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
import { Box, Button, ButtonGroup, Flex, IconButton, Popover, Portal } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { MdSettings } from "react-icons/md";
import { useParams } from "react-router-dom";

import { DirectionDropdown } from "src/components/Graph/DirectionDropdown";

type Props = {
  readonly dependencyType: "data" | "scheduling";
  readonly setDependencyType: React.Dispatch<React.SetStateAction<"data" | "scheduling">>;
};

export const AssetPanelButtons = ({ dependencyType, setDependencyType }: Props) => {
  const { t: translate } = useTranslation(["assets"]);
  const { assetId } = useParams();

  return (
    <Box borderRadius="md" position="absolute" px={2} py={1} right={2} top={1} zIndex={1}>
      <Flex justifyContent="space-between">
        <ButtonGroup attached size="sm" variant="outline">
          <Button
            bg={dependencyType === "scheduling" ? "brand.500" : "bg.subtle"}
            color={dependencyType === "scheduling" ? "white" : "fg.default"}
            colorPalette="brand"
            onClick={() => setDependencyType("scheduling")}
          >
            {translate("assets:scheduling")}
          </Button>
          <Button
            bg={dependencyType === "data" ? "brand.500" : "bg.subtle"}
            color={dependencyType === "data" ? "white" : "fg.default"}
            colorPalette="brand"
            onClick={() => setDependencyType("data")}
          >
            {translate("assets:taskDependencies")}
          </Button>
        </ButtonGroup>
        <Popover.Root positioning={{ placement: "bottom-end" }}>
          <Popover.Trigger asChild>
            <IconButton
              aria-label={translate("dag:panel.buttons.options")}
              colorPalette="brand"
              size="md"
              title={translate("dag:panel.buttons.options")}
              variant="ghost"
            >
              <MdSettings />
            </IconButton>
          </Popover.Trigger>
          <Portal>
            <Popover.Positioner>
              <Popover.Content>
                <Popover.Arrow />
                <Popover.Body
                  display="flex"
                  flexDirection="column"
                  gap={4}
                  maxH="70vh"
                  overflowY="auto"
                  p={2}
                >
                  <DirectionDropdown graphId={assetId ?? ""} />
                </Popover.Body>
              </Popover.Content>
            </Popover.Positioner>
          </Portal>
        </Popover.Root>
      </Flex>
    </Box>
  );
};

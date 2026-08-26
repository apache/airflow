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
import { Box, Button, ButtonGroup, Flex, Popover, Portal } from "@chakra-ui/react";
import type { Dispatch, SetStateAction } from "react";
import { useTranslation } from "react-i18next";
import { MdSettings } from "react-icons/md";
import { useParams } from "react-router-dom";

import { DirectionDropdown } from "src/components/Graph/DirectionDropdown";
import { IconButton, Tooltip } from "src/components/ui";

type Props = {
  readonly dependencyType: "data" | "scheduling";
  readonly setDependencyType: Dispatch<SetStateAction<"data" | "scheduling">>;
};

export const AssetPanelButtons = ({ dependencyType, setDependencyType }: Props) => {
  const { t: translate } = useTranslation(["assets"]);
  const { assetId } = useParams();

  return (
    <Box left={0} p={2} position="absolute" right={0} top={0} zIndex={1}>
      <Flex justifyContent="space-between">
        <ButtonGroup attached size="sm" variant="outline">
          <Button
            bg={dependencyType === "scheduling" ? "brand.500" : "bg.subtle"}
            color={dependencyType === "scheduling" ? "white" : "fg.default"}
            onClick={() => setDependencyType("scheduling")}
          >
            {translate("assets:scheduling")}
          </Button>
          <Button
            bg={dependencyType === "data" ? "brand.500" : "bg.subtle"}
            color={dependencyType === "data" ? "white" : "fg.default"}
            onClick={() => setDependencyType("data")}
          >
            {translate("assets:taskDependencies")}
          </Button>
        </ButtonGroup>
        <Popover.Root positioning={{ placement: "bottom-end" }}>
          {/* Tooltip and popover each need their own element: both triggers set an `id` on whatever
                they wrap, and zag resolves a trigger by id, so sharing one element leaves the loser
                unable to find its anchor and positioning at the viewport origin. */}
          <Tooltip content={translate("dag:panel.buttons.options")} portalled>
            <Box display="flex">
              <Popover.Trigger asChild>
                <IconButton aria-label={translate("dag:panel.buttons.options")} bg="bg" variant="outline">
                  <MdSettings />
                </IconButton>
              </Popover.Trigger>
            </Box>
          </Tooltip>
          <Portal>
            <Popover.Positioner>
              <Popover.Content>
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

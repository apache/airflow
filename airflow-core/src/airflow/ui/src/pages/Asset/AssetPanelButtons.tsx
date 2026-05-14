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
import { Badge, Box, Button, ButtonGroup, HStack, Icon } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuInfo } from "react-icons/lu";

import { Tooltip } from "src/components/ui";

type Props = {
  readonly dependencyType: "data" | "lineage" | "scheduling";
  readonly setDependencyType: React.Dispatch<React.SetStateAction<"data" | "lineage" | "scheduling">>;
};

export const AssetPanelButtons = ({ dependencyType, setDependencyType }: Props) => {
  const { t: translate } = useTranslation(["assets"]);

  return (
    <Box borderRadius="md" position="absolute" px={2} py={1} right={2} top={1} zIndex={6}>
      <HStack gap={2}>
        <Tooltip
          content={translate(
            "assets:read_only_tooltip",
            {
              defaultValue:
                "This view is read-only. To add assets, define inlets and outlets in your DAG scripts inside files/dags/, then restart Airflow.",
            },
          )}
          portalled
          showArrow
        >
          <Badge colorPalette="gray" cursor="default" size="sm" variant="subtle">
            {translate("assets:read_only", { defaultValue: "Read Only" })}
          </Badge>
        </Tooltip>
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
          <Tooltip
            content={translate(
              "assets:lineage_button_tooltip",
              {
                defaultValue:
                  "Shows how assets flow between tasks. Tasks declare inlets (assets they consume) and outlets (assets they produce) in the DAG script.",
              },
            )}
            portalled
            showArrow
          >
            <Button
              bg={dependencyType === "lineage" ? "brand.500" : "bg.subtle"}
              color={dependencyType === "lineage" ? "white" : "fg.default"}
              colorPalette="brand"
              onClick={() => setDependencyType("lineage")}
            >
              <HStack gap={1}>
                <span>{translate("Data Lineage")}</span>
                <Icon as={LuInfo} boxSize={3} opacity={0.7} />
              </HStack>
            </Button>
          </Tooltip>
        </ButtonGroup>
      </HStack>
    </Box>
  );
};

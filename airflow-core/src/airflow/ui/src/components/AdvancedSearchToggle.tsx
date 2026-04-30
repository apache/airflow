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
import { Box, IconButton, Stack, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuRegex } from "react-icons/lu";

import { Tooltip } from "src/components/ui";

export type AdvancedSearchToggleProps = {
  readonly enabled: boolean;
  readonly onToggle: (enabled: boolean) => void;
  readonly size?: "2xs" | "sm" | "xs";
  // "standalone" → free-floating IconButton next to a SearchBar.
  // "addon"      → flush right addon inside a rounded pill input, mirroring the label on the left.
  readonly variant?: "addon" | "standalone";
};

export const AdvancedSearchToggle = ({
  enabled,
  onToggle,
  size = "sm",
  variant = "standalone",
}: AdvancedSearchToggleProps) => {
  const { t: translate } = useTranslation("common");

  const button =
    variant === "addon" ? (
      <Box
        alignItems="center"
        alignSelf="stretch"
        aria-label="Toggle match-anywhere search"
        aria-pressed={enabled}
        as="button"
        bg={enabled ? "colorPalette.solid" : "gray.muted"}
        borderRightRadius="full"
        color={enabled ? "colorPalette.contrast" : "colorPalette.fg"}
        colorPalette={enabled ? "brand" : "gray"}
        cursor="pointer"
        data-testid="advanced-search-toggle"
        display="flex"
        onClick={() => onToggle(!enabled)}
        // Keep focus on the FilterPill input so toggling does not collapse the pill.
        onMouseDown={(event) => event.preventDefault()}
        px={3}
      >
        <LuRegex />
      </Box>
    ) : (
      <IconButton
        aria-label="Toggle match-anywhere search"
        aria-pressed={enabled}
        colorPalette="brand"
        data-testid="advanced-search-toggle"
        flexShrink={0}
        onClick={() => onToggle(!enabled)}
        onMouseDown={(event) => event.preventDefault()}
        size={size}
        variant={enabled ? "solid" : "outline"}
      >
        <LuRegex />
      </IconButton>
    );

  return (
    <Tooltip
      content={
        <Stack gap={1} maxW="320px">
          <Text fontWeight="semibold">{translate("search.advanced.title")}</Text>
          <Text>{translate("search.advanced.description")}</Text>
        </Stack>
      }
      openDelay={200}
      portalled
      showArrow
    >
      {button}
    </Tooltip>
  );
};

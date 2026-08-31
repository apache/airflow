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
import { HStack, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { Switch } from "src/system-components";

import type { MatchMode } from "src/hooks/useMatchMode";

type Props = {
  readonly mode: MatchMode;
  readonly onModeChange: (mode: MatchMode) => void;
};

export const MatchModeToggle = ({ mode, onModeChange }: Props) => {
  const { t: translate } = useTranslation();

  return (
    <HStack
      align="center"
      alignSelf="stretch"
      bg="gray.muted"
      borderRightRadius="full"
      data-testid="match-mode-toggle"
      gap={1}
      // Keep focus on the pill so toggling does not blur and collapse it.
      onMouseDown={(event) => event.preventDefault()}
      px={3}
    >
      <Text color={mode === "any" ? "fg.info" : "fg.muted"} fontSize="sm">
        {translate("table.tagMode.any")}
      </Text>
      <Switch
        checked={mode === "all"}
        onCheckedChange={({ checked }) => onModeChange(checked ? "all" : "any")}
        variant="raised"
      />
      <Text color={mode === "all" ? "fg.info" : "fg.muted"} fontSize="sm">
        {translate("table.tagMode.all")}
      </Text>
    </HStack>
  );
};

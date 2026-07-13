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
import { Box, HStack, useDisclosure } from "@chakra-ui/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FiX } from "react-icons/fi";
import { LuCheck } from "react-icons/lu";

import type { DagRunMutableStates, DAGRunResponse } from "openapi/requests/types.gen";
import { StateBadge } from "src/components/StateBadge";
import { IconButton, Menu, Tooltip } from "src/components/ui";
import { SHORTCUTS } from "src/context/keyboardShortcuts";
import { useShortcut } from "src/hooks/useShortcut";

import { allowedStates } from "../utils";
import MarkRunAsDialog from "./MarkRunAsDialog";

type Props = {
  readonly dagRun: DAGRunResponse;
  readonly isHotkeyEnabled?: boolean;
};

const MarkRunAsButton = ({ dagRun, isHotkeyEnabled = false }: Props) => {
  const { onClose, onOpen, open } = useDisclosure();
  const [state, setState] = useState<DagRunMutableStates>("success");
  const { t: translate } = useTranslation();

  useShortcut({
    ...SHORTCUTS.runActions.markRunFailed,
    callback: () => {
      setState("failed");
      onOpen();
    },
    options: { enabled: isHotkeyEnabled },
  });

  useShortcut({
    ...SHORTCUTS.runActions.markRunSuccess,
    callback: () => {
      setState("success");
      onOpen();
    },
    options: { enabled: isHotkeyEnabled },
  });

  const label = translate("dags:runAndTaskActions.markAs.button", {
    type: translate("dagRun_one"),
  });

  return (
    <Box>
      <Menu.Root positioning={{ gutter: 0, placement: "bottom" }} tooltipLabel={label}>
        <Menu.Trigger asChild>
          <IconButton aria-label={label} data-testid="mark-run-as-button">
            <HStack gap={1} mx={1}>
              <LuCheck />
              <span>/</span>
              <FiX />
            </HStack>
          </IconButton>
        </Menu.Trigger>
        <Menu.Content>
          {allowedStates.map((menuState) => {
            const content = translate(
              `dags:runAndTaskActions.markAs.buttonTooltip.${menuState === "success" ? "success" : "failed"}`,
            );

            return (
              <Tooltip
                closeDelay={100}
                content={content}
                disabled={!isHotkeyEnabled}
                key={menuState}
                openDelay={100}
              >
                {/* Not disabled when state matches: re-applying lets users also flip upstream/downstream tasks */}
                <Menu.Item
                  asChild
                  data-testid={`mark-run-as-${menuState}`}
                  key={menuState}
                  onClick={() => {
                    setState(menuState);
                    onOpen();
                  }}
                  value={menuState}
                >
                  <StateBadge my={1} state={menuState}>
                    {translate(`common:states.${menuState}`)}
                  </StateBadge>
                </Menu.Item>
              </Tooltip>
            );
          })}
        </Menu.Content>
      </Menu.Root>

      <MarkRunAsDialog dagRun={dagRun} onClose={onClose} open={open} state={state} />
    </Box>
  );
};

export default MarkRunAsButton;

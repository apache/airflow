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
import { Box, useDisclosure } from "@chakra-ui/react";
import { useState } from "react";
import { useHotkeys } from "react-hotkeys-hook";
import { useTranslation } from "react-i18next";
import { MdArrowDropDown } from "react-icons/md";

import type { TaskInstanceResponse, TaskInstanceState } from "openapi/requests/types.gen";
import { StateBadge } from "src/components/StateBadge";
import { Menu, Tooltip } from "src/components/ui";
import ActionButton from "src/components/ui/ActionButton";

import { allowedStates } from "../utils";
import MarkTaskInstanceAsDialog from "./MarkTaskInstanceAsDialog";

type Props = {
  readonly isHotkeyEnabled?: boolean;
  readonly taskInstance: TaskInstanceResponse;
  readonly withText?: boolean;
};

const MarkTaskInstanceAsButton = ({ isHotkeyEnabled = false, taskInstance, withText = true }: Props) => {
  const { onClose, onOpen, open } = useDisclosure();
  const { t: translate } = useTranslation();

  const [state, setState] = useState<TaskInstanceState>("success");

  useHotkeys(
    "shift+f",
    () => {
      setState("failed");
      onOpen();
    },
    { enabled: isHotkeyEnabled && taskInstance.state !== "failed" },
  );

  useHotkeys(
    "shift+s",
    () => {
      setState("success");
      onOpen();
    },
    { enabled: isHotkeyEnabled && taskInstance.state !== "success" },
  );

  return (
    <Box>
      <Menu.Root positioning={{ gutter: 0, placement: "bottom" }}>
        <Menu.Trigger asChild>
          <ActionButton
            actionName={translate("dags:runAndTaskActions.markAs.button", {
              type: translate("taskInstance_one"),
            })}
            flexDirection="row-reverse"
            icon={<MdArrowDropDown />}
            text={translate("dags:runAndTaskActions.markAs.button", { type: translate("taskInstance_one") })}
            withText={withText}
          />
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
                disabled={!isHotkeyEnabled || taskInstance.state === menuState}
                key={menuState}
                openDelay={100}
              >
                <Menu.Item
                  asChild
                  disabled={taskInstance.state === menuState}
                  key={menuState}
                  onClick={() => {
                    if (taskInstance.state !== menuState) {
                      setState(menuState);
                      onOpen();
                    }
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

      {open ? (
        <MarkTaskInstanceAsDialog onClose={onClose} open={open} state={state} taskInstance={taskInstance} />
      ) : undefined}
    </Box>
  );
};

export default MarkTaskInstanceAsButton;

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
import { useDisclosure } from "@chakra-ui/react";
import type { TaskInstanceResponse, TaskInstanceState } from "openapi-gen/requests/types.gen";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { MdArrowDropDown } from "react-icons/md";

import { StateBadge } from "src/components/StateBadge";
import { Menu, Tooltip } from "src/components/ui";
import ActionButton from "src/components/ui/ActionButton";

import PatchTaskInstancesDialog from "./MarkTaskInstancesAsDialog";

type Props = {
  readonly clearSelections: () => void;
  readonly dagId: string;
  readonly dagRunId: string;
  readonly patchKeys: Array<TaskInstanceResponse>;
};

const MarkTaskInstancesAsButton = ({ clearSelections, dagId, dagRunId, patchKeys }: Props) => {
  const { onClose, onOpen, open } = useDisclosure();
  const [selectedState, setSelectedState] = useState<TaskInstanceState>("success");

  const allowedStates: Array<TaskInstanceState> = ["success", "failed"];

  const { t: translate } = useTranslation();

  if (patchKeys.length === 0) {
    return undefined;
  }

  const type = translate("common:taskInstance_other");
  const patchButtonText = translate("dags:runAndTaskActions.markAs.button", { type });

  return (
    <>
      <Menu.Root positioning={{ gutter: 0, placement: "bottom" }}>
        <Menu.Trigger asChild>
          <ActionButton
            actionName={patchButtonText}
            colorPalette="blue"
            flexDirection="row-reverse"
            icon={<MdArrowDropDown />}
            text={patchButtonText}
            variant="outline"
            withText
          />
        </Menu.Trigger>
        <Menu.Content>
          {allowedStates.map((menuState) => {
            const content = translate(
              `dags:runAndTaskActions.markAs.buttonTooltip.${menuState === "success" ? "success" : "failed"}`,
            );

            return (
              <Tooltip closeDelay={100} content={content} key={menuState} openDelay={100}>
                <Menu.Item
                  asChild
                  key={menuState}
                  onClick={() => {
                    setSelectedState(menuState);
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
      <PatchTaskInstancesDialog
        clearSelections={clearSelections}
        dagId={dagId}
        dagRunId={dagRunId}
        onClose={onClose}
        open={open}
        patchKeys={patchKeys}
        selectedState={selectedState}
      />
    </>
  );
};

export default MarkTaskInstancesAsButton;

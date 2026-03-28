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
import { IconButton, useDisclosure } from "@chakra-ui/react";
import { useHotkeys } from "react-hotkeys-hook";
import { useTranslation } from "react-i18next";
import { CgRedo } from "react-icons/cg";

import type { LightGridTaskInstanceSummary, TaskInstanceResponse } from "openapi/requests/types.gen";
import { Tooltip } from "src/components/ui";

import ClearTaskInstanceDialog from "./ClearTaskInstanceDialog";

type Props = {
  readonly allMapped?: boolean;
  readonly dagId: string;
  readonly dagRunId: string;
  readonly isHotkeyEnabled?: boolean;
  readonly mapIndex?: number;
  readonly onOpen?: (ti: LightGridTaskInstanceSummary | TaskInstanceResponse) => void;
  readonly taskId: string;
  readonly taskInstance?: LightGridTaskInstanceSummary | TaskInstanceResponse;
};

const ClearTaskInstanceButton = ({
  allMapped = false,
  dagId,
  dagRunId,
  isHotkeyEnabled = false,
  mapIndex,
  onOpen,
  taskId,
  taskInstance,
}: Props) => {
  const { onClose, onOpen: onOpenInternal, open } = useDisclosure();
  const { t: translate } = useTranslation();
  const useInternalDialog = !Boolean(onOpen);

  useHotkeys(
    "shift+c",
    () => {
      if (onOpen && taskInstance) {
        onOpen(taskInstance);
      } else {
        onOpenInternal();
      }
    },
    { enabled: isHotkeyEnabled },
  );

  return (
    <>
      <Tooltip
        closeDelay={100}
        content={
          isHotkeyEnabled
            ? translate(`dags:runAndTaskActions.${allMapped ? "clearAllMapped" : "clear"}.buttonTooltip`)
            : translate(`dags:runAndTaskActions.${allMapped ? "clearAllMapped" : "clear"}.button`, {
                type: translate("taskInstance_one"),
              })
        }
        openDelay={100}
      >
        <IconButton
          aria-label={translate(`dags:runAndTaskActions.${allMapped ? "clearAllMapped" : "clear"}.button`, {
            type: translate("taskInstance_one"),
          })}
          colorPalette="brand"
          onClick={() => (onOpen && taskInstance ? onOpen(taskInstance) : onOpenInternal())}
          size="md"
          variant="ghost"
        >
          <CgRedo />
        </IconButton>
      </Tooltip>

      {useInternalDialog && open ? (
        <ClearTaskInstanceDialog
          allMapped={allMapped}
          dagId={dagId}
          dagRunId={dagRunId}
          mapIndex={mapIndex}
          onClose={onClose}
          open={open}
          taskId={taskId}
        />
      ) : undefined}
    </>
  );
};

export default ClearTaskInstanceButton;

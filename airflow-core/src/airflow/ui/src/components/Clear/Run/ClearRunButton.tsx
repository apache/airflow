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

import type { DAGRunResponse } from "openapi/requests/types.gen";
import { Tooltip } from "src/components/ui";

import ClearRunDialog from "./ClearRunDialog";

type Props = {
  readonly dagRun: DAGRunResponse;
  readonly isHotkeyEnabled?: boolean;
};

const ClearRunButton = ({ dagRun, isHotkeyEnabled = false }: Props) => {
  const { onClose, onOpen, open } = useDisclosure();
  const { t: translate } = useTranslation();

  useHotkeys(
    "shift+c",
    () => {
      onOpen();
    },
    { enabled: isHotkeyEnabled },
  );

  return (
    <>
      <Tooltip
        closeDelay={100}
        content={
          isHotkeyEnabled
            ? translate("dags:runAndTaskActions.clear.buttonTooltip")
            : translate("dags:runAndTaskActions.clear.button", { type: translate("dagRun_one") })
        }
        openDelay={100}
      >
        <IconButton
          aria-label={translate("dags:runAndTaskActions.clear.button", { type: translate("dagRun_one") })}
          colorPalette="brand"
          onClick={onOpen}
          size="md"
          variant="ghost"
        >
          <CgRedo />
        </IconButton>
      </Tooltip>

      {open ? <ClearRunDialog dagRun={dagRun} onClose={onClose} open={open} /> : undefined}
    </>
  );
};

export default ClearRunButton;

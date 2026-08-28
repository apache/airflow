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
import { useTranslation } from "react-i18next";

import { useConfig } from "src/queries/useConfig";
import { useTogglePause } from "src/queries/useTogglePause";

import { ConfirmationModal } from "./ConfirmationModal";
import { Switch, Tooltip, type SwitchProps } from "./ui";

type Props = {
  readonly dagDisplayName?: string;
  readonly dagId: string;
  readonly isPaused?: boolean;
  readonly skipConfirm?: boolean;
} & SwitchProps;

export const TogglePause = ({ dagDisplayName, dagId, isPaused, skipConfirm, ...rest }: Props) => {
  const { onClose, onOpen, open } = useDisclosure();
  const { t: translate } = useTranslation();
  const { mutate: togglePause } = useTogglePause({ dagId });
  const showConfirmation = Boolean(useConfig("require_confirmation_dag_change"));

  const onToggle = () =>
    togglePause({
      dagId,
      requestBody: {
        is_paused: !isPaused,
      },
    });

  const onChange = () => (showConfirmation && skipConfirm !== true ? onOpen() : onToggle());

  const label = `${isPaused ? translate("common:unpause") : translate("common:pause")} ${dagDisplayName ?? dagId}`;

  return (
    <>
      <Tooltip content={label}>
        <Switch
          checked={isPaused === undefined ? undefined : !isPaused}
          data-testid="toggle-pause"
          onCheckedChange={onChange}
          {...rest}
        />
      </Tooltip>
      <ConfirmationModal header={`${label}?`} onConfirm={onToggle} onOpenChange={onClose} open={open} />
    </>
  );
};

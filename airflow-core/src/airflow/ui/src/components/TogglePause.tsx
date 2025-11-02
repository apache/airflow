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
import { useCallback } from "react";

import { useConfig } from "src/queries/useConfig";
import { useTogglePause } from "src/queries/useTogglePause";

import { ConfirmationModal } from "./ConfirmationModal";
import { Switch, type SwitchProps } from "./ui";

type Props = {
  readonly dagDisplayName?: string;
  readonly dagId: string;
  readonly isPaused?: boolean;
  readonly skipConfirm?: boolean;
} & SwitchProps;

export const TogglePause = ({ dagDisplayName, dagId, isPaused, skipConfirm, ...rest }: Props) => {
  const { onClose, onOpen, open } = useDisclosure();

  const { mutate: togglePause } = useTogglePause({ dagId });
  const showConfirmation = Boolean(useConfig("require_confirmation_dag_change"));

  const onToggle = useCallback(() => {
    togglePause({
      dagId,
      requestBody: {
        is_paused: !isPaused,
      },
    });
  }, [dagId, isPaused, togglePause]);

  const onChange = () => {
    if (showConfirmation && skipConfirm !== true) {
      onOpen();
    } else {
      onToggle();
    }
  };

  return (
    <>
      <Switch
        checked={isPaused === undefined ? undefined : !isPaused}
        colorPalette="brand"
        onCheckedChange={onChange}
        size="sm"
        {...rest}
      />
      <ConfirmationModal
        header={`${isPaused ? "Unpause" : "Pause"} ${dagDisplayName ?? dagId}?`}
        onConfirm={onToggle}
        onOpenChange={onClose}
        open={open}
      />
    </>
  );
};

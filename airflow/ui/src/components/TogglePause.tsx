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
import { useQueryClient } from "@tanstack/react-query";
import { useCallback } from "react";

import {
  UseDagServiceGetDagDetailsKeyFn,
  useDagServiceGetDagsKey,
  useDagServicePatchDag,
} from "openapi/queries";
import { useConfig } from "src/queries/useConfig";

import { ConfirmationModal } from "./ConfirmationModal";
import { Switch } from "./ui";

type Props = {
  readonly dagDisplayName?: string;
  readonly dagId: string;
  readonly isPaused: boolean;
  readonly skipConfirm?: boolean;
};

export const TogglePause = ({
  dagDisplayName,
  dagId,
  isPaused,
  skipConfirm,
}: Props) => {
  const queryClient = useQueryClient();
  const { onClose, onOpen, open } = useDisclosure();

  const onSuccess = async () => {
    await queryClient.invalidateQueries({
      queryKey: [useDagServiceGetDagsKey],
    });

    await queryClient.invalidateQueries({
      queryKey: UseDagServiceGetDagDetailsKeyFn({ dagId }),
    });
  };

  const { mutate } = useDagServicePatchDag({
    onSuccess,
  });

  const showConfirmation = Boolean(
    useConfig("require_confirmation_dag_change"),
  );

  const onToggle = useCallback(() => {
    mutate({
      dagId,
      requestBody: {
        is_paused: !isPaused,
      },
    });
  }, [dagId, isPaused, mutate]);

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
        checked={!isPaused}
        colorPalette="blue"
        onCheckedChange={onChange}
        size="sm"
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

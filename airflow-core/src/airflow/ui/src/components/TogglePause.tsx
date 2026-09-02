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
import type { ComponentProps } from "react";
import { useState } from "react";

import { Spinner, useDisclosure } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { MdHourglassTop, MdPause, MdPlayArrow } from "react-icons/md";

import type { DagSchedulingState } from "openapi/requests/types.gen";

import { IconButton, Menu } from "src/system-components";

import { useConfig } from "src/queries/useConfig";
import { useTogglePause } from "src/queries/useTogglePause";

import { ConfirmationModal } from "./ConfirmationModal";

type Props = {
  readonly dagDisplayName?: string;
  readonly dagId: string;
  readonly isPaused?: boolean;
  readonly schedulingState?: DagSchedulingState;
  readonly skipConfirm?: boolean;
} & Omit<ComponentProps<typeof IconButton>, "children" | "label">;

export const TogglePause = ({
  dagDisplayName,
  dagId,
  disabled,
  isPaused,
  schedulingState,
  skipConfirm,
  ...rest
}: Props) => {
  const { onClose, onOpen, open } = useDisclosure();
  const { t: translate } = useTranslation(["common", "dags"]);
  const { isPending, mutate: togglePause } = useTogglePause({ dagId });
  const showConfirmation = Boolean(useConfig("require_confirmation_dag_change"));
  const [pendingState, setPendingState] = useState<DagSchedulingState>();
  const state = schedulingState ?? (isPaused === true ? "paused" : "active");
  const displayName = dagDisplayName ?? dagId;

  const setSchedulingState = (nextState: DagSchedulingState) =>
    togglePause({
      dagId,
      requestBody: {
        scheduling_state: nextState,
      },
    });

  const requestStateChange = (nextState: DagSchedulingState) => {
    if (showConfirmation && skipConfirm !== true) {
      setPendingState(nextState);
      onOpen();
    } else {
      setSchedulingState(nextState);
    }
  };

  const actionLabel =
    pendingState === "draining"
      ? translate("dags:schedulingActions.drain")
      : pendingState === "paused"
        ? translate("dags:schedulingActions.pauseNow")
        : translate(
            state === "draining" ? "dags:schedulingActions.cancelDrain" : "dags:schedulingActions.unpause",
          );
  const stateLabel = translate(`dags:schedulingState.${state}`);

  return (
    <>
      <Menu.Root tooltipLabel={`${stateLabel}: ${displayName}`}>
        <Menu.Trigger asChild>
          <IconButton
            aria-label={`${stateLabel}: ${displayName}`}
            colorPalette={state === "active" ? "green" : state === "draining" ? "orange" : "gray"}
            data-testid="toggle-pause"
            {...rest}
            disabled={disabled === true || isPending}
          >
            {state === "active" ? (
              <MdPlayArrow />
            ) : state === "draining" ? (
              <Spinner size="xs" />
            ) : (
              <MdPause />
            )}
          </IconButton>
        </Menu.Trigger>
        <Menu.Content>
          {state === "active" ? (
            <Menu.Item data-testid="drain-dag" onClick={() => requestStateChange("draining")} value="drain">
              <MdHourglassTop />
              {translate("dags:schedulingActions.drain")}
            </Menu.Item>
          ) : (
            <Menu.Item
              data-testid="activate-dag"
              onClick={() => requestStateChange("active")}
              value="activate"
            >
              <MdPlayArrow />
              {translate(
                state === "draining"
                  ? "dags:schedulingActions.cancelDrain"
                  : "dags:schedulingActions.unpause",
              )}
            </Menu.Item>
          )}
          {state === "paused" ? undefined : (
            <Menu.Item data-testid="pause-dag-now" onClick={() => requestStateChange("paused")} value="pause">
              <MdPause />
              {translate("dags:schedulingActions.pauseNow")}
            </Menu.Item>
          )}
        </Menu.Content>
      </Menu.Root>
      <ConfirmationModal
        header={`${actionLabel} ${displayName}?`}
        onConfirm={() => {
          if (pendingState !== undefined) {
            setSchedulingState(pendingState);
          }
        }}
        onOpenChange={() => {
          setPendingState(undefined);
          onClose();
        }}
        open={open}
      />
    </>
  );
};

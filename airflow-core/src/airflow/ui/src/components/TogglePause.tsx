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
import { useState } from "react";

import { Button, useDisclosure } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { MdHourglassTop } from "react-icons/md";

import type { DagSchedulingState } from "openapi/requests/types.gen";

import { Modal, Switch, Tooltip, type SwitchProps } from "src/system-components";

import { useConfig } from "src/queries/useConfig";
import { useTogglePause } from "src/queries/useTogglePause";

import { ConfirmationModal } from "./ConfirmationModal";

type Props = {
  readonly dagDisplayName?: string;
  readonly dagId: string;
  /** Omitting this offers the drain choice unconditionally, since the two are
   * only distinguishable once it is known whether anything is still running. */
  readonly hasUnfinishedRuns?: boolean;
  readonly isPaused?: boolean;
  readonly schedulingState?: DagSchedulingState;
  readonly skipConfirm?: boolean;
} & SwitchProps;

export const TogglePause = ({
  dagDisplayName,
  dagId,
  disabled,
  hasUnfinishedRuns,
  isPaused,
  schedulingState,
  skipConfirm,
  ...rest
}: Props) => {
  const { onClose, onOpen, open } = useDisclosure();
  const { onClose: onChoiceClose, onOpen: onChoiceOpen, open: choiceOpen } = useDisclosure();
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

  const handleCheckedChange = () => {
    if (state === "draining") {
      setSchedulingState("active");
    } else if (state === "paused") {
      requestStateChange("active");
    } else if (hasUnfinishedRuns === false) {
      // Nothing is running, so draining and pausing are equivalent — skip the choice.
      requestStateChange("paused");
    } else {
      // Unknown or unfinished runs: let the user decide between draining and pausing now.
      onChoiceOpen();
    }
  };

  const label =
    state === "draining"
      ? translate("dags:schedulingState.drainingTooltip", { dagDisplayName: displayName })
      : `${translate(state === "paused" ? "common:unpause" : "common:pause")} ${displayName}`;
  const actionLabel = pendingState === "paused" ? translate("common:pause") : translate("common:unpause");

  return (
    <>
      <Tooltip content={label}>
        <Switch
          checked={state === "active"}
          colorPalette={state === "draining" ? "orange" : undefined}
          data-testid="toggle-pause"
          {...rest}
          disabled={disabled === true || isPending}
          onCheckedChange={handleCheckedChange}
          thumbLabel={state === "draining" ? { off: <MdHourglassTop />, on: <MdHourglassTop /> } : undefined}
        />
      </Tooltip>
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
      <Modal
        footerActions={
          <>
            <Button
              data-testid="drain-dag"
              onClick={() => {
                setSchedulingState("draining");
                onChoiceClose();
              }}
            >
              {translate("dags:schedulingActions.drain")}
            </Button>
            <Button
              data-testid="pause-dag-now"
              onClick={() => {
                setSchedulingState("paused");
                onChoiceClose();
              }}
              variant="outline"
            >
              {translate("dags:schedulingActions.pauseNow")}
            </Button>
          </>
        }
        onOpenChange={onChoiceClose}
        open={choiceOpen}
        title={`${translate("common:pause")} ${displayName}?`}
      >
        {translate("dags:schedulingActions.drainPrompt")}
      </Modal>
    </>
  );
};

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

import { Button, ButtonGroup, useDisclosure } from "@chakra-ui/react";
import type { DagRunType } from "openapi-gen/requests/types.gen";
import { useTranslation } from "react-i18next";
import { FiChevronDown, FiPlay } from "react-icons/fi";
import { useParams } from "react-router-dom";

import { useDagRunServiceGetDagRun } from "openapi/queries";

import { IconButton, Menu, Tooltip } from "src/system-components";

import TriggerDAGModal from "./TriggerDAGModal";

type TriggerDAGButtonProps = {
  readonly allowedRunTypes?: Array<DagRunType> | null;
  readonly dagDisplayName: string;
  readonly dagId: string;
  readonly isPaused: boolean;
  readonly variant?: "ghost" | "outline";
  readonly withText?: boolean;
};

export const TriggerDAGButton = ({
  allowedRunTypes,
  dagDisplayName,
  dagId,
  isPaused,
  variant = "ghost",
  withText = false,
}: TriggerDAGButtonProps) => {
  const isManualRunDenied =
    allowedRunTypes !== null && allowedRunTypes !== undefined && !allowedRunTypes.includes("manual");
  const { onClose, onOpen, open } = useDisclosure();
  const { t: translate } = useTranslation("components");
  const { runId } = useParams();
  const [prefillConfig, setPrefillConfig] = useState<
    | {
        conf: Record<string, unknown> | undefined;
        logicalDate: string | undefined;
        runId: string;
      }
    | undefined
  >(undefined);

  // Check if there's a selected DAG Run
  const { data: selectedDagRun } = useDagRunServiceGetDagRun(
    {
      dagId,
      dagRunId: runId ?? "",
    },
    undefined,
    { enabled: Boolean(dagId) && Boolean(runId) },
  );

  const handleTriggerWithConfig = () => {
    if (selectedDagRun) {
      setPrefillConfig({
        conf: selectedDagRun.conf ?? undefined,
        logicalDate: selectedDagRun.logical_date ?? undefined,
        runId: selectedDagRun.dag_run_id,
      });
      onOpen();
    }
  };

  const handleNormalTrigger = () => {
    setPrefillConfig(undefined);
    onOpen();
  };

  const handleModalClose = () => {
    setPrefillConfig(undefined);
    onClose();
  };

  const triggerOptionsLabel = isManualRunDenied
    ? translate("triggerDag.manualRunDenied")
    : translate("triggerDag.triggerOptions");

  return (
    <>
      <ButtonGroup attached variant={variant}>
        <Tooltip
          content={
            isManualRunDenied ? translate("triggerDag.manualRunDenied") : translate("triggerDag.button")
          }
          disabled={withText ? !isManualRunDenied : undefined}
        >
          {withText ? (
            <Button
              aria-label={translate("triggerDag.title")}
              data-testid="trigger-dag-button"
              disabled={isManualRunDenied}
              onClick={handleNormalTrigger}
              variant={variant}
            >
              <FiPlay />
              {translate("triggerDag.button")}
            </Button>
          ) : (
            <IconButton
              aria-label={translate("triggerDag.title")}
              data-testid="trigger-dag-button"
              disabled={isManualRunDenied}
              onClick={handleNormalTrigger}
              variant={variant}
            >
              <FiPlay />
            </IconButton>
          )}
        </Tooltip>

        <Menu.Root tooltipLabel={triggerOptionsLabel}>
          <Menu.Trigger asChild>
            <IconButton
              aria-label={translate("triggerDag.triggerOptions")}
              data-testid="trigger-dag-options-button"
              disabled={isManualRunDenied}
              variant={variant}
            >
              <FiChevronDown />
            </IconButton>
          </Menu.Trigger>
          <Menu.Content>
            <Menu.Item onClick={handleNormalTrigger} value="triggerWithConfig">
              {translate("triggerDag.triggerWithConfig")}
            </Menu.Item>
            {selectedDagRun?.conf !== undefined && (
              <Menu.Item onClick={handleTriggerWithConfig} value="triggerAgainWithConfig">
                {translate("triggerDag.triggerAgainWithConfig")}
              </Menu.Item>
            )}
          </Menu.Content>
        </Menu.Root>
      </ButtonGroup>

      <TriggerDAGModal
        dagDisplayName={dagDisplayName}
        dagId={dagId}
        isPaused={isPaused}
        onClose={handleModalClose}
        open={open}
        prefillConfig={prefillConfig}
      />
    </>
  );
};

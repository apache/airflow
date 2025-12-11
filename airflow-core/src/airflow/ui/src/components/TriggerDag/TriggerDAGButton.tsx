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
import { Box } from "@chakra-ui/react";
import { useDisclosure } from "@chakra-ui/react";
import React from "react";
import { useTranslation } from "react-i18next";
import { FiPlay } from "react-icons/fi";
import { useParams } from "react-router-dom";

import { useDagRunServiceGetDagRun } from "openapi/queries";
import { Menu } from "src/components/ui";

import ActionButton from "../ui/ActionButton";
import TriggerDAGModal from "./TriggerDAGModal";

type Props = {
  readonly dagDisplayName: string;
  readonly dagId: string;
  readonly isPaused: boolean;
  readonly withText?: boolean;
};

const TriggerDAGButton: React.FC<Props> = ({ dagDisplayName, dagId, isPaused, withText = true }) => {
  const { onClose, onOpen, open } = useDisclosure();
  const { t: translate } = useTranslation("components");
  const { runId } = useParams();
  const [prefillConfig, setPrefillConfig] = React.useState<
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

  // If there's a selected DAG Run with config, show menu with options
  if (selectedDagRun?.conf !== undefined) {
    return (
      <Box>
        <Menu.Root>
          <Menu.Trigger asChild>
            <div>
              <ActionButton
                actionName={translate("triggerDag.title")}
                icon={<FiPlay />}
                text={translate("triggerDag.button")}
                variant="outline"
                withText={withText}
              />
            </div>
          </Menu.Trigger>
          <Menu.Content>
            <Menu.Item onClick={handleNormalTrigger} value="trigger">
              {translate("triggerDag.button")}
            </Menu.Item>
            <Menu.Item onClick={handleTriggerWithConfig} value="triggerWithConfig">
              {translate("triggerDag.triggerAgainWithConfig")}
            </Menu.Item>
          </Menu.Content>
        </Menu.Root>

        <TriggerDAGModal
          dagDisplayName={dagDisplayName}
          dagId={dagId}
          isPaused={isPaused}
          onClose={handleModalClose}
          open={open}
          prefillConfig={prefillConfig}
        />
      </Box>
    );
  }

  // Normal trigger button without menu
  return (
    <Box>
      <ActionButton
        actionName={translate("triggerDag.title")}
        icon={<FiPlay />}
        onClick={handleNormalTrigger}
        text={translate("triggerDag.button")}
        variant="outline"
        withText={withText}
      />

      <TriggerDAGModal
        dagDisplayName={dagDisplayName}
        dagId={dagId}
        isPaused={isPaused}
        onClose={handleModalClose}
        open={open}
        prefillConfig={undefined}
      />
    </Box>
  );
};

export default TriggerDAGButton;

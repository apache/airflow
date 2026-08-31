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
import { Badge, Box, Button, HStack, useDisclosure } from "@chakra-ui/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FiX } from "react-icons/fi";
import { LuCheck } from "react-icons/lu";

import type { DagRunMutableStates, DAGRunResponse } from "openapi/requests/types.gen";
import { ActionAccordion } from "src/components/ActionAccordion";
import { ActionErrors } from "src/components/ActionErrors";
import { allowedStates } from "src/components/MarkAs/utils";
import { StateBadge } from "src/components/StateBadge";
import { useBulkPatchDagRun } from "src/queries/useBulkPatchDagRun";
import { Modal, Menu } from "src/system-components";

type Props = {
  readonly deselectKeys: (keys: Array<string>) => void;
  readonly selectedDagRuns: Array<DAGRunResponse>;
};

const BulkMarkDagRunsAsButton = ({ deselectKeys, selectedDagRuns }: Props) => {
  const { t: translate } = useTranslation(["common", "dags"]);
  const { onClose, onOpen, open } = useDisclosure();
  const [state, setState] = useState<DagRunMutableStates>("success");
  const [note, setNote] = useState<string | null>(null);
  const { bulkAction, data, error, isPending, reset } = useBulkPatchDagRun({
    deselectKeys,
    onSuccessConfirm: onClose,
  });

  const handleOpen = (newState: DagRunMutableStates) => {
    setState(newState);
    setNote(null);
    reset();
    onOpen();
  };

  return (
    <Box>
      <Menu.Root positioning={{ gutter: 0, placement: "top" }}>
        <Menu.Trigger asChild>
          <Button variant="outline">
            <HStack gap={1} mx={1}>
              <LuCheck />
              <span>/</span>
              <FiX />
            </HStack>
            {translate("dags:runAndTaskActions.markAs.button", { type: translate("dagRun_other") })}
          </Button>
        </Menu.Trigger>
        <Menu.Content>
          {allowedStates.map((menuState) => (
            <Menu.Item key={menuState} onClick={() => handleOpen(menuState)} value={menuState}>
              <HStack justify="space-between" width="full">
                <StateBadge state={menuState}>{translate(`common:states.${menuState}`)}</StateBadge>
                <Badge colorPalette="gray" variant="subtle">
                  {selectedDagRuns.length}
                </Badge>
              </HStack>
            </Menu.Item>
          ))}
        </Menu.Content>
      </Menu.Root>

      <Modal
        footerActions={
          <Button
            loading={isPending}
            onClick={() => {
              bulkAction({
                actions: [
                  {
                    action: "update" as const,
                    action_on_non_existence: "skip",
                    entities: selectedDagRuns.map((dagRun) => ({
                      dag_id: dagRun.dag_id,
                      dag_run_id: dagRun.dag_run_id,
                      note,
                      state,
                    })),
                  },
                ],
              });
            }}
          >
            {translate("modal.confirm")}
          </Button>
        }
        onOpenChange={onClose}
        open={open}
        title={
          <>
            {translate("dags:runAndTaskActions.markAs.title", {
              state,
              type: translate("dagRun_other"),
            })}{" "}
            <StateBadge state={state} />
          </>
        }
      >
        <ActionAccordion note={note} setNote={setNote} />
        <ActionErrors actionResponse={data?.update} error={error} />
      </Modal>
    </Box>
  );
};

export default BulkMarkDagRunsAsButton;

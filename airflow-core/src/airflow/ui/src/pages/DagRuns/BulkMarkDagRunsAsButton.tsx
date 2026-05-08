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
import { Badge, Box, Button, Flex, Heading, HStack, useDisclosure, VStack } from "@chakra-ui/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FiX } from "react-icons/fi";
import { LuCheck } from "react-icons/lu";

import type { DAGRunPatchStates, DAGRunResponse } from "openapi/requests/types.gen";
import { ActionAccordion } from "src/components/ActionAccordion";
import { ErrorAlert } from "src/components/ErrorAlert";
import { allowedStates } from "src/components/MarkAs/utils";
import { StateBadge } from "src/components/StateBadge";
import { Dialog, Menu } from "src/components/ui";
import { useBulkDagRuns } from "src/queries/useBulkDagRuns";

type Props = {
  readonly clearSelections: VoidFunction;
  readonly selectedDagRuns: Array<DAGRunResponse>;
};

const BulkMarkDagRunsAsButton = ({ clearSelections, selectedDagRuns }: Props) => {
  const { t: translate } = useTranslation();
  const { onClose, onOpen, open } = useDisclosure();
  const [state, setState] = useState<DAGRunPatchStates>("success");
  const [note, setNote] = useState<string | null>(null);
  const { bulkMarkAs, error, isPending, setError } = useBulkDagRuns({
    clearSelections,
    onSuccessConfirm: onClose,
  });

  const affectedCount = (targetState: DAGRunPatchStates) =>
    selectedDagRuns.filter((dr) => dr.state !== targetState).length;

  const handleOpen = (newState: DAGRunPatchStates) => {
    setState(newState);
    setNote(null);
    setError(undefined);
    onOpen();
  };

  const directlyAffected = selectedDagRuns.filter((dr) => dr.state !== state);

  return (
    <Box>
      <Menu.Root positioning={{ gutter: 0, placement: "top" }}>
        <Menu.Trigger asChild>
          <div>
            <Button colorPalette="brand" size="sm" variant="outline">
              <HStack gap={1} mx={1}>
                <LuCheck />
                <span>/</span>
                <FiX />
              </HStack>
              {translate("dags:runAndTaskActions.markAs.button", { type: translate("dagRun_other") })}
            </Button>
          </div>
        </Menu.Trigger>
        <Menu.Content>
          {allowedStates.map((menuState) => {
            const count = affectedCount(menuState);

            return (
              <Menu.Item
                disabled={count === 0}
                key={menuState}
                onClick={() => {
                  if (count > 0) {
                    handleOpen(menuState);
                  }
                }}
                value={menuState}
              >
                <HStack justify="space-between" width="full">
                  <StateBadge state={menuState}>{translate(`common:states.${menuState}`)}</StateBadge>
                  <Badge colorPalette="gray" variant="subtle">
                    {count}
                  </Badge>
                </HStack>
              </Menu.Item>
            );
          })}
        </Menu.Content>
      </Menu.Root>

      <Dialog.Root onOpenChange={onClose} open={open} size="xl">
        <Dialog.Content backdrop>
          <Dialog.Header>
            <VStack align="start" gap={4}>
              <Heading size="xl">
                {translate("dags:runAndTaskActions.markAs.title", {
                  state,
                  type: translate("dagRun_other"),
                })}{" "}
                <StateBadge state={state} />
              </Heading>
            </VStack>
          </Dialog.Header>

          <Dialog.CloseTrigger />
          <Dialog.Body width="full">
            <ActionAccordion note={note} setNote={setNote} />
            <ErrorAlert error={error} />
            <Flex justifyContent="end" mt={3}>
              <Button
                colorPalette="brand"
                disabled={directlyAffected.length === 0}
                loading={isPending}
                onClick={() => {
                  void bulkMarkAs(directlyAffected, { note, state });
                }}
              >
                {translate("modal.confirm")}
              </Button>
            </Flex>
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
    </Box>
  );
};

export default BulkMarkDagRunsAsButton;

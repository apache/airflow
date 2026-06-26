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
import { Box, Button, Flex, Heading, Text, VStack } from "@chakra-ui/react";
import { Fragment, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { FiChevronDown } from "react-icons/fi";

import type { DagRunMutableStates, DAGRunResponse } from "openapi/requests/types.gen";
import { ActionAccordion } from "src/components/ActionAccordion";
import { StateBadge } from "src/components/StateBadge";
import { Dialog, Menu } from "src/components/ui";
import { usePatchDagRun } from "src/queries/usePatchDagRun";

type Props = {
  readonly dagRun: DAGRunResponse;
  readonly onClose: () => void;
  readonly open: boolean;
  readonly state: DagRunMutableStates;
};

const variant = (overwrite: boolean) => (overwrite ? "withOverwrite" : "withoutOverwrite");

const MarkRunAsDialog = ({ dagRun, onClose, open, state }: Props) => {
  const dagId = dagRun.dag_id;
  const dagRunId = dagRun.dag_run_id;
  const { t: translate } = useTranslation();

  const [note, setNote] = useState<string | null>(dagRun.note);
  const [overwrite, setOverwrite] = useState(true);

  useEffect(() => {
    if (open) {
      setNote(dagRun.note);
    }
  }, [dagRun.note, open]);

  const handleClose = () => {
    setNote(dagRun.note);
    onClose();
  };
  const { isPending, mutate } = usePatchDagRun({ dagId, dagRunId, onSuccess: handleClose });

  const confirm = () => {
    mutate({ dagId, dagRunId, requestBody: { note, overwrite, state } });
  };

  return (
    <Dialog.Root
      lazyMount
      onOpenChange={(details) => {
        if (!details.open) {
          handleClose();
        }
      }}
      open={open}
    >
      <Dialog.Content backdrop>
        <Dialog.Header>
          <VStack align="start" gap={4}>
            <Heading size="xl">
              {translate("dags:runAndTaskActions.markAs.title", {
                state,
                type: translate("dagRun_one"),
              })}
              : {dagRunId} <StateBadge state={state} />
            </Heading>
          </VStack>
        </Dialog.Header>

        <Dialog.CloseTrigger />

        <Dialog.Body width="full">
          <ActionAccordion note={note} setNote={setNote} />
          <Flex justifyContent="end" mt={3}>
            <Button
              borderRightRadius={0}
              data-testid="mark-run-as-confirm"
              loading={isPending}
              onClick={confirm}
            >
              {translate(`dags:runAndTaskActions.markAs.overwrite.${variant(overwrite)}.title`)}
            </Button>
            <Menu.Root positioning={{ placement: "bottom-end" }}>
              <Menu.Trigger asChild>
                <Button
                  aria-label={translate("dags:runAndTaskActions.markAs.overwrite.menuLabel")}
                  borderLeftRadius={0}
                  borderLeftWidth={1}
                  data-testid="mark-run-as-options"
                  loading={isPending}
                  px={2}
                >
                  <FiChevronDown />
                </Button>
              </Menu.Trigger>
              <Menu.Content minW="xs">
                <Menu.RadioItemGroup
                  onValueChange={({ value }) => setOverwrite(value === "true")}
                  value={String(overwrite)}
                >
                  {[true, false].map((value, index) => (
                    <Fragment key={String(value)}>
                      {index > 0 ? <Menu.Separator /> : undefined}
                      <Menu.RadioItem
                        alignItems="start"
                        data-testid={`mark-run-as-overwrite-${String(value)}`}
                        value={String(value)}
                      >
                        <Box minW={5} pt={1}>
                          <Menu.ItemIndicator />
                        </Box>
                        <VStack align="start" gap={0}>
                          <Text fontWeight="bold">
                            {translate(`dags:runAndTaskActions.markAs.overwrite.${variant(value)}.title`)}
                          </Text>
                          <Text color="fg.muted" fontSize="sm">
                            {translate(
                              `dags:runAndTaskActions.markAs.overwrite.${variant(value)}.description`,
                            )}
                          </Text>
                        </VStack>
                      </Menu.RadioItem>
                    </Fragment>
                  ))}
                </Menu.RadioItemGroup>
              </Menu.Content>
            </Menu.Root>
          </Flex>
        </Dialog.Body>
      </Dialog.Content>
    </Dialog.Root>
  );
};

export default MarkRunAsDialog;

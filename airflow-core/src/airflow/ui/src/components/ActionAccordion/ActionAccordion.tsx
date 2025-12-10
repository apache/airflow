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
import { Accordion, Box, Editable, Text, Span, VStack } from "@chakra-ui/react";
import type { ChangeEvent } from "react";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";

import type { DAGRunResponse, TaskInstanceCollectionResponse } from "openapi/requests/types.gen";
import ReactMarkdown from "src/components/ReactMarkdown";

import { DataTable } from "../DataTable";
import { getColumns } from "./columns";

type Props = {
  readonly affectedTasks?: TaskInstanceCollectionResponse;
  readonly note: DAGRunResponse["note"];
  readonly setNote: (value: string) => void;
};

// Table is in memory, pagination and sorting are disabled.
// TODO: Make a front-end only unconnected table component with client side ordering and pagination
const ActionAccordion = ({ affectedTasks, note, setNote }: Props) => {
  const showTaskSection = affectedTasks !== undefined;
  const { t: translate } = useTranslation();

  const columns = useMemo(() => getColumns(translate), [translate]);

  return (
    <Accordion.Root
      collapsible
      defaultValue={showTaskSection ? ["tasks"] : ["note"]}
      multiple={false}
      variant="enclosed"
    >
      {showTaskSection ? (
        <Accordion.Item key="tasks" value="tasks">
          <Accordion.ItemTrigger>
            <Span flex="1">
              {translate("dags:runAndTaskActions.affectedTasks.title", {
                count: affectedTasks.total_entries,
              })}
            </Span>
            <Accordion.ItemIndicator />
          </Accordion.ItemTrigger>
          <Accordion.ItemContent>
            <Accordion.ItemBody>
              <Box maxH="400px" overflowY="scroll">
                <DataTable
                  columns={columns}
                  data={affectedTasks.task_instances}
                  displayMode="table"
                  modelName={translate("common:taskInstance_other")}
                  noRowsMessage={translate("dags:runAndTaskActions.affectedTasks.noItemsFound")}
                  total={affectedTasks.total_entries}
                />
              </Box>
            </Accordion.ItemBody>
          </Accordion.ItemContent>
        </Accordion.Item>
      ) : undefined}
      <Accordion.Item key="note" value="note">
        <Accordion.ItemTrigger>
          <Span flex="1">{translate("note.label")}</Span>
          <Accordion.ItemIndicator />
        </Accordion.ItemTrigger>
        <Accordion.ItemContent>
          <Accordion.ItemBody>
            <Editable.Root
              onChange={(event: ChangeEvent<HTMLInputElement>) => setNote(event.target.value)}
              value={note ?? ""}
            >
              <Editable.Preview
                _hover={{ backgroundColor: "transparent" }}
                alignItems="flex-start"
                as={VStack}
                gap="0"
                height="200px"
                overflowY="auto"
                width="100%"
              >
                {Boolean(note) ? (
                  <ReactMarkdown>{note}</ReactMarkdown>
                ) : (
                  <Text color="fg.subtle">{translate("note.placeholder")}</Text>
                )}
              </Editable.Preview>
              <Editable.Textarea
                data-testid="notes-input"
                height="200px"
                overflowY="auto"
                placeholder={translate("note.placeholder")}
                resize="none"
              />
            </Editable.Root>
          </Accordion.ItemBody>
        </Accordion.ItemContent>
      </Accordion.Item>
    </Accordion.Root>
  );
};

export default ActionAccordion;

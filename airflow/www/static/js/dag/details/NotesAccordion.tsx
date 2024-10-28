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

import React, { useState, useRef } from "react";
import {
  Accordion,
  AccordionItem,
  AccordionButton,
  AccordionPanel,
  AccordionIcon,
  Box,
  Button,
  Flex,
  Text,
  Textarea,
  Divider,
} from "@chakra-ui/react";
import ResizeTextarea from "react-textarea-autosize";

import { getMetaValue } from "src/utils";
import { useSetDagRunNote, useSetTaskInstanceNote } from "src/api";
import { MdEdit } from "react-icons/md";
import ReactMarkdown from "src/components/ReactMarkdown";
import { useKeysPress, isInputInFocus } from "src/utils/useKeysPress";
import keyboardShortcutIdentifier from "src/dag/keyboardShortcutIdentifier";

interface Props {
  dagId: string;
  runId: string;
  taskId?: string;
  mapIndex?: number;
  initialValue?: string | null;
  isAbandonedTask?: boolean;
}

const NotesAccordion = ({
  dagId,
  runId,
  taskId,
  mapIndex,
  initialValue,
  isAbandonedTask,
}: Props) => {
  const canEdit = getMetaValue("can_edit") === "True" && !isAbandonedTask;
  const [note, setNote] = useState(initialValue ?? "");
  const [editMode, setEditMode] = useState(false);
  const [accordionIndexes, setAccordionIndexes] = useState<Array<number>>(
    canEdit ? [0] : []
  );
  const textAreaRef = useRef<HTMLTextAreaElement>(null);

  const { mutateAsync: apiCallToSetDagRunNote, isLoading: dagRunIsLoading } =
    useSetDagRunNote({ dagId, runId });
  const { mutateAsync: apiCallToSetTINote, isLoading: tiIsLoading } =
    useSetTaskInstanceNote({
      dagId,
      runId,
      taskId: taskId ?? "",
      mapIndex,
    });
  const isLoading = dagRunIsLoading || tiIsLoading;

  const objectIdentifier = taskId == null ? "DAG Run" : "Task Instance";

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (taskId == null) {
      await apiCallToSetDagRunNote(note);
    } else {
      await apiCallToSetTINote(note);
    }
    setEditMode(false);
  };

  const toggleNotesPanel = () => {
    if (accordionIndexes.includes(0)) {
      setAccordionIndexes([]);
    } else {
      setAccordionIndexes([0]);
    }
  };

  useKeysPress(keyboardShortcutIdentifier.addOrEditNotes, () => {
    if (canEdit) {
      // Notes index is 0
      if (!accordionIndexes.includes(0)) {
        setAccordionIndexes([0]);
      }
      setEditMode(true);
      setTimeout(() => textAreaRef.current?.focus(), 100);
    }
  });

  useKeysPress(keyboardShortcutIdentifier.viewNotes, toggleNotesPanel);

  return (
    <>
      <Accordion
        defaultIndex={canEdit ? [0] : []}
        index={accordionIndexes}
        allowToggle
      >
        <AccordionItem border="0">
          <AccordionButton p={0} pb={2} fontSize="inherit">
            <Box flex="1" textAlign="left" onClick={toggleNotesPanel}>
              <Text as="strong" size="lg">
                {objectIdentifier} Notes
              </Text>
            </Box>
            <AccordionIcon />
          </AccordionButton>
          <AccordionPanel pl={3}>
            {editMode ? (
              <form onSubmit={handleSubmit}>
                <Box>
                  <Textarea
                    autoFocus
                    ref={textAreaRef}
                    minH="unset"
                    overflow="hidden"
                    width="100%"
                    resize="none"
                    minRows={3}
                    maxRows={10}
                    as={ResizeTextarea}
                    value={note}
                    onChange={(e) => {
                      setNote(e.target.value);
                    }}
                    data-testid="notes-input"
                    onFocus={() => {
                      localStorage.setItem(isInputInFocus, "true");
                    }}
                    onBlur={() => {
                      localStorage.setItem(isInputInFocus, "false");
                    }}
                  />
                </Box>
                <Flex mt={3} justify="right">
                  <Button
                    type="submit"
                    isLoading={isLoading}
                    colorScheme="blue"
                  >
                    Save Note
                  </Button>
                  <Button
                    onClick={() => {
                      setNote(initialValue ?? "");
                      setEditMode(false);
                    }}
                    isLoading={isLoading}
                    ml={3}
                  >
                    Cancel
                  </Button>
                </Flex>
              </form>
            ) : (
              <Flex direction="column">
                <Flex direction="column" style={{ fontSize: "12px" }}>
                  <ReactMarkdown>{note}</ReactMarkdown>
                </Flex>
                <Flex justify="right">
                  <Button
                    onClick={() => setEditMode(true)}
                    isDisabled={!canEdit}
                    isLoading={isLoading}
                    title={`${
                      !note ? "Add" : "Edit"
                    } a note to this ${objectIdentifier}`}
                    aria-label={`${
                      !note ? "Add" : "Edit"
                    } a note to this ${objectIdentifier}`}
                    mt={2}
                    leftIcon={<MdEdit />}
                  >
                    {!note ? "Add Note" : "Edit Note"}
                  </Button>
                </Flex>
              </Flex>
            )}
          </AccordionPanel>
        </AccordionItem>
      </Accordion>
      <Divider my={0} />
    </>
  );
};

export default NotesAccordion;

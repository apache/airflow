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

import React, { useState } from 'react';
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
} from '@chakra-ui/react';
import ResizeTextarea from 'react-textarea-autosize';

import { getMetaValue } from 'src/utils';
import { useSetDagRunNote, useSetTaskInstanceNote } from 'src/api';
import { MdEdit } from 'react-icons/md';

interface Props {
  dagId: string;
  runId: string;
  taskId?: string;
  mapIndex?: number;
  initialValue?: string | null;
}

const NotesAccordion = ({
  dagId, runId, taskId, mapIndex, initialValue,
}: Props) => {
  const canEdit = getMetaValue('can_edit') === 'True';
  const [note, setNote] = useState(initialValue ?? '');
  const [editMode, setEditMode] = useState(false);

  const {
    mutateAsync: apiCallToSetDagRunNote, isLoading: dagRunIsLoading,
  } = useSetDagRunNote({ dagId, runId });
  const {
    mutateAsync: apiCallToSetTINote, isLoading: tiIsLoading,
  } = useSetTaskInstanceNote({
    dagId,
    runId,
    taskId: taskId ?? '',
    mapIndex,
  });
  const isLoading = dagRunIsLoading || tiIsLoading;

  const objectIdentifier = (taskId == null) ? 'DAG Run' : 'Task Instance';

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (taskId == null) {
      await apiCallToSetDagRunNote(note);
    } else {
      await apiCallToSetTINote(note);
    }
    setEditMode(false);
  };

  return (
    <>
      <Accordion defaultIndex={canEdit ? [0] : []} allowToggle>
        <AccordionItem border="0">
          <AccordionButton p={0} pb={2} fontSize="inherit">
            <Box flex="1" textAlign="left">
              <Text as="strong" size="lg">
                {objectIdentifier}
                {' '}
                Notes:
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
                    minH="unset"
                    overflow="hidden"
                    width="100%"
                    resize="none"
                    minRows={3}
                    maxRows={10}
                    as={ResizeTextarea}
                    value={note}
                    onChange={(e) => setNote(e.target.value)}
                    data-testid="notes-input"
                  />
                </Box>
                <Flex mt={3}>
                  <Button type="submit" isLoading={isLoading} colorScheme="blue">
                    Save Note
                  </Button>
                  <Button
                    onClick={() => { setNote(initialValue ?? ''); setEditMode(false); }}
                    isLoading={isLoading}
                    ml={3}
                  >
                    Cancel
                  </Button>
                </Flex>
              </form>
            ) : (
              <>
                <Text whiteSpace="pre-line">{note}</Text>
                <Button
                  onClick={() => setEditMode(true)}
                  isDisabled={!canEdit}
                  isLoading={isLoading}
                  title={`${!note ? 'Add' : 'Edit'} a note to this ${objectIdentifier}`}
                  aria-label={`${!note ? 'Add' : 'Edit'} a note to this ${objectIdentifier}`}
                  mt={2}
                  leftIcon={<MdEdit />}
                >
                  {!note ? 'Add Note' : 'Edit Note'}
                </Button>
              </>
            )}
          </AccordionPanel>
        </AccordionItem>
      </Accordion>
      <Divider my={0} />
    </>
  );
};

export default NotesAccordion;

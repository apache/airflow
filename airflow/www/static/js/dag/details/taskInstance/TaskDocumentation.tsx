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

import React from "react";
import {
  Accordion,
  AccordionItem,
  AccordionButton,
  AccordionPanel,
  AccordionIcon,
  Box,
  Spinner,
  Text,
} from "@chakra-ui/react";

import { useTaskDetail } from "src/api";
import ReactMarkdown from "src/components/ReactMarkdown";
import ErrorAlert from "src/components/ErrorAlert";

interface Props {
  taskId: string;
}

const TaskDocumentation = ({ taskId }: Props) => {
  const { data, isLoading, error } = useTaskDetail({
    taskId,
  });

  if (isLoading) {
    return <Spinner size="md" thickness="4px" speed="0.65s" />;
  }

  if (error) {
    return <ErrorAlert error={error} />;
  }

  if (data && data.docMd) {
    return (
      <Box mt={3} flexGrow={1}>
        <Accordion defaultIndex={[0]} allowToggle>
          <AccordionItem>
            <AccordionButton p={0} fontSize="inherit">
              <Box flex="1" textAlign="left">
                <Text as="strong" size="lg">
                  Task Documentation
                </Text>
              </Box>
              <AccordionIcon />
            </AccordionButton>
            <AccordionPanel>
              <ReactMarkdown>{data.docMd}</ReactMarkdown>
            </AccordionPanel>
          </AccordionItem>
        </Accordion>
      </Box>
    );
  }
  return null;
};

export default TaskDocumentation;

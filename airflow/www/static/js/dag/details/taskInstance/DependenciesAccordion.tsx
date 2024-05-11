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
  Flex,
  Text,
  Divider,
} from "@chakra-ui/react";

import { getMetaValue } from "src/utils";
import { useTaskInstanceDependencies } from "src/api";

interface Props {
  dagId: string;
  dagRunId: string;
  taskId?: string;
  mapIndex?: number;
}

const DependenciesAccordion = ({
  dagId,
  dagRunId,
  taskId,
  mapIndex,
}: Props) => {
  const canEdit = getMetaValue("can_edit") === "True";
  const [accordionIndexes, setAccordionIndexes] = useState<Array<number>>(
    canEdit ? [0] : []
  );

  const toggleDependenciesPanel = () => {
    if (accordionIndexes.includes(0)) {
      setAccordionIndexes([]);
    } else {
      setAccordionIndexes([0]);
    }
  };

  const { data: dependencies } = useTaskInstanceDependencies({ dagId, dagRunId, taskId, mapIndex });

  // Determine if the accordion should be collapsed by default
  const defaultIndices = dependencies?.data?.length > 0 && canEdit ? [0] : [];


  return (
    <>
      <Accordion
        defaultIndex={defaultIndices}
        // index={accordionIndexes}
        allowToggle
      >
        <AccordionItem border="0">
          <AccordionButton p={0} pb={2} fontSize="inherit">
            <Box flex="1" textAlign="left" onClick={toggleDependenciesPanel}>
              <Text as="strong" size="lg">
                Task Instance Dependencies
              </Text>
            </Box>
            <AccordionIcon />
          </AccordionButton>
          {dependencies?.map((dependency: string, index: number) => (
            <AccordionItem key={index} border="0">
              <AccordionPanel pl={3} pb={2}>
                <Box flex="1" textAlign="left">
                  <Text as="strong" size="lg">
                    {dependency}
                  </Text>
                </Box>
              </AccordionPanel>
            </AccordionItem>
          ))}
          {/* If no dependencies are available, display a message */}
          {(!dependencies || dependencies.data.length === 0) && (
            <AccordionPanel pl={3}>
              <Text>Placeholder.</Text>
            </AccordionPanel>
          )}
        </AccordionItem>
      </Accordion>
      <Divider my={0} />
    </>
  );
};

export default DependenciesAccordion;

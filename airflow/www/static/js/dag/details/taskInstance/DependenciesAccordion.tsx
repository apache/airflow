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
import ReactMarkdown from "src/components/ReactMarkdown";

interface Props {
    dagId: string;
    runId: string;
    taskId?: string;
    mapIndex?: number;
    initialValue?: string | null;
}

const DependenciesAccordion = ({
    dagId,
    runId,
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

    return (
        <>
            <Accordion
                defaultIndex={canEdit ? [0] : []}
                index={accordionIndexes}
                allowToggle
            >
                <AccordionItem border="0">
                    <AccordionButton p={0} pb={2} fontSize="inherit">
                        <Box flex="1" textAlign="left" onClick={toggleDependenciesPanel}>
                            <Text as="strong" size="lg">
                                Task Instance Dependencies:
                            </Text>
                        </Box>
                        <AccordionIcon />
                    </AccordionButton>
                    <AccordionPanel pl={3}>
                        "TODO"
                    </AccordionPanel>
                </AccordionItem>
            </Accordion>
            <Divider my={0} />
        </>
    );
};

export default DependenciesAccordion;

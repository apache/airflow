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

/* global localStorage */

import React, { useRef, useEffect } from 'react';
import {
  Table,
  Tbody,
  Box,
  Switch,
  FormControl,
  FormLabel,
  Spinner,
  Thead,
  Flex,
  useDisclosure,
  IconButton,
} from '@chakra-ui/react';
import { MdArrowForward, MdArrowBack } from 'react-icons/md';

import useTreeData from './useTreeData';
import renderTaskRows from './renderTaskRows';
import DagRuns from './dagRuns';
import Details from './details';
import { useSelection } from './providers/selection';

const sidePanelKey = 'showSidePanel';

const Tree = () => {
  const scrollRef = useRef();
  const tableRef = useRef();
  const { data: { groups = {}, dagRuns = [] }, isRefreshOn, onToggleRefresh } = useTreeData();
  const isPanelOpen = JSON.parse(localStorage.getItem(sidePanelKey));
  const { isOpen, onToggle } = useDisclosure({ defaultIsOpen: isPanelOpen });

  const { clearSelection } = useSelection();
  const toggleSidePanel = () => {
    if (!isOpen) {
      localStorage.setItem(sidePanelKey, true);
    } else {
      clearSelection();
      localStorage.setItem(sidePanelKey, false);
    }
    onToggle();
  };

  const dagRunIds = dagRuns.map((dr) => dr.runId);

  const tableWidth = tableRef && tableRef.current ? tableRef.current.offsetWidth : '100%';

  useEffect(() => {
    // Set initial scroll to top right if it is scrollable
    const runsContainer = scrollRef.current;
    if (runsContainer && runsContainer.scrollWidth > runsContainer.clientWidth) {
      runsContainer.scrollBy(runsContainer.clientWidth, 0);
    }
    // run when tableWidth or sidePanel changes
  }, [tableWidth, isOpen]);

  return (
    <Box>
      <Flex flexGrow={1} justifyContent="flex-end" alignItems="center">
        <FormControl display="flex" width="auto" mr={2}>
          {isRefreshOn && <Spinner color="blue.500" speed="1s" mr="4px" />}
          <FormLabel htmlFor="auto-refresh" mb={0} fontSize="12px" fontWeight="normal">
            Auto-refresh
          </FormLabel>
          <Switch id="auto-refresh" onChange={onToggleRefresh} isChecked={isRefreshOn} size="lg" />
        </FormControl>
        <IconButton onClick={toggleSidePanel}>
          {isOpen
            ? <MdArrowForward size="18px" aria-label="Collapse Details" title="Collapse Details" />
            : <MdArrowBack size="18px" title="Expand Details" aria-label="Expand Details" />}
        </IconButton>
      </Flex>
      <Flex flexDirection="row" justifyContent="space-between">
        <Box
          position="relative"
          mr="12px"
          mt="-8px"
          pb="12px"
          overflow="auto"
          ref={scrollRef}
          flexGrow={1}
          minWidth={isOpen && '300px'}
        >
          <Table>
            <Thead display="block" pr="10px" position="sticky" top={0} zIndex={2} bg="white">
              <DagRuns tableWidth={tableWidth} />
            </Thead>
            {/* TODO: remove hardcoded values. 665px is roughly the total heade+footer height */}
            <Tbody display="block" width="100%" maxHeight="calc(100vh - 665px)" minHeight="500px" ref={tableRef} pr="10px">
              {renderTaskRows({
                task: groups, dagRunIds, tableWidth,
              })}
            </Tbody>
          </Table>
        </Box>
        {isOpen && (
          <Details />
        )}
      </Flex>
    </Box>
  );
};

export default Tree;

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

import React, { useRef, useEffect, useState } from 'react';
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
  Button,
} from '@chakra-ui/react';

import { useTreeData } from './api';
import renderTaskRows from './renderTaskRows';
import ResetRoot from './ResetRoot';
import DagRuns from './dagRuns';
import Details from './details';
import { useSelection } from './context/selection';
import { useAutoRefresh } from './context/autorefresh';

const sidePanelKey = 'showSidePanel';

const Tree = () => {
  const scrollRef = useRef();
  const tableRef = useRef();
  const [tableWidth, setTableWidth] = useState('100%');
  const { data: { groups = {}, dagRuns = [] } } = useTreeData();
  const { isRefreshOn, toggleRefresh, isPaused } = useAutoRefresh();
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

  useEffect(() => {
    if (tableRef && tableRef.current) {
      setTableWidth(tableRef.current.offsetWidth);
      const runsContainer = scrollRef.current;
      // Set initial scroll to top right if it is scrollable
      if (runsContainer && runsContainer.scrollWidth > runsContainer.clientWidth) {
        runsContainer.scrollBy(tableRef.current.offsetWidth, 0);
      }
    }
  }, [tableRef, isOpen]);

  return (
    <Box>
      <Flex flexGrow={1} justifyContent="flex-end" alignItems="center">
        <ResetRoot />
        <FormControl display="flex" width="auto" mr={2}>
          {isRefreshOn && <Spinner color="blue.500" speed="1s" mr="4px" />}
          <FormLabel htmlFor="auto-refresh" mb={0} fontWeight="normal">
            Auto-refresh
          </FormLabel>
          <Switch
            id="auto-refresh"
            onChange={() => toggleRefresh(true)}
            isDisabled={isPaused}
            isChecked={isRefreshOn}
            size="lg"
            title={isPaused ? 'Autorefresh is disabled while the DAG is paused' : ''}
          />
        </FormControl>
        <Button
          onClick={toggleSidePanel}
          aria-label={isOpen ? 'Show Details' : 'Hide Details'}
          variant={isOpen ? 'solid' : 'outline'}
        >
          {isOpen ? 'Hide ' : 'Show '}
          Details Panel
        </Button>
      </Flex>
      <Flex flexDirection="row" justifyContent="space-between">
        <Box
          position="relative"
          mt={2}
          m="12px"
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
